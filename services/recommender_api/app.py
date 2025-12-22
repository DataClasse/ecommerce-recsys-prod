from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response, PlainTextResponse
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, REGISTRY
from prometheus_client.openmetrics.exposition import (
    CONTENT_TYPE_LATEST,
    generate_latest as generate_latest_openmetrics,
)

from .handler import RecommenderHandler

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)

MODEL_DIR = Path(os.getenv("MODEL_DIR", PROJECT_ROOT / "artifacts" / "model"))
ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", PROJECT_ROOT / "artifacts"))
ENABLE_RERANK = os.getenv("ENABLE_RERANK", "false").lower() == "true"

# Prometheus метрики
# Базовые метрики запросов (с расширенными метками)
REQ_TOTAL = Counter("recsys_requests_total", "Всего запросов к API", ["model_type", "status_code", "endpoint"])
REQ_LAT = Histogram("recsys_request_latency_seconds", "Задержка запросов", ["endpoint"])
REQ_ERRORS = Counter("recsys_errors_total", "Всего ошибок API", ["status_code", "endpoint", "model_type"])

# Бизнес-метрики
CLICKS_TOTAL = Counter("recsys_clicks_total", "Всего кликов по рекомендациям")
ADDTOCART_TOTAL = Counter("recsys_addtocart_total", "Всего добавлений в корзину")
TRANSACTIONS_TOTAL = Counter("recsys_transactions_total", "Всего транзакций")
REVENUE_TOTAL = Counter("recsys_revenue_total", "Общая выручка")

# ML-метрики реального времени
RECOMMENDATIONS_COUNT = Histogram(
    "recsys_recommendations_count",
    "Количество рекомендаций на пользователя",
    buckets=(1, 5, 10, 20, 50, 100, 200, 500)
)
ITEMS_DISTRIBUTION = Histogram(
    "recsys_items_distribution",
    "Распределение item_id в рекомендациях",
    buckets=(100, 500, 1000, 5000, 10000, 50000, 100000, 500000)
)
COLD_USERS_TOTAL = Counter("recsys_cold_users_total", "Всего холодных пользователей", ["model_type"])
RERANK_USAGE_TOTAL = Counter("recsys_rerank_usage_total", "Использование re-ranking")

# Глобальный обработчик (инициализируется в lifespan)
handler: RecommenderHandler | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Контекстный менеджер lifespan для загрузки модели при старте.
    
    Использует lifespan вместо устаревшего on_event("startup").
    """
    global handler
    
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    logger.info("Запуск Recommender API...")
    
    # Инициализация обработчика
    try:
        handler = RecommenderHandler(
            model_dir=MODEL_DIR,
            artifacts_dir=ARTIFACTS_DIR,
            enable_rerank=ENABLE_RERANK,
        )
        logger.info("Обработчик успешно инициализирован")
    except Exception as e:
        logger.error(f"Не удалось инициализировать обработчик: {e}")
        handler = None
    
    yield
    
    logger.info("Остановка Recommender API...")


app = FastAPI(title="Recommender API", version="1.0.0", lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, Any]:
    """Корневой endpoint с информацией о доступных endpoints.
    
    Returns:
        dict: Информация о API и доступных endpoints
    """
    return {
        "name": "Recommender API",
        "version": "1.0.0",
        "description": "API для рекомендательной системы",
        "endpoints": {
            "health": "/health",
            "documentation": "/docs",
            "recommendations": "/recommendations/{user_id}",
            "baseline_recommendations": "/recommendations/baseline/{user_id}",
            "stats": "/stats",
            "ab_test_stats": "/ab-test/stats",
            "metrics": "/metrics",
            "events": "/events (POST)",
        },
        "documentation": "Перейдите на /docs для интерактивной документации API",
    }


class EventRequest(BaseModel):
    """Модель запроса для логирования события."""
    user_id: int
    item_id: int
    event_type: str
    revenue: float = 0.0
    model_type: str | None = None


@app.get("/health")
async def health() -> dict[str, Any]:
    """Health check endpoint.
    
    Проверяет готовность модели и наличие fallback механизмов.
    """
    if handler is None:
        return {
            "status": "error",
            "message": "Handler not initialized",
        }
    
    return {
        "status": "ok",
        "model_loaded": handler.model is not None,
        "has_fallback_popular": bool(handler.popular),
        "rerank_enabled": handler.enable_rerank and handler._can_rerank(),
    }


@app.get("/recommendations/{user_id}")
async def recommend(
    user_id: int,
    top_k: int = 10,
    use_rerank: bool | None = None,
    use_baseline: bool = False,
    ab_test_group: str | None = None,
) -> dict[str, Any]:
    """Генерация рекомендаций для пользователя.
    
    Args:
        user_id: ID пользователя
        top_k: Количество рекомендаций (1-500)
        use_rerank: Использовать Content-Based re-ranking (опционально)
        use_baseline: Использовать baseline модель (popular items)
        ab_test_group: Группа A/B теста (auto, als, baseline). Если auto, выбирается случайно
    
    Returns:
        dict: Рекомендации с полями user_id, recommendations, cold, model_type
    """
    endpoint = "/recommendations/{user_id}"
    status_code = "200"
    model_type_label = "als"
    
    if handler is None:
        status_code = "503"
        REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type="unknown").inc()
        REQ_TOTAL.labels(model_type="unknown", status_code=status_code, endpoint=endpoint).inc()
        raise HTTPException(status_code=503, detail="Model is not ready")
    
    if top_k <= 0 or top_k > 500:
        status_code = "400"
        REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type="unknown").inc()
        REQ_TOTAL.labels(model_type="unknown", status_code=status_code, endpoint=endpoint).inc()
        raise HTTPException(status_code=400, detail="top_k must be in [1, 500]")
    
    # A/B тестирование: автоматический выбор группы
    if ab_test_group == "auto":
        import random
        ab_test_group = "baseline" if random.random() < 0.5 else "als"
    
    # Определение модели на основе A/B теста
    if ab_test_group == "baseline" or use_baseline:
        use_baseline = True
        model_type_label = "baseline"
    else:
        model_type_label = "als"
    
    with REQ_LAT.labels(endpoint=endpoint).time():
        try:
            result = handler.recommend(user_id, top_k, use_rerank=use_rerank, use_baseline=use_baseline)
            
            # Обновление модели на основе результата
            if result.get("model_type"):
                model_type_label = result["model_type"]
            
            # ML-метрики реального времени
            recommendations = result.get("recommendations", [])
            rec_count = len(recommendations)
            RECOMMENDATIONS_COUNT.observe(rec_count)
            
            # Гистограмма распределения item_id
            for rec in recommendations:
                item_id = rec.get("item_id", 0)
                ITEMS_DISTRIBUTION.observe(float(item_id))
            
            # Счетчик холодных пользователей
            if result.get("cold", False):
                COLD_USERS_TOTAL.labels(model_type=model_type_label).inc()
            
            # Метрика использования re-ranking
            if result.get("model_type") == "rerank":
                RERANK_USAGE_TOTAL.inc()
            
            # Успешный запрос
            REQ_TOTAL.labels(model_type=model_type_label, status_code="200", endpoint=endpoint).inc()
            
            # Добавляем информацию о A/B тесте
            result["ab_test_group"] = ab_test_group or ("baseline" if use_baseline else "als")
            return result
            
        except ValueError as e:
            status_code = "404"
            REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type=model_type_label).inc()
            REQ_TOTAL.labels(model_type=model_type_label, status_code=status_code, endpoint=endpoint).inc()
            raise HTTPException(status_code=404, detail=str(e))
        except RuntimeError as e:
            status_code = "503"
            REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type=model_type_label).inc()
            REQ_TOTAL.labels(model_type=model_type_label, status_code=status_code, endpoint=endpoint).inc()
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            status_code = "500"
            REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type=model_type_label).inc()
            REQ_TOTAL.labels(model_type=model_type_label, status_code=status_code, endpoint=endpoint).inc()
            logger.error(f"Ошибка при генерации рекомендаций: {e}")
            raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@app.get("/stats")
async def stats() -> dict[str, Any]:
    """Статистика использования API.
    
    Returns:
        dict: Статистика запросов и использования модели
    """
    if handler is None:
        return {
            "status": "error",
            "message": "Handler not initialized",
        }
    
    return handler.get_stats()


@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    """Prometheus metrics endpoint.
    
    Возвращает метрики в формате OpenMetrics, который требует Prometheus.
    Prometheus ожидает, что метрики заканчиваются на "# EOF\n".
    """
    # Генерируем метрики и добавляем EOF маркер для OpenMetrics формата
    metrics_data = generate_latest(REGISTRY)
    # Преобразуем в строку и добавляем EOF маркер
    metrics_text = metrics_data.decode('utf-8')
    if not metrics_text.endswith('# EOF\n'):
        metrics_text += '# EOF\n'
    return PlainTextResponse(
        content=metrics_text,
        media_type=CONTENT_TYPE_LATEST
    )


@app.post("/events")
async def log_event(event: EventRequest) -> dict[str, Any]:
    """Логирование бизнес-событий (click, addtocart, transaction).
    
    Args:
        event: Запрос с данными события
    
    Returns:
        dict: Статус логирования
    """
    endpoint = "/events"
    status_code = "200"
    
    if handler is None:
        status_code = "503"
        REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type="unknown").inc()
        REQ_TOTAL.labels(model_type="unknown", status_code=status_code, endpoint=endpoint).inc()
        raise HTTPException(status_code=503, detail="Handler is not ready")
    
    if event.event_type not in ["click", "addtocart", "transaction"]:
        status_code = "400"
        REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type="unknown").inc()
        REQ_TOTAL.labels(model_type="unknown", status_code=status_code, endpoint=endpoint).inc()
        raise HTTPException(status_code=400, detail="event_type должен быть click, addtocart или transaction")
    
    with REQ_LAT.labels(endpoint=endpoint).time():
        try:
            # Обновление Prometheus метрик
            if event.event_type == "click":
                CLICKS_TOTAL.inc()
            elif event.event_type == "addtocart":
                ADDTOCART_TOTAL.inc()
            elif event.event_type == "transaction":
                TRANSACTIONS_TOTAL.inc()
                REVENUE_TOTAL.inc(event.revenue)
            
            # Логирование в handler (с model_type если передан)
            handler.log_event(
                event.user_id,
                event.item_id,
                event.event_type,
                event.revenue,
                model_type=getattr(event, 'model_type', None),
            )
            
            # Успешный запрос
            REQ_TOTAL.labels(model_type="unknown", status_code="200", endpoint=endpoint).inc()
            
            return {
                "status": "ok",
                "event_logged": True,
                "event_type": event.event_type,
            }
        except Exception as e:
            status_code = "500"
            REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type="unknown").inc()
            REQ_TOTAL.labels(model_type="unknown", status_code=status_code, endpoint=endpoint).inc()
            logger.error(f"Ошибка при логировании события: {e}")
            raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@app.get("/recommendations/baseline/{user_id}")
async def recommend_baseline(
    user_id: int,
    top_k: int = 10,
) -> dict[str, Any]:
    """Генерация рекомендаций от baseline модели (popular items).
    
    Args:
        user_id: ID пользователя
        top_k: Количество рекомендаций (1-500)
    
    Returns:
        dict: Рекомендации от baseline модели
    """
    endpoint = "/recommendations/baseline/{user_id}"
    model_type_label = "baseline"
    status_code = "200"
    
    if handler is None:
        status_code = "503"
        REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type=model_type_label).inc()
        REQ_TOTAL.labels(model_type=model_type_label, status_code=status_code, endpoint=endpoint).inc()
        raise HTTPException(status_code=503, detail="Модель не готова")
    
    if top_k <= 0 or top_k > 500:
        status_code = "400"
        REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type=model_type_label).inc()
        REQ_TOTAL.labels(model_type=model_type_label, status_code=status_code, endpoint=endpoint).inc()
        raise HTTPException(status_code=400, detail="top_k должен быть в диапазоне [1, 500]")
    
    with REQ_LAT.labels(endpoint=endpoint).time():
        try:
            result = handler.get_baseline_recommendations(user_id, top_k)
            
            # ML-метрики реального времени
            recommendations = result.get("recommendations", [])
            rec_count = len(recommendations)
            RECOMMENDATIONS_COUNT.observe(rec_count)
            
            # Гистограмма распределения item_id
            for rec in recommendations:
                item_id = rec.get("item_id", 0)
                ITEMS_DISTRIBUTION.observe(float(item_id))
            
            # Счетчик холодных пользователей
            if result.get("cold", False):
                COLD_USERS_TOTAL.labels(model_type=model_type_label).inc()
            
            # Успешный запрос
            REQ_TOTAL.labels(model_type=model_type_label, status_code="200", endpoint=endpoint).inc()
            
            return result
            
        except RuntimeError as e:
            status_code = "503"
            REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type=model_type_label).inc()
            REQ_TOTAL.labels(model_type=model_type_label, status_code=status_code, endpoint=endpoint).inc()
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            status_code = "500"
            REQ_ERRORS.labels(status_code=status_code, endpoint=endpoint, model_type=model_type_label).inc()
            REQ_TOTAL.labels(model_type=model_type_label, status_code=status_code, endpoint=endpoint).inc()
            logger.error(f"Ошибка при генерации baseline рекомендаций: {e}")
            raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@app.get("/ab-test/stats")
async def ab_test_stats() -> dict[str, Any]:
    """Статистика A/B тестирования.
    
    Returns:
        dict: Статистика A/B теста и бизнес-метрики
    """
    if handler is None:
        return {
            "status": "error",
            "message": "Обработчик не инициализирован",
        }
    
    stats = handler.get_stats()
    
    return {
        "status": "ok",
        "ab_test": {
            "als_requests": stats["personal_recommendations"] + stats["rerank_applied"],
            "baseline_requests": stats["baseline_recommendations"],
            "fallback_requests": stats["popular_fallback"],
        },
        "business_metrics": stats["business_metrics"],
        "model_stats": {
            "model_loaded": stats["model_loaded"],
            "rerank_enabled": stats["rerank_enabled"],
        },
    }


