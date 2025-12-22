"""
Production скрипт для обучения рекомендательной системы.

Реализует алгоритм из финального эксперимента (WEIGHTS_RERANK_a0.6_b0.4):
- ALS с оптимизированными весами событий
- Content-Based Re-ranking параметры сохраняются в метаданных (применяются при генерации)
- Обучение на всех данных (без test split)

Параметры загружаются из конфигурационного YAML файла (configs/config.yaml).

Используется только в Airflow DAG для ежедневного переобучения модели.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import polars as pl
import yaml

# Парсинг аргументов нужен только для запуска из командной строки
try:
    import argparse
except ImportError:
    argparse = None  # type: ignore

from src.recsys.artifacts import save_artifacts
from src.recsys.data.events import load_events
from src.recsys.data.validation import validate_events
from src.recsys.matrix import build_user_item_matrix
from src.recsys.models.als import train_als
from src.recsys.models.popular import top_popular_items
from src.recsys.preprocess.interactions import InteractionConfig, build_interactions
from src.recsys.utils.logging import setup_logging


def load_config(config_path: Path) -> dict:
    """Загружает конфигурацию из YAML файла.
    
    Args:
        config_path: Путь к файлу конфигурации
        
    Returns:
        Словарь с параметрами конфигурации
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    return config


def main(
    config_path: str | Path,
    data_dir: str | Path,
    artifacts_dir: str | Path,
    run_name: str | None = None,
) -> None:
    """Главная функция обучения production модели.
    
    Args:
        config_path: Путь к конфигурационному YAML файлу
        data_dir: Директория с данными (содержит events.csv)
        artifacts_dir: Директория для сохранения артефактов модели
        run_name: Имя запуска для логирования (опционально)
    """
    setup_logging()
    logger = logging.getLogger(__name__)

    # Загрузка конфигурации
    config_path = Path(config_path)
    config = load_config(config_path)
    
    data_config = config.get("data", {})
    weights_config = config.get("weights", {})
    als_config = config.get("als", {})
    eval_config = config.get("evaluation", {})
    
    interaction_config = InteractionConfig(
        min_user_interactions=data_config.get("min_user_interactions", 5),
        min_item_interactions=data_config.get("min_item_interactions", 5),
        weight_view=weights_config.get("view", 1.0),
        weight_addtocart=weights_config.get("addtocart", 4.0),
        weight_transaction=weights_config.get("transaction", 8.0),
    )
    
    data_dir = Path(data_dir)
    artifacts_dir = Path(artifacts_dir)
    
    run_id = run_name or os.getenv("MLFLOW_RUN_NAME") or datetime.now().strftime("%Y%m%d_%H%M%S")
    
    logger.info(f"Обучение production модели: {run_id}")
    
    # Загрузка данных
    events = load_events(data_dir)
    events, events_stats = validate_events(events, log_stats=False)
    logger.info(f"События: {len(events):,}")
    
    # Построение interactions
    interactions = build_interactions(events, interaction_config)
    logger.info(f"Interactions: {len(interactions):,} "
                f"(пользователей: {interactions.select('user_id').n_unique():,}, "
                f"товаров: {interactions.select('item_id').n_unique():,})")
    
    # Построение user-item матрицы
    user_item, mappings = build_user_item_matrix(interactions)
    logger.info(f"Матрица: {user_item.shape[0]:,} пользователей × {user_item.shape[1]:,} товаров")
    
    # Обучение ALS модели
    num_threads = als_config.get("num_threads", 0)
    num_threads = None if num_threads == 0 else num_threads
    
    model = train_als(
        user_item=user_item,
        factors=als_config.get("factors", 256),
        iterations=als_config.get("iterations", 25),
        regularization=als_config.get("regularization", 0.1),
        alpha=als_config.get("alpha", 4.0),
        random_state=als_config.get("random_state", 42),
        num_threads=num_threads,
    )
    
    # Генерация популярных товаров
    popular_k = eval_config.get("popular_k", 100)
    popular_items = top_popular_items(interactions, k=popular_k)
    
    # Извлечение покупок
    purchases = events.filter(pl.col("event") == "transaction").select([
        pl.col("user_id"),
        pl.col("item_id"),
    ]).unique()
    
    # Формирование параметров для сохранения
    params = {
        "min_user_interactions": interaction_config.min_user_interactions,
        "min_item_interactions": interaction_config.min_item_interactions,
        "split_mode": "production",  # Production модель обучается на всех данных
        "weights": {
            "view": interaction_config.weight_view,
            "addtocart": interaction_config.weight_addtocart,
            "transaction": interaction_config.weight_transaction,
        },
        "als": {
            "factors": als_config.get("factors", 256),
            "iterations": als_config.get("iterations", 25),
            "regularization": als_config.get("regularization", 0.1),
            "alpha": als_config.get("alpha", 4.0),
            "random_state": als_config.get("random_state", 42),
            "num_threads": int(num_threads or 0),
        },
        "rerank": config.get("rerank", {}),
        "recommendations": config.get("recommendations", {}),
        "evaluation": eval_config,
        "experiment_name": config.get("experiment_name"),
        "run_id": run_id,
    }
    
    # Сохранение артефактов
    paths = save_artifacts(
        artifacts_dir=artifacts_dir,
        model=model,
        user_item=user_item,
        mappings=mappings,
        popular_items=popular_items,
        params=params,
        user_purchases=purchases,
    )
    
    # Логирование в MLflow (опционально)
    if os.getenv("MLFLOW_TRACKING_URI") is not None:
        # Загрузка .env файла для S3 credentials (если не загружен)
        try:
            from dotenv import load_dotenv
            project_root = Path(artifacts_dir).parent
            env_path = project_root / ".env"
            if env_path.exists():
                load_dotenv(env_path, override=False)
                # Устанавливаем MLFLOW_S3_ENDPOINT_URL из S3_ENDPOINT_URL если нужно
                if os.getenv("S3_ENDPOINT_URL") and not os.getenv("MLFLOW_S3_ENDPOINT_URL"):
                    os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv("S3_ENDPOINT_URL")
                
                # Устанавливаем AWS_DEFAULT_REGION если не установлен (для Yandex Cloud)
                if not os.getenv("AWS_DEFAULT_REGION") and not os.getenv("AWS_REGION"):
                    os.environ["AWS_DEFAULT_REGION"] = "ru-central1"
        except ImportError:
            pass
        except Exception as e:
            logger.warning(f"Ошибка загрузки .env для S3 credentials: {e}")
        
        try:
            import mlflow
            
            experiment_name = "recsys_production"
            experiment = mlflow.get_experiment_by_name(experiment_name)
            if experiment is None:
                experiment_id = mlflow.create_experiment(experiment_name)
            else:
                experiment_id = experiment.experiment_id
            
            with mlflow.start_run(
                run_name=f"train_{run_id}",
                experiment_id=experiment_id,
            ):
                # Логирование параметров обучения
                mlflow.log_params({
                    "run_id": run_id,
                    "split_mode": "production",
                    "min_user_interactions": interaction_config.min_user_interactions,
                    "min_item_interactions": interaction_config.min_item_interactions,
                    "weight_view": interaction_config.weight_view,
                    "weight_addtocart": interaction_config.weight_addtocart,
                    "weight_transaction": interaction_config.weight_transaction,
                    "als_factors": als_config.get("factors", 256),
                    "als_iterations": als_config.get("iterations", 25),
                    "als_regularization": als_config.get("regularization", 0.1),
                    "als_alpha": als_config.get("alpha", 4.0),
                    "als_random_state": als_config.get("random_state", 42),
                    "num_threads": int(num_threads or 0),
                })
                
                # Логирование метрик обучения (статистика данных)
                mlflow.log_metrics({
                    "events_count": len(events),
                    "interactions_count": len(interactions),
                    "unique_users": interactions.select("user_id").n_unique(),
                    "unique_items": interactions.select("item_id").n_unique(),
                    "user_item_matrix_rows": user_item.shape[0],
                    "user_item_matrix_cols": user_item.shape[1],
                    "popular_items_count": len(popular_items),
                })
                
                # Логирование модели (опционально, ошибки S3 не блокируют выполнение)
                try:
                    mlflow.log_artifact(str(paths.model_pkl), "model")
                except Exception:
                    pass
                
                try:
                    mlflow.log_artifact(str(paths.metadata_json), "model")
                except Exception:
                    pass
        except ImportError:
            logger.warning("MLflow не установлен, пропускаем логирование")
        except Exception as e:
            logger.warning(f"Ошибка логирования в MLflow: {e}")
    
    logger.info(f"Обучение завершено успешно: {run_id}")


if __name__ == "__main__":
    """Поддержка запуска из командной строки (для тестирования)."""
    if argparse is None:
        raise ImportError("argparse required for command-line execution")
    
    p = argparse.ArgumentParser(
        description="Обучение production модели рекомендательной системы на основе конфига"
    )
    p.add_argument("--config", type=str, required=True, help="Путь к конфигурационному файлу")
    p.add_argument("--data-dir", type=str, required=True, help="Директория с данными (events.csv)")
    p.add_argument("--artifacts-dir", type=str, required=True, help="Директория для сохранения артефактов")
    p.add_argument("--run-name", type=str, default=None, help="Имя запуска для логирования")
    args = p.parse_args()
    
    main(
        config_path=args.config,
        data_dir=args.data_dir,
        artifacts_dir=args.artifacts_dir,
        run_name=args.run_name,
    )

