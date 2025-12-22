"""Скрипт для валидации обученной модели.

Вычисляет метрики качества на валидационной выборке и сравнивает
с метриками предыдущей модели для обнаружения деградации.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from src.recsys.data.events import load_events
from src.recsys.data.validation import validate_events
from src.recsys.evaluation.metrics import compute_all_metrics
from src.recsys.matrix import build_user_item_matrix
from src.recsys.models.als import train_als
from src.recsys.models.popular import top_popular_items
from src.recsys.models.recommendations import generate_recommendations
from src.recsys.models.rerank import category_based_rerank
from src.recsys.artifacts import (
    get_item_metadata,
    get_category_stats,
    get_category_tree_enhanced,
)
from src.recsys.preprocess.interactions import InteractionConfig, build_interactions
from src.recsys.split import temporal_split_per_user
from src.recsys.utils.logging import setup_logging


def load_config(config_path: Path) -> dict:
    """Загружает конфигурацию из YAML файла."""
    if not config_path.exists():
        raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    return config


def check_degradation(
    current_metrics: dict[str, float],
    previous_metrics: dict[str, float],
    degradation_threshold: float = 0.1,
) -> dict[str, Any] | None:
    """Проверяет деградацию метрик по сравнению с предыдущей моделью.
    
    Args:
        current_metrics: Текущие метрики
        previous_metrics: Метрики предыдущей модели
        degradation_threshold: Порог деградации (0.1 = 10% падение)
    
    Returns:
        dict с информацией о деградации или None, если деградации нет
    """
    degradation_info = {}
    
    # Ключевые метрики для проверки
    key_metrics = [
        "precision@5",
        "precision@10",
        "recall@10",
        "recall@20",
        "ndcg@10",
        "hit_rate@5",
        "hit_rate@10",
    ]
    
    for metric in key_metrics:
        if metric in current_metrics and metric in previous_metrics:
            current = current_metrics[metric]
            previous = previous_metrics[metric]
            
            if previous > 0:
                relative_change = (current - previous) / previous
                if relative_change < -degradation_threshold:
                    degradation_info[metric] = {
                        "current": current,
                        "previous": previous,
                        "change": relative_change,
                        "change_pct": relative_change * 100,
                    }
    
    if degradation_info:
        return {
            "degraded_metrics": degradation_info,
            "threshold": degradation_threshold,
        }
    
    return None


def validate_model(
    model_path: Path,
    data_dir: Path,
    config_path: Path,
    artifacts_dir: Path,
    previous_metrics_path: Path | None = None,
    degradation_threshold: float = 0.1,
    validation_size: float = 0.15,  # 15% данных для валидации
) -> dict[str, Any]:
    """Валидирует модель и сравнивает с предыдущей.
    
    Args:
        model_path: Путь к обученной модели (als_model.pkl)
        data_dir: Директория с данными (events.csv)
        config_path: Путь к конфигурационному файлу
        artifacts_dir: Директория с артефактами (для загрузки mappings, popular items)
        previous_metrics_path: Путь к метрикам предыдущей модели (опционально)
        degradation_threshold: Порог деградации (0.1 = 10% падение)
        validation_size: Доля данных для валидации (0.15 = 15%)
    
    Returns:
        dict с метриками и статусом валидации
    
    Raises:
        ValueError: Если обнаружена деградация модели
    """
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Начало валидации модели")
    
    if not model_path.exists():
        raise FileNotFoundError(f"Модель не найдена: {model_path}")
    
    config = load_config(config_path)
    data_config = config.get("data", {})
    weights_config = config.get("weights", {})
    eval_config = config.get("evaluation", {})
    
    # Загрузка данных
    events = load_events(data_dir)
    events, events_stats = validate_events(events, log_stats=False)
    logger.info(f"Загружено событий: {len(events):,}")
    
    # Временной split для валидации
    
    # Преобразуем timestamp обратно в Datetime для временного split
    events = events.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="ms").alias("timestamp_dt")
    )
    
    # Находим cutoff дату для валидации (последние validation_size% по времени)
    max_timestamp = events.select(pl.col("timestamp_dt").max()).item()
    min_timestamp = events.select(pl.col("timestamp_dt").min()).item()
    total_duration = (max_timestamp - min_timestamp).total_seconds()
    cutoff_duration = total_duration * validation_size
    cutoff_timestamp = max_timestamp - timedelta(seconds=int(cutoff_duration))
    
    # Разделяем данные по времени
    # Train: данные до cutoff (которые модель видела при обучении)
    # Validation: данные после cutoff (которые модель НЕ видела)
    train_events = events.filter(pl.col("timestamp_dt") < cutoff_timestamp)
    val_events = events.filter(pl.col("timestamp_dt") >= cutoff_timestamp)
    
    logger.info(f"Split: train={len(train_events):,} ({len(train_events)/len(events)*100:.1f}%), val={len(val_events):,} ({len(val_events)/len(events)*100:.1f}%)")
    
    # Возвращаем timestamp в миллисекунды для дальнейшей обработки
    train_events = train_events.with_columns(
        (pl.col("timestamp_dt").cast(pl.Int64) // 1_000_000).alias("timestamp")
    ).drop("timestamp_dt")
    val_events = val_events.with_columns(
        (pl.col("timestamp_dt").cast(pl.Int64) // 1_000_000).alias("timestamp")
    ).drop("timestamp_dt")
    
    # Создание test_events для фильтрации
    test_events = val_events.filter(pl.col("event").is_in(["addtocart", "transaction"]))
    
    # Построение interactions
    interaction_config = InteractionConfig(
        min_user_interactions=data_config.get("min_user_interactions", 5),
        min_item_interactions=data_config.get("min_item_interactions", 5),
        weight_view=weights_config.get("view", 1.0),
        weight_addtocart=weights_config.get("addtocart", 4.0),
        weight_transaction=weights_config.get("transaction", 8.0),
    )
    all_interactions = build_interactions(train_events, interaction_config)
    
    # Разделение на train и validation
    train_interactions, val_interactions = temporal_split_per_user(
        all_interactions,
        test_size=validation_size,
    )
    logger.info(f"Interactions: train={len(train_interactions):,}, val={len(val_interactions):,}")
    
    # Построение user-item матрицы
    user_item_train, mappings = build_user_item_matrix(train_interactions)
    logger.info(f"Матрица: {user_item_train.shape[0]:,} пользователей × {user_item_train.shape[1]:,} товаров")
    
    # Обучение модели на train_interactions
    als_config = config.get("als", {})
    num_threads = als_config.get("num_threads", 0)
    num_threads = None if num_threads == 0 else num_threads
    
    model = train_als(
        user_item=user_item_train,
        factors=als_config.get("factors", 256),
        iterations=als_config.get("iterations", 25),
        regularization=als_config.get("regularization", 0.1),
        alpha=als_config.get("alpha", 4.0),
        random_state=als_config.get("random_state", 42),
        num_threads=num_threads,
    )
    
    # Получение популярных товаров
    popular_k = config.get("popular", {}).get("k", 100)
    popular_items = top_popular_items(train_interactions, k=popular_k)
    
    # Загрузка артефактов для re-ranking
    project_root = artifacts_dir.parent
    item_metadata = get_item_metadata(project_root)
    category_stats = get_category_stats(project_root)
    category_tree = get_category_tree_enhanced(project_root)
    
    # Генерация рекомендаций
    # Получение warm test users
    user_index_keys_set = set(mappings.user_index.keys())
    warm_test_users = (
        val_interactions.select("user_id")
        .unique()
        .filter(pl.col("user_id").is_in(user_index_keys_set))
        .get_column("user_id")
        .to_list()
    )
    
    eval_k = eval_config.get("k", 5)
    recall_k = eval_config.get("recall_k", 20)
    num_recs_to_generate = max(recall_k, eval_k * 4)
    
    # Генерация рекомендаций
    recs, warm_count, cold_count = generate_recommendations(
        model=model,
        user_item_matrix=user_item_train,
        mappings=mappings,
        warm_test_users=warm_test_users,
        popular_items=popular_items,
        eval_k=num_recs_to_generate,
        test_interactions_df=val_interactions,
        item_metadata=item_metadata,
        user_recent_events=test_events,
    )
    logger.info(f"Рекомендации: {len(recs):,} (warm: {warm_count}, cold: {cold_count})")
    
    # Применение Content-Based Re-ranking
    rerank_config = config.get("rerank", {})
    if rerank_config.get("enabled", True):
        user_history_for_rerank = (
            train_interactions
            .join(
                item_metadata.select(["item_id", "category_id"]),
                on="item_id",
                how="left",
                coalesce=True
            )
            .filter(pl.col("category_id").is_not_null())
            .select(["user_id", "item_id", "category_id"])
        )
        
        recs = category_based_rerank(
            recommendations=recs,
            user_item_history=user_history_for_rerank,
            item_metadata=item_metadata,
            category_stats=category_stats,
            category_tree=category_tree,
            top_k=num_recs_to_generate,
            alpha=rerank_config.get("alpha", 0.6),
            beta=rerank_config.get("beta", 0.4),
            adaptive=rerank_config.get("adaptive", True),
        )
    
    # Создание test_interactions_target
    val_interactions_target = val_interactions.join(
        test_events.select(["user_id", "item_id"]).unique(),
        on=["user_id", "item_id"],
        how="inner"
    )
    
    # Проверка минимального количества test interactions
    min_test_interactions = 100
    if len(val_interactions_target) < min_test_interactions:
        logger.warning(f"Мало test interactions: {len(val_interactions_target):,} (рекомендуется >= {min_test_interactions:,})")
    
    # Вычисление метрик
    
    # Размер каталога для Coverage метрики
    catalog_size = int(train_interactions.select("item_id").n_unique())
    
    # Основные метрики из финального эксперимента
    eval_k = eval_config.get("k", 5)
    
    # Используем compute_all_metrics
    # Передаем val_interactions (все) и val_interactions_target (только addtocart/transaction)
    all_metrics = compute_all_metrics(
        test_interactions=val_interactions,  # Все validation interactions
        recs=recs,
        train_interactions=train_interactions,
        catalog_size=catalog_size,
        eval_k=eval_k,
        model_name="WEIGHTS_RERANK_a0.6_b0.4",
        item_metadata=item_metadata,
        test_interactions_target=val_interactions_target,  # Только addtocart/transaction
        test_events=test_events,  # Для cart_prediction_rate
    )
    
    # Логирование основных метрик
    logger.info(f"Метрики: Recall@20={all_metrics['recall@20']:.4f}, Precision@5={all_metrics['precision@5']:.4f}, "
                f"NDCG@10={all_metrics['ndcg@10']:.4f}, Hit Rate@5={all_metrics['hit_rate@5']:.4f}")
    
    # Сохранение метрик
    metrics_path = model_path.parent / "validation_metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(all_metrics, f, indent=2, ensure_ascii=False)
    
    # Сравнение с предыдущей моделью
    degradation = None
    if previous_metrics_path and previous_metrics_path.exists():
        with open(previous_metrics_path, "r", encoding="utf-8") as f:
            previous_metrics = json.load(f)
        
        degradation = check_degradation(
            current_metrics=all_metrics,
            previous_metrics=previous_metrics,
            degradation_threshold=degradation_threshold,
        )
        
        if degradation:
            logger.warning("Обнаружена деградация")
    
    # 12. Логирование в MLflow (ПОСЛЕ проверки деградации, но ДО ValueError)
    import os
    if os.getenv("MLFLOW_TRACKING_URI") is not None:
        # Загрузка .env файла для S3 credentials (если не загружен)
        try:
            from dotenv import load_dotenv
            from pathlib import Path
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
            from datetime import datetime
            
            experiment_name = "recsys_production"
            experiment = mlflow.get_experiment_by_name(experiment_name)
            if experiment is None:
                experiment_id = mlflow.create_experiment(experiment_name)
            else:
                experiment_id = experiment.experiment_id
            
            with mlflow.start_run(
                run_name=f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                experiment_id=experiment_id,
            ):
                # Логирование параметров валидации
                mlflow.log_params({
                    "validation_size": validation_size,
                    "degradation_threshold": degradation_threshold,
                    "warm_users": warm_count,
                    "cold_users": cold_count,
                })
                
                # Логирование всех метрик (заменяем @ на _ для совместимости с MLflow)
                mlflow_metrics = {k.replace("@", "_"): v for k, v in all_metrics.items()}
                mlflow.log_metrics(mlflow_metrics)
                
                # Логирование информации о деградации
                if degradation:
                    mlflow.log_params({
                        "degradation_detected": True,
                        "degraded_metrics_count": len(degradation["degraded_metrics"]),
                    })
                    for metric, info in degradation["degraded_metrics"].items():
                        # Заменяем @ на _ в имени метрики для совместимости с MLflow
                        metric_name = metric.replace("@", "_")
                        mlflow.log_metric(f"degradation_{metric_name}_change_pct", info["change_pct"])
                else:
                    mlflow.log_params({"degradation_detected": False})
                
                # Логирование файла с метриками (опционально, ошибки S3 не блокируют выполнение)
                try:
                    mlflow.log_artifact(str(metrics_path), "validation")
                except Exception:
                    pass
        except ImportError:
            logger.warning("MLflow не установлен, пропускаем логирование")
        except Exception as e:
            logger.warning(f"Ошибка логирования в MLflow: {e}")
    
    # 13. Проверка деградации и выброс ValueError (ПОСЛЕ логирования в MLflow)
    if degradation:
        # Проверяем, достаточно ли test interactions для надежной валидации
        min_test_interactions = 100
        if len(val_interactions_target) < min_test_interactions:
            logger.warning(f"Деградация обнаружена, но test interactions мало ({len(val_interactions_target):,}), не блокируем развертывание")
        else:
            logger.error("Обнаружена деградация модели!")
            for metric, info in degradation["degraded_metrics"].items():
                logger.error(f"{metric}: {info['previous']:.4f} -> {info['current']:.4f} ({info['change_pct']:.1f}%)")
            raise ValueError(f"Model degradation detected: {degradation['degraded_metrics']}")
    
    logger.info("Валидация завершена успешно")
    
    return {
        "status": "success",
        "metrics": all_metrics,
        "degradation": degradation,
        "warm_users": warm_count,
        "cold_users": cold_count,
        "validation_size": len(val_interactions),
    }


def main(
    model_path: str | Path,
    data_dir: str | Path,
    config_path: str | Path,
    artifacts_dir: str | Path,
    previous_metrics_path: str | Path | None = None,
    degradation_threshold: float = 0.1,
    validation_size: float = 0.1,
) -> dict[str, Any]:
    """Точка входа для валидации модели."""
    return validate_model(
        model_path=Path(model_path),
        data_dir=Path(data_dir),
        config_path=Path(config_path),
        artifacts_dir=Path(artifacts_dir),
        previous_metrics_path=Path(previous_metrics_path) if previous_metrics_path else None,
        degradation_threshold=degradation_threshold,
        validation_size=validation_size,
    )


if __name__ == "__main__":
    """Поддержка запуска из командной строки."""
    import argparse
    
    p = argparse.ArgumentParser(
        description="Валидация обученной модели рекомендательной системы"
    )
    p.add_argument("--model-path", type=str, required=True, help="Путь к модели (als_model.pkl)")
    p.add_argument("--data-dir", type=str, required=True, help="Директория с данными (events.csv)")
    p.add_argument("--config-path", type=str, required=True, help="Путь к конфигурационному файлу")
    p.add_argument("--artifacts-dir", type=str, required=True, help="Директория с артефактами")
    p.add_argument("--previous-metrics-path", type=str, default=None, help="Путь к метрикам предыдущей модели")
    p.add_argument("--degradation-threshold", type=float, default=0.1, help="Порог деградации (0.1 = 10%%)")
    p.add_argument("--validation-size", type=float, default=0.1, help="Доля данных для валидации (0.1 = 10%%)")
    args = p.parse_args()
    
    main(
        model_path=args.model_path,
        data_dir=args.data_dir,
        config_path=args.config_path,
        artifacts_dir=args.artifacts_dir,
        previous_metrics_path=args.previous_metrics_path,
        degradation_threshold=args.degradation_threshold,
        validation_size=args.validation_size,
    )

