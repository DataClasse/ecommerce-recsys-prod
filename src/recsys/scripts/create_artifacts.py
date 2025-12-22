"""Скрипт для создания артефактов.

Создаёт следующие артефакты:
- item_metadata.parquet (item_id, category_id, level, available_value, ...)
- category_stats.parquet (category_id, n_events, n_users, n_items)
- category_tree.parquet (category_id, parent_id, level)
- item_availability.parquet (item_id, available_value)
- item_popularity.parquet (item_id, n_events, n_users, weighted_score)

Все колонки используют стандартное именование:
- item_id 
- category_id
- parent_id 
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import polars as pl

from src.recsys.data.events import load_events
from src.recsys.data.item_properties import (
    load_category_tree,
    load_item_properties,
)
from src.recsys.data.validation import (
    validate_category_tree,
    validate_events,
    validate_item_properties,
)
from src.recsys.preprocess.metadata import (
    calculate_category_depth,
    calculate_item_popularity,
    extract_category_stats,
    extract_item_availability,
    extract_latest_categories,
)


def create_artifacts(
    project_root: Path,
    data_dir: Path | None = None,
    artifacts_dir: Path | None = None,
    logger: logging.Logger | None = None,
    weight_view: float = 1.0,
    weight_addtocart: float = 4.0,
    weight_transaction: float = 8.0,
    log_to_mlflow: bool = False,
) -> dict[str, Path]:
    """Создаёт все артефакты с правильными именами колонок.
    
    Args:
        project_root: Корневая директория проекта
        data_dir: Директория с исходными данными (по умолчанию project_root / "data" / "raw")
        artifacts_dir: Директория для сохранения артефактов (по умолчанию project_root / "artifacts")
        logger: Логгер для вывода информации (опционально)
        weight_view: Вес события "view" для расчёта популярности
        weight_addtocart: Вес события "addtocart" для расчёта популярности
        weight_transaction: Вес события "transaction" для расчёта популярности
    
    Returns:
        Словарь с путями к созданным артефактам:
        {
            "item_metadata": Path,
            "category_stats": Path,
            "category_tree": Path,
            "item_availability": Path,
            "item_popularity": Path,
        }
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    if data_dir is None:
        data_dir = project_root / "data" / "raw"
    if artifacts_dir is None:
        artifacts_dir = project_root / "artifacts"
    
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("Создание артефактов")
    
    # Загрузка и валидация данных
    events = load_events(data_dir)
    events, events_stats = validate_events(events, log_stats=False)
    logger.info(f"События: {len(events):,}")
    
    item_properties = load_item_properties(data_dir)
    item_properties, props_stats = validate_item_properties(item_properties, log_stats=False)
    logger.info(f"Свойства товаров: {len(item_properties):,}")
    
    category_tree = load_category_tree(data_dir)
    category_tree, tree_stats = validate_category_tree(category_tree, log_stats=False)
    logger.info(f"Категории: {len(category_tree):,}")
    
    # Извлечение категорий товаров
    latest_categories = extract_latest_categories(item_properties)
    logger.info(f"Товары с категориями: {latest_categories.select('item_id').n_unique():,}, "
                f"уникальных категорий: {latest_categories.select('category_id').n_unique():,}")
    
    # Подсчёт статистики по категориям
    category_stats = extract_category_stats(events, latest_categories)
    category_stats_path = artifacts_dir / "category_stats.parquet"
    category_stats.write_parquet(category_stats_path)
    
    # Вычисление глубины категорий
    category_levels_df = calculate_category_depth(category_tree)
    category_levels = category_tree.join(
        category_levels_df, on="category_id", how="left", coalesce=True
    )
    category_tree_path = artifacts_dir / "category_tree.parquet"
    category_levels.write_parquet(category_tree_path)
    
    # Извлечение свойства 'available'
    latest_available = extract_item_availability(item_properties)
    available_path = artifacts_dir / "item_availability.parquet"
    latest_available.write_parquet(available_path)
    
    # Подсчёт популярности товаров
    item_popularity = calculate_item_popularity(
        events,
        weight_view=weight_view,
        weight_addtocart=weight_addtocart,
        weight_transaction=weight_transaction,
    )
    popularity_path = artifacts_dir / "item_popularity.parquet"
    item_popularity.write_parquet(popularity_path)
    
    # Создание объединённого датафрейма метаданных
    item_metadata = (
        latest_categories.join(
            category_levels.select(["category_id", "level"]),
            on="category_id",
            how="left",
            coalesce=True,
        )
        .join(latest_available, on="item_id", how="left", coalesce=True)
        .join(
            item_popularity.select(
                ["item_id", "n_events", "n_users", "weighted_score"]
            ),
            on="item_id",
            how="left",
            coalesce=True,
        )
    )
    
    item_metadata = item_metadata.with_columns([
        pl.col("n_events").fill_null(0),
        pl.col("n_users").fill_null(0),
        pl.col("weighted_score").fill_null(0.0),
    ])
    
    metadata_path = artifacts_dir / "item_metadata.parquet"
    item_metadata.write_parquet(metadata_path)
    logger.info(f"Метаданные товаров: {len(item_metadata):,} товаров")
    logger.info("Создание артефактов завершено")
    
    artifacts_paths = {
        "item_metadata": metadata_path,
        "category_stats": category_stats_path,
        "category_tree": category_tree_path,
        "item_availability": available_path,
        "item_popularity": popularity_path,
    }
    
    # Логирование в MLflow (опционально)
    if log_to_mlflow:
        # Загрузка .env файла для S3 credentials (если не загружен)
        try:
            from dotenv import load_dotenv
            env_path = project_root / ".env"
            if env_path.exists():
                load_dotenv(env_path, override=False)
                # Устанавливаем MLFLOW_S3_ENDPOINT_URL из S3_ENDPOINT_URL если нужно
                import os
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
                run_name=f"create_artifacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                experiment_id=experiment_id,
            ):
                # Логирование параметров
                mlflow.log_params({
                    "data_dir": str(data_dir),
                    "artifacts_dir": str(artifacts_dir),
                    "weight_view": weight_view,
                    "weight_addtocart": weight_addtocart,
                    "weight_transaction": weight_transaction,
                })
                
                # Логирование метрик
                mlflow.log_metrics({
                    "events_count": len(events),
                    "item_properties_count": len(item_properties),
                    "category_tree_count": len(category_tree),
                    "items_with_categories": latest_categories.select("item_id").n_unique(),
                    "unique_categories": latest_categories.select("category_id").n_unique(),
                    "items_with_availability": latest_available.select("item_id").n_unique(),
                })
                
                # Логирование артефактов (опционально, ошибки S3 не блокируют выполнение)
                artifacts_logged = 0
                artifacts_failed = 0
                for name, path in artifacts_paths.items():
                    try:
                        mlflow.log_artifact(str(path), "artifacts")
                        artifacts_logged += 1
                    except Exception as artifact_error:
                        # Ошибка загрузки артефактов в S3 не критична
                        logger.warning(f"Не удалось загрузить артефакт {name} в MLflow (S3 недоступен): {artifact_error}")
                        artifacts_failed += 1
                
                if artifacts_failed > 0:
                    logger.warning(f"Не удалось загрузить {artifacts_failed} артефактов в MLflow (S3 недоступен)")
        except ImportError:
            logger.warning("MLflow не установлен, пропускаем логирование")
        except Exception as e:
            logger.warning(f"Ошибка логирования в MLflow: {e}")
    
    return artifacts_paths


def main(
    project_root: str | Path | None = None,
    data_dir: str | Path | None = None,
    artifacts_dir: str | Path | None = None,
    log_to_mlflow: bool = False,
) -> dict[str, Path]:
    """Точка входа для создания артефактов.
    
    Args:
        project_root: Корневая директория проекта (опционально, определяется автоматически)
        data_dir: Директория с исходными данными (опционально)
        artifacts_dir: Директория для сохранения артефактов (опционально)
    
    Returns:
        Словарь с путями к созданным артефактам
    """
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    
    # Определение project_root
    if project_root is None:
        # project_root на 2 уровня выше (src/recsys/scripts -> project_root)
        project_root = Path(__file__).resolve().parents[2]
    else:
        project_root = Path(project_root).resolve()
    
    if data_dir is None:
        data_dir = project_root / "data" / "raw"
    else:
        data_dir = Path(data_dir).resolve()
    
    if artifacts_dir is None:
        artifacts_dir = project_root / "artifacts"
    else:
        artifacts_dir = Path(artifacts_dir).resolve()
    
    return create_artifacts(
        project_root=project_root,
        data_dir=data_dir,
        artifacts_dir=artifacts_dir,
        logger=logger,
        log_to_mlflow=log_to_mlflow,
    )


if __name__ == "__main__":
    """Поддержка запуска из командной строки (для тестирования)."""
    import sys
    try:
        import argparse
    except ImportError:
        argparse = None
    
    if argparse is None:
        raise ImportError("argparse required for command-line execution")
    
    p = argparse.ArgumentParser(description="Создание артефактов из сырых данных")
    p.add_argument("--data-dir", type=str, default=None, help="Директория с данными")
    p.add_argument("--artifacts-dir", type=str, default=None, help="Директория для артефактов")
    args = p.parse_args()
    
    main(
        data_dir=args.data_dir if args.data_dir else None,
        artifacts_dir=args.artifacts_dir if args.artifacts_dir else None,
    )

