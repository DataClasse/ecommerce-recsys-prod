from __future__ import annotations

import json
import logging
import pickle
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path

import polars as pl
import scipy.sparse as sp

from .matrix import Mappings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelPaths:
    model_pkl: Path
    metadata_json: Path
    user_item_npz: Path
    popular_json: Path
    user_purchases_parquet: Path | None = None


def save_artifacts(
    artifacts_dir: str | Path,
    model,
    user_item: sp.csr_matrix,
    mappings: Mappings,
    popular_items: list[int],
    params: dict,
    user_purchases: pl.DataFrame | None = None,
) -> ModelPaths:
    artifacts_dir = Path(artifacts_dir)
    model_dir = artifacts_dir / "model"
    model_dir.mkdir(parents=True, exist_ok=True)

    model_pkl = model_dir / "als_model.pkl"
    metadata_json = model_dir / "metadata.json"
    user_item_npz = model_dir / "user_item_matrix.npz"
    popular_json = model_dir / "popular.json"

    with open(model_pkl, "wb") as f:
        pickle.dump(model, f, protocol=pickle.HIGHEST_PROTOCOL)

    sp.save_npz(user_item_npz, user_item)

    with open(popular_json, "w", encoding="utf-8") as f:
        json.dump({"popular": popular_items}, f, ensure_ascii=False, indent=2)

    # Сохранение покупок (для фильтрации в API)
    user_purchases_parquet: Path | None = None
    if user_purchases is not None and len(user_purchases) > 0:
        user_purchases_parquet = artifacts_dir / "user_purchases.parquet"
        user_purchases.write_parquet(user_purchases_parquet)
        logger.info(f"Saved user purchases: {len(user_purchases):,} unique transactions")

    meta = {
        "params": params,
        "mappings": asdict(mappings),
        "artifacts": {
            "model_pkl": str(model_pkl),
            "user_item_npz": str(user_item_npz),
            "popular_json": str(popular_json),
        },
        "shapes": {"n_users": int(user_item.shape[0]), "n_items": int(user_item.shape[1])},
    }
    with open(metadata_json, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return ModelPaths(
        model_pkl=model_pkl,
        metadata_json=metadata_json,
        user_item_npz=user_item_npz,
        popular_json=popular_json,
        user_purchases_parquet=user_purchases_parquet,
    )


# Глобальный кэш для артефактов (в памяти)
_artifact_cache: dict[str, pl.DataFrame] = {}


def clear_artifact_cache() -> None:
    """Очищает кэш артефактов.
    
    Полезно при пересоздании артефактов с новыми именами колонок.
    """
    global _artifact_cache
    _artifact_cache.clear()
    # Также очищаем LRU кэш
    load_artifact_cached.cache_clear()


@lru_cache(maxsize=5)
def load_artifact_cached(artifact_path: str) -> pl.DataFrame:
    """Кэшированная загрузка артефактов с использованием LRU кэша.
    
    Args:
        artifact_path: Путь к parquet файлу с артефактом
    
    Returns:
        Загруженный DataFrame
    """
    return pl.read_parquet(artifact_path)


def _validate_item_metadata(df: pl.DataFrame) -> pl.DataFrame:
    """Валидирует метаданные товаров.
    
    Проверяет наличие обязательных колонок: item_id, category_id
    
    Args:
        df: DataFrame с метаданными товаров
    
    Returns:
        Валидированный DataFrame
    
    Raises:
        ValueError: Если отсутствуют обязательные колонки
    """
    required_cols = {"item_id", "category_id"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"В item_metadata отсутствуют обязательные колонки: {missing_cols}\n"
            f"Найденные колонки: {list(df.columns)}"
        )
    
    # Проверка типов данных
    if df["item_id"].dtype not in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
        logger.warning(f"item_id имеет неожиданный тип: {df['item_id'].dtype}, ожидается целочисленный")
    
    if df["category_id"].dtype not in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
        logger.warning(f"category_id имеет неожиданный тип: {df['category_id'].dtype}, ожидается целочисленный")
    
    return df


def get_item_metadata(project_root: Path) -> pl.DataFrame:
    """Загрузка метаданных товаров с кэшированием в памяти.
    
    Args:
        project_root: Корневая директория проекта
    
    Returns:
        DataFrame с метаданными товаров (item_id, category_id, ...)
    
    Raises:
        FileNotFoundError: Если файл не найден
        ValueError: Если отсутствуют обязательные колонки
    """
    cache_key = "item_metadata"
    if cache_key not in _artifact_cache:
        path = project_root / "artifacts" / "item_metadata.parquet"
        
        # Проверка существования файла
        if not path.exists():
            raise FileNotFoundError(
                f"Файл item_metadata.parquet не найден в директории: {project_root / 'artifacts'}\n"
                f"Ожидаемый путь: {path}"
            )
        
        df = pl.read_parquet(path)
        df = _validate_item_metadata(df)
        _artifact_cache[cache_key] = df
        logger.info(f"Загружено метаданных товаров: {len(df):,} записей")
    
    return _artifact_cache[cache_key]


def _validate_category_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Валидирует статистику категорий.
    
    Проверяет наличие обязательных колонок: category_id, n_events, n_users, n_items
    
    Args:
        df: DataFrame со статистикой категорий
    
    Returns:
        Валидированный DataFrame
    
    Raises:
        ValueError: Если отсутствуют обязательные колонки
    """
    required_cols = {"category_id", "n_events", "n_users", "n_items"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"В category_stats отсутствуют обязательные колонки: {missing_cols}\n"
            f"Найденные колонки: {list(df.columns)}"
        )
    
    # Проверка типов данных
    if df["category_id"].dtype not in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
        logger.warning(f"category_id имеет неожиданный тип: {df['category_id'].dtype}, ожидается целочисленный")
    
    numeric_cols = ["n_events", "n_users", "n_items"]
    for col in numeric_cols:
        if df[col].dtype not in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
            logger.warning(f"{col} имеет неожиданный тип: {df[col].dtype}, ожидается целочисленный")
    
    return df


def get_category_stats(project_root: Path) -> pl.DataFrame:
    """Загрузка статистики категорий с кэшированием в памяти.
    
    Args:
        project_root: Корневая директория проекта
    
    Returns:
        DataFrame со статистикой категорий (category_id, n_events, n_users, n_items)
    
    Raises:
        FileNotFoundError: Если файл не найден
        ValueError: Если отсутствуют обязательные колонки
    """
    cache_key = "category_stats"
    if cache_key not in _artifact_cache:
        path = project_root / "artifacts" / "category_stats.parquet"
        
        # Проверка существования файла
        if not path.exists():
            raise FileNotFoundError(
                f"Файл category_stats.parquet не найден в директории: {project_root / 'artifacts'}\n"
                f"Ожидаемый путь: {path}"
            )
        
        df = pl.read_parquet(path)
        df = _validate_category_stats(df)
        _artifact_cache[cache_key] = df
        logger.info(f"Загружено статистики категорий: {len(df):,} записей")
    
    return _artifact_cache[cache_key]


def _validate_category_tree(df: pl.DataFrame) -> pl.DataFrame:
    """Валидирует дерево категорий.
    
    Проверяет наличие обязательных колонок: category_id, parent_id
    
    Args:
        df: DataFrame с деревом категорий
    
    Returns:
        Валидированный DataFrame
    
    Raises:
        ValueError: Если отсутствуют обязательные колонки
    """
    required_cols = {"category_id", "parent_id"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"В category_tree отсутствуют обязательные колонки: {missing_cols}\n"
            f"Найденные колонки: {list(df.columns)}"
        )
    
    # Проверка типов данных
    if df["category_id"].dtype not in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
        logger.warning(f"category_id имеет неожиданный тип: {df['category_id'].dtype}, ожидается целочисленный")
    
    if df["parent_id"].dtype not in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32, pl.Null):
        logger.warning(f"parent_id имеет неожиданный тип: {df['parent_id'].dtype}, ожидается целочисленный или Null")
    
    return df


def get_category_tree_enhanced(project_root: Path) -> pl.DataFrame:
    """Загрузка дерева категорий с кэшированием в памяти.
    
    Args:
        project_root: Корневая директория проекта
    
    Returns:
        DataFrame с деревом категорий (category_id, parent_id, level)
    
    Raises:
        FileNotFoundError: Если файл не найден
        ValueError: Если отсутствуют обязательные колонки
    """
    cache_key = "category_tree"
    if cache_key not in _artifact_cache:
        path = project_root / "artifacts" / "category_tree.parquet"
        
        # Проверка существования файла
        if not path.exists():
            raise FileNotFoundError(
                f"Файл category_tree.parquet не найден в директории: {project_root / 'artifacts'}\n"
                f"Ожидаемый путь: {path}"
            )
        
        df = pl.read_parquet(path)
        df = _validate_category_tree(df)
        _artifact_cache[cache_key] = df
        logger.info(f"Загружено дерева категорий: {len(df):,} записей")
    
    return _artifact_cache[cache_key]


