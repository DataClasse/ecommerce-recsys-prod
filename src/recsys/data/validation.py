"""Модуль валидации и очистки данных для production ETL."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import polars as pl

logger = logging.getLogger(__name__)

# Валидные типы событий
VALID_EVENTS = {"view", "addtocart", "transaction"}

# Разумные границы для timestamp (миллисекунды с начала эпохи)
# Примерно 2010-01-01 до 2030-01-01
MIN_TIMESTAMP_MS = int(datetime(2010, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
MAX_TIMESTAMP_MS = int(datetime(2030, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def validate_events(
    events: pl.DataFrame,
    log_stats: bool = True,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Валидирует и очищает DataFrame с событиями.
    
    Выполняет следующие проверки:
    - Удаляет строки с null в критических полях (user_id, item_id, timestamp)
    - Фильтрует некорректные типы событий
    - Удаляет события с timestamp вне разумных границ
    - Удаляет дубликаты
    - Проверяет, что user_id и item_id положительные числа
    
    Args:
        events: DataFrame с событиями (user_id, item_id, event, timestamp, transaction_id)
        log_stats: Логировать статистику валидации
    
    Returns:
        Кортеж (очищенный DataFrame, статистика удалённых записей):
        - Очищенный DataFrame с валидными событиями
        - Словарь со статистикой: {"initial_rows", "nulls_removed", "invalid_events", 
          "invalid_timestamps", "invalid_ids", "duplicates_removed", "final_rows"}
    """
    stats = {
        "initial_rows": len(events),
        "nulls_removed": 0,
        "invalid_events": 0,
        "invalid_timestamps": 0,
        "invalid_ids": 0,
        "duplicates_removed": 0,
        "final_rows": 0,
    }
    
    if len(events) == 0:
        logger.warning("Пустой DataFrame событий")
        return events, stats
    
    # Проверка наличия обязательных колонок
    required_cols = {"user_id", "item_id", "timestamp", "event"}
    missing_cols = required_cols - set(events.columns)
    if missing_cols:
        raise ValueError(f"Отсутствуют обязательные колонки: {missing_cols}")
    
    df = events.clone()
    
    # 1. Удаление строк с null в критических полях
    initial_count = len(df)
    df = df.filter(
        pl.col("user_id").is_not_null()
        & pl.col("item_id").is_not_null()
        & pl.col("timestamp").is_not_null()
        & pl.col("event").is_not_null()
    )
    stats["nulls_removed"] = initial_count - len(df)
    
    if len(df) == 0:
        logger.error("Все события удалены из-за null значений")
        stats["final_rows"] = 0
        return df, stats
    
    # 2. Проверка типов событий
    initial_count = len(df)
    df = df.filter(pl.col("event").is_in(list(VALID_EVENTS)))
    stats["invalid_events"] = initial_count - len(df)
    
    if len(df) == 0:
        logger.error("Все события удалены из-за некорректных типов событий")
        stats["final_rows"] = 0
        return df, stats
    
    # 3. Проверка timestamp (должен быть datetime или int в миллисекундах)
    # Преобразуем datetime в int, если нужно
    if df["timestamp"].dtype in (pl.Datetime, pl.Datetime("ms"), pl.Datetime("us")):
        df = df.with_columns(
            pl.col("timestamp").dt.timestamp("ms").cast(pl.Int64).alias("timestamp")
        )
    elif df["timestamp"].dtype != pl.Int64:
        # Пытаемся преобразовать в int64
        try:
            df = df.with_columns(pl.col("timestamp").cast(pl.Int64).alias("timestamp"))
        except Exception as e:
            logger.error(f"Не удалось преобразовать timestamp: {e}")
            stats["final_rows"] = 0
            return df.select([]), stats
    
    # Фильтруем timestamp вне разумных границ
    initial_count = len(df)
    df = df.filter(
        (pl.col("timestamp") >= MIN_TIMESTAMP_MS)
        & (pl.col("timestamp") <= MAX_TIMESTAMP_MS)
    )
    stats["invalid_timestamps"] = initial_count - len(df)
    
    if len(df) == 0:
        logger.error("Все события удалены из-за некорректных timestamp")
        stats["final_rows"] = 0
        return df, stats
    
    # 4. Проверка user_id и item_id (должны быть положительными)
    initial_count = len(df)
    df = df.filter(
        (pl.col("user_id") > 0) & (pl.col("item_id") > 0)
    )
    stats["invalid_ids"] = initial_count - len(df)
    
    if len(df) == 0:
        logger.error("Все события удалены из-за некорректных ID")
        stats["final_rows"] = 0
        return df, stats
    
    # 5. Удаление дубликатов (полные дубликаты по всем полям)
    initial_count = len(df)
    df = df.unique()
    stats["duplicates_removed"] = initial_count - len(df)
    
    stats["final_rows"] = len(df)
    
    # Логирование статистики
    if log_stats:
        logger.info("Статистика валидации событий:")
        logger.info(f"  Исходных записей: {stats['initial_rows']:,}")
        if stats["nulls_removed"] > 0:
            logger.warning(f"  Удалено записей с null: {stats['nulls_removed']:,}")
        if stats["invalid_events"] > 0:
            logger.warning(f"  Удалено записей с некорректными событиями: {stats['invalid_events']:,}")
        if stats["invalid_timestamps"] > 0:
            logger.warning(f"  Удалено записей с некорректными timestamp: {stats['invalid_timestamps']:,}")
        if stats["invalid_ids"] > 0:
            logger.warning(f"  Удалено записей с некорректными ID: {stats['invalid_ids']:,}")
        if stats["duplicates_removed"] > 0:
            logger.info(f"  Удалено дубликатов: {stats['duplicates_removed']:,}")
        logger.info(f"  Финальных записей: {stats['final_rows']:,} ({stats['final_rows']/stats['initial_rows']*100:.2f}%)")
    
    return df, stats


def validate_item_properties(
    item_properties: pl.DataFrame,
    log_stats: bool = True,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Валидирует и очищает DataFrame со свойствами товаров.
    
    Args:
        item_properties: DataFrame со свойствами (item_id, property, value, timestamp)
        log_stats: Логировать статистику валидации
    
    Returns:
        Кортеж (очищенный DataFrame, статистика)
    """
    stats = {
        "initial_rows": len(item_properties),
        "nulls_removed": 0,
        "invalid_timestamps": 0,
        "invalid_ids": 0,
        "duplicates_removed": 0,
        "final_rows": 0,
    }
    
    if len(item_properties) == 0:
        logger.warning("Пустой DataFrame свойств товаров")
        return item_properties, stats
    
    # Проверка наличия обязательных колонок
    required_cols = {"item_id", "property", "value", "timestamp"}
    missing_cols = required_cols - set(item_properties.columns)
    if missing_cols:
        raise ValueError(f"Отсутствуют обязательные колонки: {missing_cols}")
    
    df = item_properties.clone()
    
    # 1. Удаление строк с null в критических полях (item_id, property, timestamp)
    # value может быть null
    initial_count = len(df)
    df = df.filter(
        pl.col("item_id").is_not_null()
        & pl.col("property").is_not_null()
        & pl.col("timestamp").is_not_null()
    )
    stats["nulls_removed"] = initial_count - len(df)
    
    if len(df) == 0:
        logger.error("Все свойства удалены из-за null значений")
        stats["final_rows"] = 0
        return df, stats
    
    # 2. Проверка timestamp (если нужно, преобразуем в int)
    if df["timestamp"].dtype in (pl.Datetime, pl.Datetime("ms"), pl.Datetime("us")):
        df = df.with_columns(
            pl.col("timestamp").dt.timestamp("ms").cast(pl.Int64).alias("timestamp")
        )
    elif df["timestamp"].dtype != pl.Int64:
        try:
            df = df.with_columns(pl.col("timestamp").cast(pl.Int64).alias("timestamp"))
        except Exception as e:
            logger.error(f"Не удалось преобразовать timestamp: {e}")
            stats["final_rows"] = 0
            return df.select([]), stats
    
    # Фильтруем timestamp вне разумных границ
    initial_count = len(df)
    df = df.filter(
        (pl.col("timestamp") >= MIN_TIMESTAMP_MS)
        & (pl.col("timestamp") <= MAX_TIMESTAMP_MS)
    )
    stats["invalid_timestamps"] = initial_count - len(df)
    
    # 3. Проверка item_id (должен быть положительным)
    initial_count = len(df)
    df = df.filter(pl.col("item_id") > 0)
    stats["invalid_ids"] = initial_count - len(df)
    
    # 4. Удаление дубликатов
    initial_count = len(df)
    df = df.unique()
    stats["duplicates_removed"] = initial_count - len(df)
    
    stats["final_rows"] = len(df)
    
    if log_stats:
        logger.info("Статистика валидации свойств товаров:")
        logger.info(f"  Исходных записей: {stats['initial_rows']:,}")
        if stats["nulls_removed"] > 0:
            logger.warning(f"  Удалено записей с null: {stats['nulls_removed']:,}")
        if stats["invalid_timestamps"] > 0:
            logger.warning(f"  Удалено записей с некорректными timestamp: {stats['invalid_timestamps']:,}")
        if stats["invalid_ids"] > 0:
            logger.warning(f"  Удалено записей с некорректными ID: {stats['invalid_ids']:,}")
        if stats["duplicates_removed"] > 0:
            logger.info(f"  Удалено дубликатов: {stats['duplicates_removed']:,}")
        logger.info(f"  Финальных записей: {stats['final_rows']:,} ({stats['final_rows']/stats['initial_rows']*100:.2f}%)")
    
    return df, stats


def validate_category_tree(
    category_tree: pl.DataFrame,
    log_stats: bool = True,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Валидирует и очищает DataFrame с деревом категорий.
    
    Args:
        category_tree: DataFrame с деревом (category_id, parent_id)
        log_stats: Логировать статистику валидации
    
    Returns:
        Кортеж (очищенный DataFrame, статистика)
    """
    stats = {
        "initial_rows": len(category_tree),
        "nulls_removed": 0,
        "invalid_ids": 0,
        "duplicates_removed": 0,
        "final_rows": 0,
    }
    
    if len(category_tree) == 0:
        logger.warning("Пустой DataFrame дерева категорий")
        return category_tree, stats
    
    # Проверка наличия обязательных колонок
    required_cols = {"category_id"}
    missing_cols = required_cols - set(category_tree.columns)
    if missing_cols:
        raise ValueError(f"Отсутствуют обязательные колонки: {missing_cols}")
    
    df = category_tree.clone()
    
    # 1. Удаление строк с null в category_id (parent_id может быть null для корневых)
    initial_count = len(df)
    df = df.filter(pl.col("category_id").is_not_null())
    stats["nulls_removed"] = initial_count - len(df)
    
    if len(df) == 0:
        logger.error("Все категории удалены из-за null значений")
        stats["final_rows"] = 0
        return df, stats
    
    # 2. Проверка category_id (должен быть положительным)
    initial_count = len(df)
    df = df.filter(pl.col("category_id") > 0)
    
    # Если есть parent_id и он не null, проверяем, что он тоже положительный
    if "parent_id" in df.columns:
        df = df.filter(
            pl.col("parent_id").is_null() | (pl.col("parent_id") > 0)
        )
    
    stats["invalid_ids"] = initial_count - len(df)
    
    # 3. Удаление дубликатов
    initial_count = len(df)
    df = df.unique()
    stats["duplicates_removed"] = initial_count - len(df)
    
    stats["final_rows"] = len(df)
    
    if log_stats:
        logger.info("Статистика валидации дерева категорий:")
        logger.info(f"  Исходных записей: {stats['initial_rows']:,}")
        if stats["nulls_removed"] > 0:
            logger.warning(f"  Удалено записей с null: {stats['nulls_removed']:,}")
        if stats["invalid_ids"] > 0:
            logger.warning(f"  Удалено записей с некорректными ID: {stats['invalid_ids']:,}")
        if stats["duplicates_removed"] > 0:
            logger.info(f"  Удалено дубликатов: {stats['duplicates_removed']:,}")
        logger.info(f"  Финальных записей: {stats['final_rows']:,} ({stats['final_rows']/stats['initial_rows']*100:.2f}%)")
    
    return df, stats

