"""Модуль для обработки метаданных товаров.

На основе выводов EDA:
- Извлечение категорий товаров (с учётом истории изменений)
- Подсчёт статистики по категориям
- Вычисление глубины категорий в дереве
- Извлечение доступности товаров
- Подсчёт популярности товаров
"""

from __future__ import annotations

import polars as pl


def extract_latest_categories(item_properties: pl.DataFrame) -> pl.DataFrame:
    """Извлекает последнюю категорию для каждого товара.
    
    В item_properties содержится история изменений категорий.
    Для каждого товара берётся актуальная категория (max timestamp).
    
    Args:
        item_properties: DataFrame с полями item_id, property, value, timestamp
    
    Returns:
        DataFrame с колонками: item_id, category_id
    """
    categories = (
        item_properties
        .filter(pl.col("property") == "categoryid")
        .select([
            pl.col("item_id"),
            pl.col("value").cast(pl.Int64).alias("category_id"),
            pl.col("timestamp"),
        ])
    )
    
    latest = (
        categories
        .sort(["item_id", "timestamp"], descending=[False, True])
        .group_by("item_id")
        .first()
        .select(["item_id", "category_id"])
    )
    
    return latest


def calculate_category_depth(category_tree: pl.DataFrame) -> pl.DataFrame:
    """Вычисляет глубину каждой категории итеративно через векторизованные операции.
    
    Использует итеративный join для вычисления уровней (быстрее чем рекурсия).
    
    Args:
        category_tree: DataFrame с полями category_id, parent_id
    
    Returns:
        DataFrame с колонками: category_id, level
    """
    # Векторизованное вычисление глубины через итеративный join
    # Создаем DataFrame с категориями и их родителями
    cat_parent_df = category_tree.select(["category_id", "parent_id"])
    
    # Инициализируем уровень для категорий без родителей (level = 1)
    depths_df = cat_parent_df.with_columns(
        pl.when(pl.col("parent_id").is_null())
        .then(pl.lit(1))
        .otherwise(None)
        .alias("level")
    )
    
    # Итеративно вычисляем уровни для категорий с родителями
    # Максимум итераций = максимальная глубина дерева (обычно < 10)
    max_iterations = 20
    for _ in range(max_iterations):
        # Объединяем с родителями для получения их уровней
        depths_df = depths_df.join(
            depths_df.select(["category_id", "level"]).rename({"category_id": "parent_id", "level": "parent_level"}),
            on="parent_id",
            how="left"
        ).with_columns(
            pl.when(pl.col("level").is_null())
            .then(pl.col("parent_level") + 1)
            .otherwise(pl.col("level"))
            .alias("level")
        ).select(["category_id", "parent_id", "level"])
        
        # Если все уровни вычислены, выходим
        if depths_df.filter(pl.col("level").is_null()).height == 0:
            break
    
    return depths_df.select(["category_id", "level"])


def extract_category_stats(
    events: pl.DataFrame,
    latest_categories: pl.DataFrame,
) -> pl.DataFrame:
    """Подсчитывает статистику по категориям на основе событий.
    
    Args:
        events: DataFrame с событиями (user_id, item_id, event)
        latest_categories: DataFrame с категориями товаров (item_id, category_id)
    
    Returns:
        DataFrame с колонками: category_id, n_events, n_users, n_items
    """
    events_with_cats = events.join(
        latest_categories,
        on="item_id",
        how="left",
        coalesce=True
    )
    
    stats = (
        events_with_cats
        .filter(pl.col("category_id").is_not_null())
        .group_by("category_id")
        .agg([
            pl.len().alias("n_events"),
            pl.n_unique("user_id").alias("n_users"),
            pl.n_unique("item_id").alias("n_items"),
        ])
        .sort("n_events", descending=True)
    )
    
    return stats


def extract_item_availability(item_properties: pl.DataFrame) -> pl.DataFrame:
    """Извлекает последнее значение доступности для каждого товара.
    
    Args:
        item_properties: DataFrame с полями item_id, property, value, timestamp
    
    Returns:
        DataFrame с колонками: item_id, available_value
    """
    available = (
        item_properties
        .filter(pl.col("property") == "available")
        .select([
            pl.col("item_id"),
            pl.col("value").alias("available_value"),
            pl.col("timestamp"),
        ])
    )
    
    latest = (
        available
        .sort(["item_id", "timestamp"], descending=[False, True])
        .group_by("item_id")
        .first()
        .select(["item_id", "available_value"])
    )
    
    return latest


def calculate_item_popularity(
    events: pl.DataFrame,
    weight_view: float = 1.0,
    weight_addtocart: float = 4.0,
    weight_transaction: float = 8.0,
) -> pl.DataFrame:
    """Подсчитывает популярность товаров с учётом весов событий.
    
    Args:
        events: DataFrame с событиями (item_id, event)
        weight_view: Вес события "view"
        weight_addtocart: Вес события "addtocart"
        weight_transaction: Вес события "transaction"
    
    Returns:
        DataFrame с колонками: item_id, n_events, n_users, weighted_score
    """
    events_with_weights = events.with_columns([
        pl.when(pl.col("event") == "view")
        .then(weight_view)
        .when(pl.col("event") == "addtocart")
        .then(weight_addtocart)
        .when(pl.col("event") == "transaction")
        .then(weight_transaction)
        .otherwise(weight_view)
        .cast(pl.Float32)
        .alias("event_weight")
    ])
    
    popularity = (
        events_with_weights
        .group_by("item_id")
        .agg([
            pl.len().alias("n_events"),
            pl.n_unique("user_id").alias("n_users"),
            pl.col("event_weight").sum().alias("weighted_score")
        ])
        .sort("weighted_score", descending=True)
        .select(["item_id", "n_events", "n_users", "weighted_score"])
    )
    
    return popularity

