"""Трансформации для данных взаимодействий: временное затухание, фильтрация покупок, субсемплирование."""

from __future__ import annotations

import polars as pl


def apply_time_decay(
    interactions: pl.DataFrame,
    max_timestamp: int | None = None,
    decay_period_days: float = 14.0,
) -> pl.DataFrame:
    """Применяет экспоненциальное временное затухание к весам взаимодействий.

    Args:
        interactions: DataFrame с колонками: user_id, item_id, rating, timestamp
        max_timestamp: Максимальный timestamp (если None, вычисляется из данных)
        decay_period_days: Период затухания в днях (параметр τ). События старше τ дней
                          получают ~36.8% от исходного веса.

    Returns:
        DataFrame с затухшими весами в колонке 'rating'.
    """
    if max_timestamp is None:
        max_ts_val = interactions.select(pl.col("timestamp").max()).item()
        # Преобразуем в int, если это datetime или другой тип
        if isinstance(max_ts_val, (float, int)):
            max_timestamp = int(max_ts_val)
        else:
            # Если это datetime, преобразуем в миллисекунды (предполагая Unix timestamp)
            import datetime
            if isinstance(max_ts_val, datetime.datetime):
                max_timestamp = int(max_ts_val.timestamp() * 1000)
            else:
                max_timestamp = int(max_ts_val)

    # Убеждаемся, что колонка timestamp числовая (преобразуем при необходимости)
    # Проверяем, является ли timestamp datetime и преобразуем в int (миллисекунды)
    ts_dtype = interactions.schema.get("timestamp")
    if ts_dtype and "Datetime" in str(ts_dtype):
        # Преобразуем datetime в миллисекунды (Unix timestamp * 1000)
        interactions = interactions.with_columns(
            pl.col("timestamp").dt.timestamp("ms").cast(pl.Int64).alias("timestamp")
        )
        # Также преобразуем max_timestamp, если это datetime
        import datetime
        if isinstance(max_timestamp, datetime.datetime):
            max_timestamp = int(max_timestamp.timestamp() * 1000)

    # Преобразуем timestamp (мс) в дни
    # timestamp теперь должен быть int в миллисекундах
    ms_per_day = 24 * 60 * 60 * 1000
    
    # Убеждаемся, что оба значения int для вычитания
    max_ts_expr = pl.lit(max_timestamp, dtype=pl.Int64)
    days_ago = (
        (max_ts_expr - pl.col("timestamp")) / ms_per_day
    ).alias("days_ago")

    # Экспоненциальное затухание: weight * exp(-days_ago / decay_period)
    decay_factor = (-days_ago / decay_period_days).exp()
    decayed_rating = (pl.col("rating") * decay_factor).alias("rating")

    result = (
        interactions
        .with_columns([days_ago, decayed_rating])
        .drop("days_ago")
    )

    return result


def filter_purchased_items(
    interactions: pl.DataFrame,
    purchased_interactions: pl.DataFrame | None = None,
    filter_mode: str = "exclude",
) -> pl.DataFrame:
    """Фильтрует взаимодействия на основе купленных товаров.

    Args:
        interactions: DataFrame со всеми взаимодействиями (user_id, item_id, ...)
        purchased_interactions: DataFrame с купленными товарами (user_id, item_id)
                               Если None, фильтрует товары с событиями transaction
        filter_mode: "exclude" - удалить купленные товары (по умолчанию)
                    "keep" - оставить только купленные товары

    Returns:
        Отфильтрованный DataFrame.
    """
    if purchased_interactions is None:
        # Предполагаем, что interactions содержат тип события или нужно вывести из контекста
        # Пока требуем, чтобы purchased_interactions был предоставлен
        raise ValueError(
            "purchased_interactions must be provided or interactions must have event column"
        )

    purchased_pairs = purchased_interactions.select(["user_id", "item_id"]).unique()

    if filter_mode == "exclude":
        # Anti-join: удаляем купленные товары
        result = interactions.join(
            purchased_pairs,
            on=["user_id", "item_id"],
            how="anti",
        )
    else:  # keep
        # Inner join: оставляем только купленные товары
        result = interactions.join(
            purchased_pairs,
            on=["user_id", "item_id"],
            how="inner",
        )

    return result


def subsample_views(
    events: pl.DataFrame,
    max_views_per_user: int = 10,
    strategy: str = "last",
) -> pl.DataFrame:
    """Субсемплирует события просмотра для уменьшения доминирования просмотров.

    Args:
        events: DataFrame с событиями с колонками: user_id, item_id, event, timestamp
        max_views_per_user: Максимальное количество событий просмотра для сохранения на пользователя
        strategy: "last" - сохранить последние N просмотров (самые свежие)
                 "first" - сохранить первые N просмотров
                 "random" - случайная выборка N просмотров

    Returns:
        Отфильтрованный DataFrame с событиями.
    """
    # Сохраняем исходный порядок колонок
    original_columns = events.columns
    
    view_events = events.filter(pl.col("event") == "view")
    non_view_events = events.filter(pl.col("event") != "view")

    if strategy == "last":
        # Сохраняем последние N просмотров на пользователя (самые свежие)
        sampled_views = (
            view_events
            .sort(["user_id", "timestamp"], descending=[False, True])
            .group_by("user_id")
            .head(max_views_per_user)
            .select(original_columns)  # Восстанавливаем исходный порядок колонок
        )
    elif strategy == "first":
        # Сохраняем первые N просмотров на пользователя
        sampled_views = (
            view_events
            .sort(["user_id", "timestamp"])
            .group_by("user_id")
            .head(max_views_per_user)
            .select(original_columns)  # Восстанавливаем исходный порядок колонок
        )
    elif strategy == "random":
        # Случайная выборка N просмотров на пользователя
        sampled_views = (
            view_events
            .group_by("user_id")
            .sample(n=max_views_per_user, seed=42)
            .select(original_columns)  # Восстанавливаем исходный порядок колонок
        )
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    # Объединяем выбранные просмотры с событиями, не являющимися просмотрами
    # Убеждаемся, что оба DataFrame имеют одинаковый порядок колонок
    if len(non_view_events) == 0:
        result = sampled_views
    elif len(sampled_views) == 0:
        result = non_view_events
    else:
        # Убеждаемся, что оба DataFrame имеют одинаковый порядок колонок
        result = pl.concat([non_view_events.select(original_columns), sampled_views.select(original_columns)])

    return result


def filter_purchased_categories(
    recs: pl.DataFrame,
    purchased_interactions: pl.DataFrame,
    item_metadata: pl.DataFrame,
    category_tree: pl.DataFrame | None = None,
    heavy_category_ids: list[int] | None = None,
) -> pl.DataFrame:
    """Исключает товары из категорий, где пользователь уже совершил покупку.
    
    Это помогает избегать рекомендаций товаров из категорий, где пользователь
    уже выразил интерес через покупку. Может применяться только к "тяжелым" категориям
    (например, крупным покупкам) через параметр heavy_category_ids.
    
    Args:
        recs: DataFrame с рекомендациями (user_id, item_id, rank)
        purchased_interactions: DataFrame с купленными товарами (user_id, item_id)
        item_metadata: DataFrame с метаданными товаров (item_id, category_id)
        category_tree: DataFrame с деревом категорий (category_id, parent_id) - опционально
        heavy_category_ids: Список ID категорий для фильтрации (если None, фильтрует все)
    
    Returns:
        Отфильтрованный DataFrame с рекомендациями (user_id, item_id, rank)
    """
    # Получаем категории купленных товаров
    purchased_with_cats = (
        purchased_interactions
        .join(
            item_metadata.select(["item_id", "category_id"]),
            on="item_id",
            how="left",
            coalesce=True
        )
        .filter(pl.col("category_id").is_not_null())
    )
    
    # Фильтруем только "тяжелые" категории, если указано
    if heavy_category_ids:
        purchased_with_cats = purchased_with_cats.filter(
            pl.col("category_id").is_in(heavy_category_ids)
        )
    
    # Получаем уникальные пары (user_id, category_id) купленных категорий
    purchased_categories = purchased_with_cats.select(["user_id", "category_id"]).unique()
    
    # Добавляем категории к рекомендациям
    recs_with_cats = recs.join(
        item_metadata.select(["item_id", "category_id"]),
        on="item_id",
        how="left",
        coalesce=True
    )
    
    # Исключаем рекомендации из купленных категорий (anti-join)
    filtered = (
        recs_with_cats
        .join(
            purchased_categories,
            on=["user_id", "category_id"],
            how="anti"
        )
        .select(["user_id", "item_id", "rank"])
    )
    
    return filtered

