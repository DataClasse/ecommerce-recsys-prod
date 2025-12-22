from __future__ import annotations

import polars as pl


def top_popular_items(interactions: pl.DataFrame, k: int) -> list[int]:
    """Базовый алгоритм популярности по суммарному рейтингу (взвешенные взаимодействия).
    
    Args:
        interactions: DataFrame с взаимодействиями (item_id, rating)
        k: Количество топ-товаров для возврата
    
    Returns:
        Список ID топ-k популярных товаров
    """
    items = (
        interactions.group_by("item_id")
        .agg(pl.col("rating").sum().alias("score"))
        .sort("score", descending=True)
        .head(k)
        .get_column("item_id")
        .to_list()
    )
    return [int(x) for x in items]


def top_popular_items_by_cart(
    events: pl.DataFrame,
    k: int,
    weight_addtocart: float = 10.0,
    weight_transaction: float = 8.0,
) -> list[int]:
    """Популярные товары по добавлениям в корзину и покупкам.
    
    Вычисляет популярность товаров на основе событий addtocart и transaction
    с учетом весов. Игнорирует события view для фокуса на более значимых действиях.
    
    Args:
        events: DataFrame с событиями (item_id, event)
        k: Количество топ-товаров для возврата
        weight_addtocart: Вес события addtocart
        weight_transaction: Вес события transaction
    
    Returns:
        Список ID топ-k популярных товаров
    """
    # Фильтруем только значимые события (addtocart, transaction)
    cart_events = events.filter(
        pl.col("event").is_in(["addtocart", "transaction"])
    )
    
    if len(cart_events) == 0:
        return []
    
    items = (
        cart_events
        .with_columns([
            pl.when(pl.col("event") == "addtocart")
            .then(weight_addtocart)
            .when(pl.col("event") == "transaction")
            .then(weight_transaction)
            .otherwise(0.0)
            .cast(pl.Float32)
            .alias("score")
        ])
        .group_by("item_id")
        .agg(pl.col("score").sum().alias("total_score"))
        .sort("total_score", descending=True)
        .head(k)
        .get_column("item_id")
        .to_list()
    )
    
    return [int(x) for x in items]


