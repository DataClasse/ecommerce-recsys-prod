from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class InteractionConfig:
    min_user_interactions: int = 32
    min_item_interactions: int = 32
    weight_view: float = 1.0
    weight_addtocart: float = 4.0
    weight_transaction: float = 8.0

    user_col: str = "user_id"
    item_col: str = "item_id"
    event_col: str = "event"
    ts_col: str = "timestamp"


def build_interactions(events: pl.DataFrame, cfg: InteractionConfig) -> pl.DataFrame:
    """Строит взаимодействия (user_id, item_id, rating, timestamp).

    - rating - взвешенная сумма по типу события
    - timestamp - время последнего взаимодействия (max) для пары (user, item)
    - фильтрует пользователей и товары по минимальному количеству взаимодействий после агрегации
    
    Args:
        events: DataFrame с событиями (user_id, item_id, event, timestamp)
        cfg: Конфигурация для построения взаимодействий
    
    Returns:
        DataFrame с взаимодействиями (user_id, item_id, rating, timestamp)
    """
    e = events.select([cfg.user_col, cfg.item_col, cfg.event_col, cfg.ts_col])

    rating = (
        pl.when(pl.col(cfg.event_col) == "view")
        .then(cfg.weight_view)
        .when(pl.col(cfg.event_col) == "addtocart")
        .then(cfg.weight_addtocart)
        .when(pl.col(cfg.event_col) == "transaction")
        .then(cfg.weight_transaction)
        .otherwise(cfg.weight_view)
        .cast(pl.Float32)
        .alias("rating")
    )

    agg = (
        e.with_columns(rating)
        .group_by([cfg.user_col, cfg.item_col])
        .agg(
            pl.col("rating").sum().alias("rating"),
            pl.col(cfg.ts_col).max().alias("timestamp"),
        )
    )

    user_counts = agg.group_by(cfg.user_col).len().rename({"len": "u_cnt"})
    item_counts = agg.group_by(cfg.item_col).len().rename({"len": "i_cnt"})

    filtered = (
        agg.join(user_counts, on=cfg.user_col, how="inner", coalesce=True)
        .join(item_counts, on=cfg.item_col, how="inner", coalesce=True)
        .filter(
            (pl.col("u_cnt") >= cfg.min_user_interactions)
            & (pl.col("i_cnt") >= cfg.min_item_interactions)
        )
        .select(
            [
                pl.col(cfg.user_col).alias("user_id"),
                pl.col(cfg.item_col).alias("item_id"),
                pl.col("rating"),
                pl.col("timestamp"),
            ]
        )
        .sort(["user_id", "timestamp"])
    )

    return filtered


