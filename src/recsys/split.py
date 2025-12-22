from __future__ import annotations

import polars as pl


def temporal_split_per_user(
    interactions: pl.DataFrame,
    test_size: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Временное разделение по пользователям для уменьшения проблемы 'all-cold-users'.

    Для каждого пользователя последняя доля `test_size` взаимодействий идёт в тест.
    
    Args:
        interactions: DataFrame с взаимодействиями (user_id, item_id, timestamp, ...)
        test_size: Доля тестовой выборки (0 < test_size < 1)
    
    Returns:
        Кортеж (train_df, test_df) с разделёнными взаимодействиями
    """
    if not (0.0 < test_size < 1.0):
        raise ValueError("test_size must be in (0, 1)")

    df = interactions.sort(["user_id", "timestamp"])

    with_stats = df.with_columns(
        pl.len().over("user_id").alias("_n"),
        pl.col("timestamp").rank("ordinal").over("user_id").alias("_r"),
    ).with_columns(
        (pl.col("_n") * (1.0 - test_size)).floor().cast(pl.Int64).alias("_cut")
    )

    # Сохраняем хотя бы 1 обучающее взаимодействие на пользователя.
    with_stats = with_stats.with_columns(
        pl.when(pl.col("_cut") < 1).then(1).otherwise(pl.col("_cut")).alias("_cut")
    )

    train_df = with_stats.filter(pl.col("_r") <= pl.col("_cut")).drop(
        ["_n", "_r", "_cut"]
    )
    test_df = with_stats.filter(pl.col("_r") > pl.col("_cut")).drop(
        ["_n", "_r", "_cut"]
    )
    return train_df, test_df


def temporal_split_global(
    interactions: pl.DataFrame,
    test_size: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Глобальное временное разделение по квантилю timestamp (может создать много cold пользователей).
    
    Args:
        interactions: DataFrame с взаимодействиями (user_id, item_id, timestamp, ...)
        test_size: Доля тестовой выборки (0 < test_size < 1)
    
    Returns:
        Кортеж (train_df, test_df) с разделёнными взаимодействиями
    """
    if not (0.0 < test_size < 1.0):
        raise ValueError("test_size must be in (0, 1)")

    # Сначала фильтруем null timestamps
    interactions_clean = interactions.filter(pl.col("timestamp").is_not_null())
    
    if len(interactions_clean) == 0:
        # Возвращаем пустые DataFrame, если нет валидных timestamps
        return interactions_clean, interactions_clean
    
    # Используем sort + index вместо quantile (более надёжно)
    sorted_interactions = interactions_clean.sort("timestamp")
    split_idx = int(len(sorted_interactions) * (1.0 - test_size))
    
    if split_idx == 0:
        split_idx = 1  # Обеспечиваем хотя бы 1 элемент в train
    elif split_idx >= len(sorted_interactions):
        split_idx = len(sorted_interactions) - 1  # Обеспечиваем хотя бы 1 элемент в test
    
    ts_cut = sorted_interactions[split_idx]["timestamp"][0]
    
    train_df = sorted_interactions.filter(pl.col("timestamp") < ts_cut)
    test_df = sorted_interactions.filter(pl.col("timestamp") >= ts_cut)
    return train_df, test_df


def global_time_split(
    interactions: pl.DataFrame,
    events: pl.DataFrame,
    split_date: str | None = None,
    test_size: float = 0.15,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Глобальное временное разделение данных по дате (отсечка по дате).
    
    Разделяет данные на train/test по временной метке, используя глобальную дату отсечки.
    Это более реалистичный подход для продакшена, чем per-user split.
    
    Args:
        interactions: DataFrame с взаимодействиями (user_id, item_id, timestamp, ...)
        events: DataFrame с событиями (для определения диапазона дат)
        split_date: Дата разделения в формате ISO (YYYY-MM-DD) или None для автоматического расчета
        test_size: Доля тестовой выборки (используется если split_date=None)
    
    Returns:
        Кортеж (train_df, test_df) с разделенными взаимодействиями
    """
    import datetime
    
    if isinstance(split_date, str):
        split_date = datetime.datetime.fromisoformat(split_date)
    
    if split_date is None:
        # Автоматический расчёт split_date на основе test_size
        max_ts = events.select(pl.col("timestamp").max()).item()
        min_ts = events.select(pl.col("timestamp").min()).item()
        
        if isinstance(max_ts, datetime.datetime) and isinstance(min_ts, datetime.datetime):
            time_range = (max_ts - min_ts).days
            split_date = max_ts - datetime.timedelta(days=int(time_range * test_size))
        else:
            # Если timestamps в миллисекундах
            if isinstance(max_ts, (int, float)) and isinstance(min_ts, (int, float)):
                split_ts = int(max_ts * (1.0 - test_size))
                split_date = datetime.datetime.fromtimestamp(split_ts / 1000.0)
            else:
                # Fallback: используем максимальную дату минус test_size
                if isinstance(max_ts, datetime.datetime):
                    split_date = max_ts - datetime.timedelta(days=int(365 * test_size))
                else:
                    split_ts = int(max_ts * (1.0 - test_size))
                    split_date = datetime.datetime.fromtimestamp(split_ts / 1000.0)
    
    # Преобразуем timestamp в datetime для сравнения
    interactions_with_dt = interactions.with_columns(
        pl.col("timestamp").cast(pl.Datetime(time_unit="ms")).alias("dt")
    )
    
    # Разделяем по дате
    train_df = interactions_with_dt.filter(pl.col("dt") < split_date).drop("dt")
    test_df = interactions_with_dt.filter(pl.col("dt") >= split_date).drop("dt")
    
    return train_df, test_df


