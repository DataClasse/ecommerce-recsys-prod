from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def load_events(
    data_dir: str | Path,
) -> pl.DataFrame:
    """Загружает события RetailRocket из CSV с эффективным использованием памяти через Polars.
    
    Загружает сырые данные без валидации. Для валидации используйте функцию
    validate_events() из модуля validation после загрузки.
    
    CSV хранит timestamp в миллисекундах с начала эпохи.
    Колонки автоматически переименовываются в стандартные имена:
    - visitorid -> user_id (для единообразия с interactions)
    - itemid -> item_id
    - transactionid -> transaction_id

    Args:
        data_dir: Директория, содержащая events.csv

    Returns:
        DataFrame с колонками: timestamp, user_id, item_id, event, transaction_id
    """
    data_dir = Path(data_dir)
    path = data_dir / "events.csv"
    
    # Проверка существования файла
    if not path.exists():
        raise FileNotFoundError(
            f"Файл events.csv не найден в директории: {data_dir}\n"
            f"Ожидаемый путь: {path}"
        )
    
    # Имена колонок CSV (формат RetailRocket)
    CSV_COLUMNS = {
        "timestamp": pl.Int64,
        "visitorid": pl.Int64,
        "itemid": pl.Int64,
        "event": pl.Utf8,
        "transactionid": pl.Utf8,
    }
    
    lf = pl.scan_csv(
        path,
        dtypes=CSV_COLUMNS,
        ignore_errors=False,
    )
    
    df = (
        lf.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").alias("timestamp")
        )
        .collect(streaming=True)
        .rename({
            "visitorid": "user_id",
            "itemid": "item_id",
            "transactionid": "transaction_id",
            # timestamp, event остаются без изменений
        })
    )
    
    return df


