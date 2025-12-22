from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def load_item_properties(
    data_dir: str | Path,
    part1: str = "item_properties_part1.csv",
    part2: str = "item_properties_part2.csv",
) -> pl.DataFrame:
    """Загружает и объединяет свойства товаров из файлов part1 и part2.
    
    Загружает сырые данные без валидации. Для валидации используйте функцию
    validate_item_properties() из модуля validation после загрузки.
    
    Колонки автоматически переименовываются: itemid->item_id.
    
    Args:
        data_dir: Директория, содержащая CSV файлы
        part1: Имя файла part1
        part2: Имя файла part2
    
    Returns:
        Объединённый DataFrame с колонками: timestamp, item_id, property, value
    """
    data_dir = Path(data_dir)
    
    # Загрузка part1
    lf1 = pl.scan_csv(
        data_dir / part1,
        dtypes={
            "timestamp": pl.Int64,
            "itemid": pl.Int64,
            "property": pl.Utf8,
            "value": pl.Utf8,
        },
        ignore_errors=False,
    )
    
    # Загрузка part2
    lf2 = pl.scan_csv(
        data_dir / part2,
        dtypes={
            "timestamp": pl.Int64,
            "itemid": pl.Int64,
            "property": pl.Utf8,
            "value": pl.Utf8,
        },
        ignore_errors=False,
    )
    
    # Объединение обеих частей и переименование колонок
    df = (
        pl.concat([lf1, lf2])
        .collect(streaming=True)
        .rename({"itemid": "item_id"})
    )
    
    return df


def load_category_tree(
    data_dir: str | Path,
    filename: str = "category_tree.csv",
) -> pl.DataFrame:
    """Загружает дерево категорий с отношениями родитель-потомок.
    
    Загружает сырые данные без валидации. Для валидации используйте функцию
    validate_category_tree() из модуля validation после загрузки.
    
    Args:
        data_dir: Директория, содержащая CSV файл
        filename: Имя файла дерева категорий
    
    Returns:
        DataFrame с колонками: category_id, parent_id
    """
    data_dir = Path(data_dir)
    
    df = (
        pl.scan_csv(
            data_dir / filename,
            dtypes={
                "categoryid": pl.Int64,
                "parentid": pl.Utf8,  # Может быть пустой строкой для корневых категорий
            },
            ignore_errors=False,
        )
        .collect(streaming=True)
        # Преобразуем пустые строки в null для parentid
        .with_columns(
            pl.when(pl.col("parentid") == "")
            .then(None)
            .otherwise(pl.col("parentid").cast(pl.Int64))
            .alias("parentid")
        )
        # Переименовываем колонки
        .rename({
            "categoryid": "category_id",
            "parentid": "parent_id",
        })
    )
    
    return df

