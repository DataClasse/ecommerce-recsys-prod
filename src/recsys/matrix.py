from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl
import scipy.sparse as sp


@dataclass(frozen=True)
class Mappings:
    user_index: dict[int, int]
    item_index: dict[int, int]
    index_to_user: dict[int, int]
    index_to_item: dict[int, int]


def build_user_item_matrix(
    interactions: pl.DataFrame,
) -> tuple[sp.csr_matrix, Mappings]:
    """Строит CSR матрицу пользователь-товар и стабильные маппинги ID.
    
    Args:
        interactions: DataFrame с взаимодействиями (user_id, item_id, rating)
    
    Returns:
        Кортеж (csr_matrix, Mappings):
        - csr_matrix: Разреженная CSR матрица взаимодействий
        - Mappings: Маппинги между внешними и внутренними ID
    """
    df = interactions.select(["user_id", "item_id", "rating"]).with_columns(
        pl.col("rating").cast(pl.Float32)
    )

    # Получаем уникальных пользователей и товары, создаём маппинги
    unique_users = df.select("user_id").unique().sort("user_id").get_column("user_id").to_list()
    unique_items = df.select("item_id").unique().sort("item_id").get_column("item_id").to_list()
    
    user_index = {int(uid): int(idx) for idx, uid in enumerate(unique_users)}
    item_index = {int(iid): int(idx) for idx, iid in enumerate(unique_items)}
    index_to_user = {int(idx): int(uid) for idx, uid in enumerate(unique_users)}
    index_to_item = {int(idx): int(iid) for idx, iid in enumerate(unique_items)}

    # Маппим в плотные индексы используя numpy
    user_arr = df.get_column("user_id").to_numpy()
    item_arr = df.get_column("item_id").to_numpy()

    # Используем int64 для индексов (требуется библиотекой implicit)
    user_cats_arr = np.array(unique_users, dtype=np.int64)
    item_cats_arr = np.array(unique_items, dtype=np.int64)
    user_idx_arr = np.searchsorted(user_cats_arr, user_arr, side='left').astype(np.int64)
    item_idx_arr = np.searchsorted(item_cats_arr, item_arr, side='left').astype(np.int64)
    # Используем float64 для значений (ожидается библиотекой implicit)
    r = df.get_column("rating").to_numpy().astype(np.float64, copy=False)

    n_users = int(user_cats_arr.shape[0])
    n_items = int(item_cats_arr.shape[0])

    # Создаём COO матрицу с индексами int64 и значениями float64
    coo = sp.coo_matrix((r, (user_idx_arr, item_idx_arr)), shape=(n_users, n_items), dtype=np.float64)
    csr = coo.tocsr()
    # Убеждаемся, что индексы int32 (требование библиотеки implicit)
    csr.indices = csr.indices.astype(np.int32, copy=False)
    csr.indptr = csr.indptr.astype(np.int32, copy=False)

    return csr, Mappings(
        user_index=user_index,
        item_index=item_index,
        index_to_user=index_to_user,
        index_to_item=index_to_item,
    )


