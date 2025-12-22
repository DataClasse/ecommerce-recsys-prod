"""Модуль для генерации рекомендаций с интеллектуальным fallback для cold start."""

from __future__ import annotations

from typing import Optional

import numpy as np
import polars as pl

from ..matrix import Mappings


def generate_recommendations(
    model,
    user_item_matrix,
    mappings: Mappings,
    warm_test_users: list[int],
    popular_items: list[int],
    eval_k: int,
    test_interactions_df: Optional[pl.DataFrame] = None,
    item_metadata: Optional[pl.DataFrame] = None,
    user_recent_events: Optional[pl.DataFrame] = None,
) -> tuple[pl.DataFrame, int, int]:
    """Генерация рекомендаций с интеллектуальным fallback для cold start.
    
    Для warm users использует модель ALS для генерации рекомендаций.
    Для cold users применяет интеллектуальный fallback:
    - Если доступны метаданные и история событий: рекомендует товары из категорий,
      которые пользователь просматривал (на основе последних событий)
    - Иначе: использует простой fallback на популярные товары
    
    Args:
        model: Обученная модель ALS
        user_item_matrix: Матрица взаимодействий пользователь-товар (CSR)
        mappings: Маппинги ID пользователей и товаров
        warm_test_users: Список ID warm пользователей (есть в train)
        popular_items: Список популярных товаров для fallback
        eval_k: Количество рекомендаций для генерации
        test_interactions_df: DataFrame с тестовыми взаимодействиями (для определения cold users)
        item_metadata: Метаданные товаров (item_id, category_id) - опционально
        user_recent_events: Недавние события пользователей (user_id, item_id, event) - опционально
    
    Returns:
        Кортеж (recs, warm_count, cold_count):
        - recs: DataFrame с рекомендациями (user_id, item_id, rank)
        - warm_count: Количество warm пользователей
        - cold_count: Количество cold пользователей
    """
    recs_rows = []
    warm_count = 0
    # Генерируем минимум 20 рекомендаций для корректной оценки Recall@20
    # Если eval_k < 20, генерируем 20, иначе используем eval_k
    N = max(eval_k, 20)

    # Генерация рекомендаций для warm users
    for user_id in warm_test_users:
        user_idx = mappings.user_index[int(user_id)]
        user_items_row = user_item_matrix[user_idx]
        ids, _scores = model.recommend(
            userid=user_idx,
            user_items=user_items_row,
            N=N,
            filter_already_liked_items=True
        )

        # Фильтруем только те item_idx, которые есть в mappings
        # Это защищает от ошибок, когда модель возвращает индексы вне диапазона
        valid_mask = np.isin(ids, list(mappings.index_to_item.keys()))
        valid_ids = ids[valid_mask]
        
        # Создание записей рекомендаций
        n_valid = len(valid_ids)
        if n_valid > 0:
            ranks = np.arange(1, n_valid + 1, dtype=np.int32)
            item_ids = np.array([mappings.index_to_item[int(item_idx)] for item_idx in valid_ids], dtype=np.int64)
            user_ids_array = np.full(n_valid, user_id, dtype=np.int64)
            
            # Добавляем все записи сразу
            for i in range(n_valid):
                recs_rows.append({
                    "user_id": int(user_ids_array[i]),
                    "item_id": int(item_ids[i]),
                    "rank": int(ranks[i])
                })
        warm_count += 1

    # Определение cold users
    if test_interactions_df is None:
        cold_test_users = []
    else:
        test_users_df = test_interactions_df.select("user_id").unique()
        cold_test_users = (
            test_users_df
            .filter(~pl.col("user_id").is_in(warm_test_users))
            .get_column("user_id")
            .to_list()
        )
    cold_count = len(cold_test_users)

    # Генерация рекомендаций для cold users
    if len(cold_test_users) > 0:
        if item_metadata is not None and user_recent_events is not None:
            # Интеллектуальный fallback: используем категории из истории
            item_categories = (
                item_metadata
                .select(["item_id", "category_id"])
            )
            # Используем N рекомендаций для интеллектуального fallback
            popular_items_df = pl.DataFrame({"item_id": popular_items[:N]})
            popular_items_with_cats = popular_items_df.join(
                item_categories,
                on="item_id",
                how="left"
            )

            user_recent_events_with_cats = (
                user_recent_events
                .filter(pl.col("user_id").is_in(cold_test_users))
                .join(
                    item_categories,
                    on="item_id",
                    how="left"
                )
            )

            # Группируем категории по пользователям
            user_categories_map = (
                user_recent_events_with_cats
                .filter(pl.col("category_id").is_not_null())
                .select(["user_id", "category_id"])
                .unique()
                .group_by("user_id")
                .agg(pl.col("category_id"))
            )

            cold_recs_rows = []
            for user_id in cold_test_users:
                user_row = user_categories_map.filter(pl.col("user_id") == user_id)
                if len(user_row) > 0:
                    user_cats_list = user_row.get_column("category_id").explode().to_list()
                    category_items = (
                        popular_items_with_cats
                        .filter(pl.col("category_id").is_in(user_cats_list))
                        .get_column("item_id")
                        .head(N)
                        .to_list()
                    )
                    if len(category_items) < N:
                        used_items_set = set(category_items)
                        remaining = [
                            item for item in popular_items
                            if item not in used_items_set
                        ][:N - len(category_items)]
                        category_items.extend(remaining)
                    items_to_use = category_items[:N]
                else:
                    items_to_use = popular_items[:N]

                for rank, item_id in enumerate(items_to_use, start=1):
                    cold_recs_rows.append({
                        "user_id": user_id,
                        "item_id": item_id,
                        "rank": rank
                    })

            recs_rows.extend(cold_recs_rows)
        else:
            # Простой fallback: популярные товары 
            n_cold = len(cold_test_users)
            n_items = min(N, len(popular_items))
            cold_user_ids = np.repeat(cold_test_users, n_items)
            cold_item_ids = np.tile(popular_items[:n_items], n_cold)
            cold_ranks = np.tile(np.arange(1, n_items + 1), n_cold)
            cold_recs_df = pl.DataFrame({
                "user_id": cold_user_ids,
                "item_id": cold_item_ids,
                "rank": cold_ranks
            })
            recs_rows.extend(cold_recs_df.to_dicts())

    # Формируем итоговый DataFrame
    if recs_rows:
        recs = pl.DataFrame(recs_rows)
    else:
        recs = pl.DataFrame({"user_id": [], "item_id": [], "rank": []})
    
    return recs, warm_count, cold_count

