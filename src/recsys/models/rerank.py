"""Модуль для переранжирования рекомендаций на основе категорий."""

from __future__ import annotations

import polars as pl


def category_based_rerank(
    recommendations: pl.DataFrame,  # user_id, item_id, rank (или score)
    user_item_history: pl.DataFrame,  # user_id, item_id, category_id
    item_metadata: pl.DataFrame,  # item_id, category_id
    category_stats: pl.DataFrame,  # category_id, n_events (популярность)
    category_tree: pl.DataFrame | None = None,  # category_id, parent_id (для похожих категорий)
    top_k: int = 5,
    alpha: float = 0.7,  # вес ALS score
    beta: float = 0.3,   # вес category score
    adaptive: bool = True,  # адаптивное переранжирование в зависимости от размера истории
) -> pl.DataFrame:
    """Переранжирует рекомендации на основе категорий.

    Для каждого пользователя:
    1. Извлекает категории из истории (топ-3 популярные категории)
    2. Для каждой рекомендации вычисляет category_score:
       - +1.0 если категория совпадает с категориями пользователя
       - +0.5 если категория похожа (родитель/ребёнок в дереве)
       - +0.2 * (popularity_normalized) для других категорий
    3. Финальный score = alpha * (1 / rank) + beta * category_score
       (используем 1/rank как proxy для ALS score)
    4. Сортирует по финальному score и возвращает top_k

    Args:
        recommendations: DataFrame с рекомендациями (user_id, item_id, rank)
        user_item_history: DataFrame с историей пользователей (user_id, item_id, category_id)
        item_metadata: DataFrame с метаданными товаров (item_id, category_id)
        category_stats: DataFrame со статистикой категорий (category_id, n_events)
        category_tree: DataFrame с деревом категорий (category_id, parent_id) - опционально
        top_k: Количество рекомендаций для возврата
        alpha: Вес ALS score (1/rank)
        beta: Вес category score

    Returns:
        DataFrame с переранжированными рекомендациями (user_id, item_id, rank)
    """
    if len(recommendations) == 0:
        return recommendations

    # Нормализуем популярность категорий
    max_popularity = category_stats.select(pl.col("n_events").max()).item()
    if max_popularity == 0:
        max_popularity = 1.0

    category_stats_normalized = category_stats.with_columns(
        (pl.col("n_events") / float(max_popularity)).alias("popularity_norm")
    )

    # Извлекаем категории из истории пользователей (топ-3 популярные)
    user_top_categories = (
        user_item_history
        .filter(pl.col("category_id").is_not_null())
        .group_by(["user_id", "category_id"])
        .len()
        .sort(["user_id", "len"], descending=[False, True])
        .group_by("user_id")
        .head(3)
        .select(["user_id", "category_id"])
    )

    # Создаём словарь категорий для каждого пользователя (векторизованно)
    user_categories_dict = {}
    if len(user_top_categories) > 0:
        user_categories_grouped = (
            user_top_categories
            .group_by("user_id")
            .agg(pl.col("category_id"))
        )
        for row in user_categories_grouped.iter_rows(named=True):
            user_id = row["user_id"]
            cat_ids = row["category_id"]
            user_categories_dict[user_id] = set(cat_ids) if isinstance(cat_ids, list) else {cat_ids}

    # Создаём словарь родительских/дочерних категорий (векторизованно)
    parent_child_map = {}
    if category_tree is not None and len(category_tree) > 0:
        tree_with_parent = category_tree.filter(pl.col("parent_id").is_not_null())
        if len(tree_with_parent) > 0:
            parent_child_pairs = tree_with_parent.select(["category_id", "parent_id"]).to_numpy()
            for cat_id, parent_id in parent_child_pairs:
                if cat_id not in parent_child_map:
                    parent_child_map[cat_id] = set()
                parent_child_map[cat_id].add(parent_id)
                if parent_id not in parent_child_map:
                    parent_child_map[parent_id] = set()
                parent_child_map[parent_id].add(cat_id)
    
    # Вычисляем длины истории пользователей для адаптивного переранжирования
    user_history_lengths = {}
    if adaptive:
        user_history_lengths = (
            user_item_history
            .group_by("user_id")
            .len()
            .rename({"len": "history_len"})
            .to_dict(as_series=False)
        )
        user_history_lengths = dict(zip(user_history_lengths["user_id"], user_history_lengths["history_len"]))

    # Добавляем категории к рекомендациям
    recs_with_cats = recommendations.join(
        item_metadata.select(["item_id", "category_id"]),
        on="item_id",
        how="left",
        coalesce=True,
    ).join(
        category_stats_normalized.select(["category_id", "popularity_norm"]),
        on="category_id",
        how="left",
        coalesce=True,
    )

    # Вычисляем финальные scores и переранжируем
    reranked_rows = []

    for user_id in recs_with_cats.select("user_id").unique().get_column("user_id").to_list():
        user_recs = recs_with_cats.filter(pl.col("user_id") == user_id)
        user_categories = user_categories_dict.get(user_id, set())
        
        # Адаптивное переранжирование: для пользователей с малой историей
        # больше веса на категории, для опытных - на ALS
        if adaptive:
            history_len = user_history_lengths.get(user_id, 0)
            if history_len < 5:
                # Новые пользователи: больше веса на категории
                user_alpha = 0.3
                user_beta = 0.7
            elif history_len < 20:
                # Пользователи со средней историей: баланс
                user_alpha = 0.5
                user_beta = 0.5
            else:
                # Опытные пользователи: больше веса на ALS
                user_alpha = alpha
                user_beta = beta
        else:
            user_alpha = alpha
            user_beta = beta

        scored_items = []

        for row in user_recs.iter_rows(named=True):
            item_id = row["item_id"]
            rank = row["rank"]
            cat_id = row.get("category_id")
            popularity_norm = row.get("popularity_norm", 0.0) or 0.0

            # ALS score (прокси через 1/rank)
            als_score = 1.0 / float(rank) if rank > 0 else 0.0

            # Category score (оценка категории)
            category_score = 0.0
            if cat_id is not None:
                if cat_id in user_categories:
                    # Прямое совпадение
                    category_score = 1.0
                elif category_tree is not None and cat_id in parent_child_map:
                    # Проверяем похожесть через дерево
                    related_cats = parent_child_map[cat_id]
                    if user_categories.intersection(related_cats):
                        category_score = 0.5
                    else:
                        # Другие категории с учётом популярности
                        category_score = 0.2 * popularity_norm
                else:
                    # Другие категории с учётом популярности
                    category_score = 0.2 * popularity_norm

            # Финальный score с адаптивными весами
            final_score = user_alpha * als_score + user_beta * category_score

            scored_items.append({
                "user_id": user_id,
                "item_id": item_id,
                "original_rank": rank,
                "final_score": final_score,
            })

        # Сортируем по final_score и берём top_k
        scored_items_sorted = sorted(scored_items, key=lambda x: x["final_score"], reverse=True)

        for new_rank, item in enumerate(scored_items_sorted[:top_k], start=1):
            reranked_rows.append({
                "user_id": item["user_id"],
                "item_id": item["item_id"],
                "rank": new_rank,
            })

    if not reranked_rows:
        return recommendations.head(0).select(["user_id", "item_id", "rank"])

    return pl.DataFrame(reranked_rows)

