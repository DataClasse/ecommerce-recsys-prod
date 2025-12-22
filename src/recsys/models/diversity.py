"""Модуль для применения ограничений разнообразия к рекомендациям."""

from __future__ import annotations

import polars as pl


def apply_diversity_constraint(
    recommendations: pl.DataFrame,  # user_id, item_id, rank
    item_metadata: pl.DataFrame,  # item_id, category_id
    max_items_per_category: int = 2,
    popular_items_by_category: dict[int, list[int]] | None = None,
) -> pl.DataFrame:
    """Применяет ограничение разнообразия к рекомендациям.

    Алгоритм:
    1. Для каждого пользователя группирует рекомендации по категориям
    2. Если в категории больше max_items_per_category товаров:
       - Оставляет топ max_items_per_category по rank (лучшие)
       - Заменяет остальные на товары из категорий с < max_items_per_category
    3. Использует популярные товары из недостающих категорий (если доступны)
    4. Возвращает обновлённые рекомендации

    Args:
        recommendations: DataFrame с рекомендациями (user_id, item_id, rank)
        item_metadata: DataFrame с метаданными товаров (item_id, category_id)
        max_items_per_category: Максимальное количество товаров из одной категории
        popular_items_by_category: Словарь {category_id: [item_id, ...]} популярных товаров по категориям

    Returns:
        DataFrame с обновлёнными рекомендациями (user_id, item_id, rank)
    """
    if len(recommendations) == 0:
        return recommendations

    # Добавляем категории к рекомендациям
    recs_with_cats = recommendations.join(
        item_metadata.select(["item_id", "category_id"]),
        on="item_id",
        how="left",
        coalesce=True,
    )

    # Группируем по пользователям и применяем constraint
    diverse_recs_rows = []

    for user_id in recs_with_cats.select("user_id").unique().get_column("user_id").to_list():
        user_recs = recs_with_cats.filter(pl.col("user_id") == user_id).sort("rank")

        # Группируем по категориям
        category_groups = {}
        items_without_cat = []

        for row in user_recs.iter_rows(named=True):
            cat_id = row["category_id"]
            if cat_id is None:
                items_without_cat.append(row)
            else:
                if cat_id not in category_groups:
                    category_groups[cat_id] = []
                category_groups[cat_id].append(row)

        # Применяем constraint: оставляем топ max_items_per_category из каждой категории
        selected_items = []
        category_counts = {}

        # Сначала выбираем товары, которые не превышают лимит
        for cat_id, items in category_groups.items():
            sorted_items = sorted(items, key=lambda x: x["rank"])
            category_counts[cat_id] = len(sorted_items)

            if len(sorted_items) <= max_items_per_category:
                # Всё оставляем
                selected_items.extend(sorted_items)
            else:
                # Оставляем только топ max_items_per_category
                selected_items.extend(sorted_items[:max_items_per_category])

        # Добавляем товары без категорий (без ограничений, но в конец)
        selected_items.extend(items_without_cat)

        # Подсчитываем, сколько категорий уже представлено
        selected_categories = set(
            item["category_id"] for item in selected_items if item.get("category_id") is not None
        )

        # Если нужно добавить категории из популярных
        if popular_items_by_category is not None and len(selected_items) < len(user_recs):
            needed = len(user_recs) - len(selected_items)

            # Ищем категории, которых ещё нет
            for cat_id, popular_items in popular_items_by_category.items():
                if cat_id not in selected_categories and needed > 0:
                    # Добавляем популярный товар из этой категории
                    for item_id in popular_items:
                        if item_id not in [item["item_id"] for item in selected_items]:
                            selected_items.append({
                                "user_id": user_id,
                                "item_id": item_id,
                                "rank": len(selected_items) + 1,
                                "category_id": cat_id,
                            })
                            selected_categories.add(cat_id)
                            needed -= 1
                            if needed == 0:
                                break
                    if needed == 0:
                        break

        # Пересчитываем ранги
        selected_items_sorted = sorted(selected_items, key=lambda x: x.get("rank", 999))
        for new_rank, item in enumerate(selected_items_sorted, start=1):
            diverse_recs_rows.append({
                "user_id": item["user_id"],
                "item_id": item["item_id"],
                "rank": new_rank,
            })

    if not diverse_recs_rows:
        return recommendations

    return pl.DataFrame(diverse_recs_rows)


def build_popular_items_by_category(
    train_interactions: pl.DataFrame,
    item_metadata: pl.DataFrame,
    k_per_category: int = 10,
) -> dict[int, list[int]]:
    """Строит словарь популярных товаров по категориям для использования в diversity constraint.

    Args:
        train_interactions: DataFrame с взаимодействиями (item_id, rating)
        item_metadata: DataFrame с метаданными (item_id, category_id)
        k_per_category: Количество популярных товаров на категорию

    Returns:
        Словарь {category_id: [item_id, ...]} популярных товаров по категориям
    """
    # Подсчитываем популярность товаров (по сумме rating)
    item_popularity = (
        train_interactions
        .group_by("item_id")
        .agg(pl.col("rating").sum().alias("total_rating"))
        .sort("total_rating", descending=True)
    )

    # Добавляем категории
    item_pop_with_cats = item_popularity.join(
        item_metadata.select(["item_id", "category_id"]),
        on="item_id",
        how="left",
        coalesce=True,
    ).filter(pl.col("category_id").is_not_null())

    # Группируем по категориям и берём топ-k
    popular_by_category = {}

    for cat_id in item_pop_with_cats.select("category_id").unique().get_column("category_id").to_list():
        cat_items = (
            item_pop_with_cats
            .filter(pl.col("category_id") == cat_id)
            .head(k_per_category)
            .get_column("item_id")
            .to_list()
        )
        popular_by_category[int(cat_id)] = [int(item_id) for item_id in cat_items]

    return popular_by_category

