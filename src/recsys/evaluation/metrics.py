from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)


def precision_recall_ndcg_at_k(
    test_interactions: pl.DataFrame,
    recs: pl.DataFrame,
    k: int,
) -> dict[str, float]:
    """Вычисляет Precision@k, Recall@k, nDCG@k для warm пользователей.

    Предполагает:
    - `test_interactions` содержит колонки: user_id, item_id
    - `recs` содержит колонки: user_id, item_id, rank (1..k)
    """
    if k <= 0:
        raise ValueError("k must be positive")

    test_pairs = test_interactions.select(["user_id", "item_id"]).unique()
    recs_k = recs.filter(pl.col("rank") <= k).select(["user_id", "item_id", "rank"])

    common_users = np.intersect1d(
        recs_k.get_column("user_id").unique().to_numpy(),
        test_pairs.get_column("user_id").unique().to_numpy(),
    )
    if common_users.size == 0:
        return {"precision@k": 0.0, "recall@k": 0.0, "ndcg@k": 0.0}

    test_u = test_pairs.filter(pl.col("user_id").is_in(common_users))
    recs_u = recs_k.filter(pl.col("user_id").is_in(common_users)).sort(
        ["user_id", "rank"]
    )

    hits = recs_u.join(
        test_u.with_columns(pl.lit(1).alias("_rel")),
        on=["user_id", "item_id"],
        how="left",
        coalesce=True,
    ).with_columns(pl.col("_rel").fill_null(0).cast(pl.Int8).alias("rel"))

    # Precision и Recall
    hits_per_user = hits.group_by("user_id").agg(pl.col("rel").sum().alias("h"))
    rel_per_user = test_u.group_by("user_id").len().rename({"len": "r"})
    pr = hits_per_user.join(rel_per_user, on="user_id", how="inner").select(
        [
            (pl.col("h") / float(k)).alias("p"),
            (pl.col("h") / pl.col("r")).alias("rec"),
        ]
    )
    precision = float(pr.get_column("p").mean())
    recall = float(pr.get_column("rec").mean())

    # Вычисление nDCG@k
    discounts = 1.0 / np.log2(np.arange(2, k + 2, dtype=np.float32))
    hits_np = hits.select(["user_id", "rel", "rank"]).to_numpy()
    # Колонки: user_id, rel, rank
    user_ids = hits_np[:, 0].astype(np.int64, copy=False)
    rel = hits_np[:, 1].astype(np.float32, copy=False)
    rank = hits_np[:, 2].astype(np.int64, copy=False)
    dcg_part = rel * discounts[rank - 1]

    # Суммируем DCG по пользователям (векторизовано через argsort + reduceat)
    order = np.argsort(user_ids, kind="mergesort")
    u_sorted = user_ids[order]
    dcg_sorted = dcg_part[order]
    _, idx_start = np.unique(u_sorted, return_index=True)
    dcg = np.add.reduceat(dcg_sorted, idx_start)

    # IDCG для каждого пользователя: min(r, k) лучших попаданий
    r = rel_per_user.filter(pl.col("user_id").is_in(common_users)).sort("user_id")
    r_np = r.get_column("r").to_numpy().astype(np.int64, copy=False)
    r_clip = np.minimum(r_np, k)
    cumsum_disc = np.concatenate([[0.0], np.cumsum(discounts)])
    idcg = cumsum_disc[r_clip]
    ndcg = float(np.mean(np.divide(dcg, idcg, out=np.zeros_like(dcg), where=idcg > 0)))
    ndcg = float(np.clip(ndcg, 0.0, 1.0))

    return {"precision@k": precision, "recall@k": recall, "ndcg@k": ndcg}


def hit_rate_at_k(
    test_interactions: pl.DataFrame,
    recs: pl.DataFrame,
    k: int,
) -> float:
    """Вычисляет Hit Rate@k: доля пользователей с хотя бы одним попаданием.

    Hit Rate@k = (количество пользователей с ≥1 релевантным товаром в top-k) / (всего пользователей)

    Предполагает:
    - `test_interactions` содержит колонки: user_id, item_id
    - `recs` содержит колонки: user_id, item_id, rank (1..k)
    """
    if k <= 0:
        raise ValueError("k must be positive")

    test_pairs = test_interactions.select(["user_id", "item_id"]).unique()
    
    # Обработка пустых рекомендаций (cold пользователи)
    if len(recs) == 0:
        return 0.0
    
    recs_k = recs.filter(pl.col("rank") <= k).select(["user_id", "item_id"])
    
    # Фильтруем null user_id (не должно происходить, но проверка безопасности)
    recs_k = recs_k.filter(pl.col("user_id").is_not_null())

    if len(recs_k) == 0:
        return 0.0

    # Находим пользователей с хотя бы одним попаданием
    hits = recs_k.join(test_pairs, on=["user_id", "item_id"], how="inner")
    users_with_hits = hits.select("user_id").unique()
    
    total_test_users = test_pairs.select("user_id").unique()
    
    if len(total_test_users) == 0:
        return 0.0
    
    hit_rate = len(users_with_hits) / len(total_test_users)
    return float(hit_rate)


def coverage_at_k(
    recs: pl.DataFrame,
    catalog_size: int,
    k: int,
) -> float:
    """Вычисляет Coverage@k: доля товаров каталога, появляющихся в top-k рекомендациях.

    Coverage@k = (количество уникальных товаров в top-k рекомендациях) / (размер каталога)

    Предполагает:
    - `recs` содержит колонки: user_id, item_id, rank (1..k)
    - `catalog_size` - общее количество товаров в каталоге (например, из обучающей выборки)
    """
    if k <= 0:
        raise ValueError("k must be positive")
    if catalog_size <= 0:
        raise ValueError("catalog_size must be positive")

    if len(recs) == 0:
        return 0.0

    recs_k = recs.filter(pl.col("rank") <= k).filter(pl.col("item_id").is_not_null())
    unique_items = recs_k.select("item_id").unique()
    
    if len(unique_items) == 0:
        return 0.0
    
    coverage = len(unique_items) / float(catalog_size)
    return float(min(coverage, 1.0))  # Ограничиваем до [0, 1]


def novelty_at_k(
    recs: pl.DataFrame,
    train_interactions: pl.DataFrame,
    k: int,
) -> float:
    """Вычисляет Novelty@k: средняя самоинформация рекомендованных товаров

    Novelty@k = - (1/N) * Σ log2(popularity(item_i))
    
    где popularity(item_i) = frequency(item_i) / total_interactions в обучающей выборке.
    Более высокая новизна = менее популярные товары (более разнообразные рекомендации).

    Предполагает:
    - `recs` содержит колонки: user_id, item_id, rank (1..k)
    - `train_interactions` содержит колонки: item_id (и опционально user_id)
    """
    if k <= 0:
        raise ValueError("k must be positive")

    if len(recs) == 0:
        return 0.0

    recs_k = recs.filter(pl.col("rank") <= k).filter(pl.col("item_id").is_not_null())
    
    if len(recs_k) == 0:
        return 0.0

    # Вычисляем популярность товаров в обучающей выборке
    item_counts = train_interactions.group_by("item_id").len().rename({"len": "count"})
    total_interactions = len(train_interactions)
    
    if total_interactions == 0:
        return 0.0

    # Объединяем с рекомендациями и вычисляем самоинформацию
    recs_with_pop = recs_k.join(item_counts, on="item_id", how="left", coalesce=True)
    recs_with_pop = recs_with_pop.with_columns(
        pl.col("count").fill_null(1).alias("count")  # Неизвестные товары получают min count=1
    )
    
    # Популярность = count / total_interactions
    # Самоинформация = -log2(popularity) = -log2(count) + log2(total_interactions)
    recs_with_pop = recs_with_pop.with_columns(
        (pl.col("count").cast(pl.Float64) / float(total_interactions)).alias("pop")
    ).with_columns(
        (-pl.col("pop").log(base=2.0)).alias("self_info")
    )
    
    # Средняя самоинформация (выше = более новое)
    novelty = float(recs_with_pop.get_column("self_info").mean())
    
    # Обработка граничных случаев (не должно происходить с корректными данными)
    if np.isnan(novelty) or np.isinf(novelty):
        return 0.0
    
    return novelty


def diversity_at_k(
    recs: pl.DataFrame,
    item_metadata: pl.DataFrame,
    k: int,
) -> float:
    """Вычисляет Diversity@k: среднее количество уникальных категорий в top-k рекомендациях на пользователя.

    Diversity@k = (1/N) * Σ unique_categories(user_i, top-k)
    
    Более высокое разнообразие = больше категорий представлено в рекомендациях.
    Измеряет, насколько разнообразны рекомендации с точки зрения категорий товаров.

    Предполагает:
    - `recs` содержит колонки: user_id, item_id, rank (1..k)
    - `item_metadata` содержит колонки: item_id, category_id
    """
    if k <= 0:
        raise ValueError("k must be positive")

    if len(recs) == 0:
        return 0.0

    recs_k = recs.filter(pl.col("rank") <= k).filter(pl.col("item_id").is_not_null())
    
    if len(recs_k) == 0:
        return 0.0

    # Объединяем с метаданными для получения категорий
    recs_with_cats = recs_k.join(
        item_metadata.select(["item_id", "category_id"]),
        on="item_id",
        how="left",
        coalesce=True,
    ).filter(pl.col("category_id").is_not_null())

    if len(recs_with_cats) == 0:
        return 0.0

    # Подсчитываем уникальные категории на пользователя
    unique_cats_per_user = (
        recs_with_cats
        .select(["user_id", "category_id"])
        .unique()
        .group_by("user_id")
        .len()
        .rename({"len": "n_categories"})
    )

    if len(unique_cats_per_user) == 0:
        return 0.0

    # Среднее количество уникальных категорий на пользователя
    diversity = float(unique_cats_per_user.get_column("n_categories").mean())
    
    # Обработка граничных случаев
    if np.isnan(diversity) or np.isinf(diversity):
        return 0.0
    
    return diversity


def cart_prediction_rate(
    test_events: pl.DataFrame,
    recs: pl.DataFrame,
    k: int,
) -> float:
    """Вычисляет Cart Prediction Rate@k: доля пользователей, добавивших рекомендованные товары в корзину.
    
    Cart Prediction Rate@k = (количество пользователей с ≥1 рекомендованным товаром в корзине) / (всего пользователей с рекомендациями)
    
    Эта метрика измеряет, насколько хорошо рекомендации соответствуют реальному поведению покупок (добавление в корзину).
    
    Предполагает:
    - `test_events` содержит колонки: user_id, item_id, event
    - `recs` содержит колонки: user_id, item_id, rank (1..k)
    """
    if k <= 0:
        raise ValueError("k must be positive")
    
    # Извлекаем события addtocart
    addtocart_events = (
        test_events
        .filter(pl.col("event") == "addtocart")
        .select(["user_id", "item_id"])
        .unique()
    )
    
    if len(addtocart_events) == 0:
        return 0.0
    
    # Получаем top-k рекомендаций
    recs_k = recs.filter(pl.col("rank") <= k).select(["user_id", "item_id"])
    recs_k = recs_k.filter(pl.col("user_id").is_not_null())
    
    if len(recs_k) == 0:
        return 0.0
    
    # Находим рекомендации, которые были добавлены в корзину
    hits = recs_k.join(addtocart_events, on=["user_id", "item_id"], how="inner")
    users_with_cart_hits = hits.select("user_id").unique()
    total_recs_users = recs_k.select("user_id").unique()
    
    if len(total_recs_users) == 0:
        return 0.0
    
    return len(users_with_cart_hits) / len(total_recs_users)


def compute_all_metrics(
    test_interactions: pl.DataFrame,
    recs: pl.DataFrame,
    train_interactions: pl.DataFrame,
    catalog_size: int,
    eval_k: int,
    model_name: str,
    item_metadata: Optional[pl.DataFrame] = None,
    test_interactions_target: Optional[pl.DataFrame] = None,
    test_events: Optional[pl.DataFrame] = None,
) -> dict[str, float]:
    """Вычисляет все метрики для рекомендательной системы.
    
    Args:
        test_interactions: Тестовые взаимодействия (user_id, item_id)
        recs: Рекомендации (user_id, item_id, rank)
        train_interactions: Обучающие взаимодействия (user_id, item_id, rating)
        catalog_size: Размер каталога (количество уникальных товаров)
        eval_k: Значение k для метрик @k
        model_name: Название модели (для логирования)
        item_metadata: Метаданные товаров (item_id, category_id) - опционально
        test_interactions_target: Целевые тестовые взаимодействия (для гибридного подхода) - опционально
        test_events: Тестовые события (user_id, item_id, event) - опционально
    
    Returns:
        Словарь с метриками:
        - recall@20, precision@5, ndcg@10, hit_rate@5 (PRIMARY)
        - recall@50 (МОНИТОРИНГ)
        - precision@k, recall@k, ndcg@k, hit_rate@k (по eval_k)
        - cart_prediction_rate@5 (если test_events предоставлен)
        - coverage@k, novelty@k (SECONDARY)
        - diversity@k (если item_metadata предоставлен)
    """
    metrics = {}
    
    if test_interactions_target is None:
        test_interactions_target = test_interactions
    
    # PRIMARY метрики (гибридный подход)
    metrics_at_20 = precision_recall_ndcg_at_k(test_interactions_target, recs, k=20)
    metrics["recall@20"] = metrics_at_20["recall@k"]
    
    metrics_at_5 = precision_recall_ndcg_at_k(test_interactions_target, recs, k=5)
    metrics["precision@5"] = metrics_at_5["precision@k"]
    
    metrics_at_10 = precision_recall_ndcg_at_k(test_interactions_target, recs, k=10)
    metrics["ndcg@10"] = metrics_at_10["ndcg@k"]
    
    metrics["hit_rate@5"] = hit_rate_at_k(test_interactions_target, recs, k=5)
    
    # МОНИТОРИНГ
    metrics_at_50 = precision_recall_ndcg_at_k(test_interactions_target, recs, k=50)
    metrics["recall@50"] = metrics_at_50["recall@k"]
    
    # Метрики по eval_k
    metrics_at_k = precision_recall_ndcg_at_k(test_interactions_target, recs, k=eval_k)
    metrics["precision@k"] = metrics_at_k["precision@k"]
    metrics["recall@k"] = metrics_at_k["recall@k"]
    metrics["ndcg@k"] = metrics_at_k["ndcg@k"]
    metrics["hit_rate@k"] = hit_rate_at_k(test_interactions_target, recs, k=eval_k)
    
    # Cart prediction rate (если есть test_events)
    if test_events is not None:
        metrics["cart_prediction_rate@5"] = cart_prediction_rate(test_events, recs, k=5)
    
    # SECONDARY метрики (guardrails)
    metrics["coverage@k"] = coverage_at_k(recs, catalog_size, k=eval_k)
    metrics["novelty@k"] = novelty_at_k(recs, train_interactions, k=eval_k)
    
    # Diversity (если есть метаданные)
    if item_metadata is not None:
        metrics["diversity@k"] = diversity_at_k(recs, item_metadata, k=eval_k)
    
    # Логирование результатов
    logger.info(f"МЕТРИКИ {model_name} (гибридный подход, по addtocart/transaction):")
    logger.info("  PRIMARY:")
    logger.info(f"    recall@20: {metrics['recall@20']:.6f} (основная метрика оптимизации)")
    logger.info(f"    precision@5: {metrics['precision@5']:.6f} (критична для бизнеса)")
    logger.info(f"    ndcg@10: {metrics['ndcg@10']:.6f} (качество ранжирования)")
    logger.info(f"    hit_rate@5: {metrics['hit_rate@5']:.6f} (покрытие пользователей)")
    if "cart_prediction_rate@5" in metrics:
        logger.info(f"    cart_prediction_rate@5: {metrics['cart_prediction_rate@5']:.6f} (доля рекомендаций с addtocart)")
    logger.info("  МОНИТОРИНГ:")
    logger.info(f"    recall@50: {metrics['recall@50']:.6f}")
    logger.info("  SECONDARY (guardrails):")
    logger.info(f"    coverage@{eval_k}: {metrics['coverage@k']:.6f}")
    logger.info(f"    novelty@{eval_k}: {metrics['novelty@k']:.6f}")
    if "diversity@k" in metrics:
        logger.info(f"    diversity@{eval_k}: {metrics['diversity@k']:.6f}")
    
    return metrics
