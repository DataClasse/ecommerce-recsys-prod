from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl

try:
    import mlflow
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False

from src.recsys.artifacts import save_artifacts
from src.recsys.data.events import load_events
from src.recsys.data.validation import validate_events
from src.recsys.evaluation.metrics import (
    precision_recall_ndcg_at_k,
    hit_rate_at_k,
    coverage_at_k,
    novelty_at_k,
)
from src.recsys.matrix import build_user_item_matrix
from src.recsys.models.als import train_als
from src.recsys.models.popular import top_popular_items
from src.recsys.preprocess.interactions import InteractionConfig, build_interactions
from src.recsys.split import temporal_split_global, temporal_split_per_user
from src.recsys.utils.logging import setup_logging


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", type=str, required=True)
    p.add_argument("--artifacts-dir", type=str, required=True)

    p.add_argument("--min-user-interactions", type=int, default=32)
    p.add_argument("--min-item-interactions", type=int, default=32)
    p.add_argument("--test-size", type=float, default=0.15)
    p.add_argument("--split-mode", choices=["user", "global"], default="user")

    p.add_argument("--weight-view", type=float, default=1.0)
    p.add_argument("--weight-addtocart", type=float, default=4.0)
    p.add_argument("--weight-transaction", type=float, default=8.0)

    p.add_argument("--als-factors", type=int, default=64)
    p.add_argument("--als-iterations", type=int, default=25)
    p.add_argument("--als-regularization", type=float, default=0.05)
    p.add_argument("--als-alpha", type=float, default=2.0)
    p.add_argument("--als-random-state", type=int, default=42)
    p.add_argument("--als-num-threads", type=int, default=0)

    p.add_argument("--eval-k", type=int, default=5)
    p.add_argument("--popular-k", type=int, default=100)

    p.add_argument("--mlflow-project", type=str, default="mle-pr-final")
    p.add_argument("--mlflow-run-name", type=str, default=None)
    return p.parse_args()


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    args = _parse_args()

    cfg = InteractionConfig(
        min_user_interactions=args.min_user_interactions,
        min_item_interactions=args.min_item_interactions,
        weight_view=args.weight_view,
        weight_addtocart=args.weight_addtocart,
        weight_transaction=args.weight_transaction,
    )

    data_dir = Path(args.data_dir)
    artifacts_dir = Path(args.artifacts_dir)

    events = load_events(data_dir)
    events, events_stats = validate_events(events, log_stats=True)
    interactions = build_interactions(events, cfg)

    if args.split_mode == "user":
        train_i, test_i = temporal_split_per_user(interactions, args.test_size)
    else:
        train_i, test_i = temporal_split_global(interactions, args.test_size)

    user_item, mappings = build_user_item_matrix(train_i)

    num_threads = args.als_num_threads if args.als_num_threads > 0 else None
    model = train_als(
        user_item=user_item,
        factors=args.als_factors,
        iterations=args.als_iterations,
        regularization=args.als_regularization,
        alpha=args.als_alpha,
        random_state=args.als_random_state,
        num_threads=num_threads,
    )

    # Генерируем рекомендации для warm пользователей в тесте (цикл по пользователям).
    test_users = (
        test_i.select("user_id")
        .unique()
        .filter(pl.col("user_id").is_in(list(mappings.user_index.keys())))
        .get_column("user_id")
        .to_list()
    )
    if test_users:
        rec_rows = []
        N = max(args.eval_k, 10)
        for user_id in test_users:
            user_idx = mappings.user_index[int(user_id)]
            user_items_row = user_item[user_idx]
            ids, _scores = model.recommend(
                userid=user_idx,
                user_items=user_items_row,
                N=N,
                filter_already_liked_items=True,
            )
            # ids - одномерный массив индексов товаров
            for rank, item_idx in enumerate(ids, start=1):
                rec_rows.append({
                    "user_id": user_id,
                    "item_id": mappings.index_to_item[int(item_idx)],
                    "rank": rank,
                })
        recs = pl.DataFrame(rec_rows)
    else:
        recs = pl.DataFrame({"user_id": [], "item_id": [], "rank": []})

    # Основные метрики качества модели
    metrics = precision_recall_ndcg_at_k(test_i, recs, k=args.eval_k)
    hit_rate = hit_rate_at_k(test_i, recs, k=args.eval_k)
    metrics["hit_rate@k"] = hit_rate
    
    # Метрики-ограничители (проверка разнообразия и покрытия)
    catalog_size = int(train_i.select("item_id").n_unique())
    if catalog_size > 0:
        coverage = coverage_at_k(recs, catalog_size, k=args.eval_k)
        novelty = novelty_at_k(recs, train_i, k=args.eval_k)
    else:
        # Граничный случай: нет товаров в train (не должно происходить, но обрабатываем корректно)
        coverage = 0.0
        novelty = 0.0
    metrics["coverage@k"] = coverage
    metrics["novelty@k"] = novelty
    
    popular = top_popular_items(train_i, k=args.popular_k)

    params = {
        "min_user_interactions": args.min_user_interactions,
        "min_item_interactions": args.min_item_interactions,
        "test_size": args.test_size,
        "split_mode": args.split_mode,
        "weights": {
            "view": args.weight_view,
            "addtocart": args.weight_addtocart,
            "transaction": args.weight_transaction,
        },
        "als": {
            "factors": args.als_factors,
            "iterations": args.als_iterations,
            "regularization": args.als_regularization,
            "alpha": args.als_alpha,
            "random_state": args.als_random_state,
            "num_threads": int(num_threads or 0),
        },
        "eval_k": args.eval_k,
        "popular_k": args.popular_k,
    }

    paths = save_artifacts(
        artifacts_dir=artifacts_dir,
        model=model,
        user_item=user_item,
        mappings=mappings,
        popular_items=popular,
        params=params,
    )

    # Сохраняем метрики в JSON для отчёта
    metrics_dir = artifacts_dir / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    run_id = args.mlflow_run_name or os.getenv("MLFLOW_RUN_NAME") or datetime.now().strftime("%Y%m%d_%H%M%S")
    metrics_file = metrics_dir / f"{run_id}_metrics.json"
    
    training_report = {
        "run_id": run_id,
        "parameters": {
            "min_user_interactions": args.min_user_interactions,
            "min_item_interactions": args.min_item_interactions,
            "test_size": args.test_size,
            "split_mode": args.split_mode,
            "weights": {
                "view": args.weight_view,
                "addtocart": args.weight_addtocart,
                "transaction": args.weight_transaction,
            },
            "als": {
                "factors": args.als_factors,
                "iterations": args.als_iterations,
                "regularization": args.als_regularization,
                "alpha": args.als_alpha,
                "random_state": args.als_random_state,
                "num_threads": int(num_threads or 0),
            },
            "eval_k": args.eval_k,
            "popular_k": args.popular_k,
        },
        "metrics": metrics,
        "data_stats": {
            "train_users": int(train_i.select("user_id").n_unique()),
            "train_items": int(train_i.select("item_id").n_unique()),
            "train_interactions": len(train_i),
            "test_users": int(test_i.select("user_id").n_unique()),
            "test_items": int(test_i.select("item_id").n_unique()),
            "test_interactions": len(test_i),
            "warm_users_in_test": len(test_users),
            "n_users_in_model": int(user_item.shape[0]),
            "n_items_in_model": int(user_item.shape[1]),
        },
    }
    
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(training_report, f, ensure_ascii=False, indent=2)
    
    # Выводим метрики через логирование
    logger.info("")
    logger.info(f"Обучение модели завершено. Run ID: {run_id}")
    logger.info("")
    logger.info("Параметры модели:")
    logger.info(f"  Split mode: {args.split_mode}")
    logger.info(f"  Min user interactions: {args.min_user_interactions}")
    logger.info(f"  Min item interactions: {args.min_item_interactions}")
    logger.info(f"  Test size: {args.test_size}")
    logger.info(f"  Weights: view={args.weight_view}, addtocart={args.weight_addtocart}, transaction={args.weight_transaction}")
    logger.info(f"  ALS: factors={args.als_factors}, iterations={args.als_iterations}, reg={args.als_regularization}, alpha={args.als_alpha}")
    logger.info("")
    logger.info("Статистика данных:")
    logger.info(f"  Train: {training_report['data_stats']['train_users']} users, "
                f"{training_report['data_stats']['train_items']} items, "
                f"{training_report['data_stats']['train_interactions']} interactions")
    logger.info(f"  Test: {training_report['data_stats']['test_users']} users, "
                f"{training_report['data_stats']['test_items']} items, "
                f"{training_report['data_stats']['test_interactions']} interactions")
    logger.info(f"  Warm users in test: {training_report['data_stats']['warm_users_in_test']}")
    logger.info("")
    logger.info(f"Метрики качества (k={args.eval_k}):")
    logger.info("  Основные метрики:")
    logger.info(f"    precision@k: {metrics['precision@k']:.6f}")
    logger.info(f"    ndcg@k: {metrics['ndcg@k']:.6f}")
    logger.info(f"    hit_rate@k: {metrics['hit_rate@k']:.6f}")
    logger.info("  Метрики-ограничители:")
    logger.info(f"    coverage@k: {metrics['coverage@k']:.6f}")
    logger.info(f"    novelty@k: {metrics['novelty@k']:.6f}")
    logger.info("  Дополнительные метрики:")
    logger.info(f"    recall@k: {metrics['recall@k']:.6f}")
    logger.info("")
    logger.info(f"Отчёт сохранён: {metrics_file}")
    logger.info("")

    # MLflow (опционально, только если MLFLOW_TRACKING_URI установлен)
    if MLFLOW_AVAILABLE and os.getenv("MLFLOW_TRACKING_URI"):
        mlflow.set_experiment(args.mlflow_project)
        with mlflow.start_run(run_name=run_id):
            mlflow.log_params(
                {
                    "min_user_interactions": args.min_user_interactions,
                    "min_item_interactions": args.min_item_interactions,
                    "test_size": args.test_size,
                    "split_mode": args.split_mode,
                    "als_factors": args.als_factors,
                    "als_iterations": args.als_iterations,
                    "als_regularization": args.als_regularization,
                    "als_alpha": args.als_alpha,
                    "eval_k": args.eval_k,
                }
            )
            mlflow.log_metrics(metrics)
            mlflow.log_artifact(str(paths.model_pkl), artifact_path="model")
            mlflow.log_artifact(str(paths.metadata_json), artifact_path="model")
            mlflow.log_artifact(str(paths.popular_json), artifact_path="model")


if __name__ == "__main__":
    main()

