"""Скрипт для анализа результатов A/B тестирования.

Сравнивает метрики ALS модели и baseline модели (popular items)
на основе логов событий и бизнес-метрик.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from src.recsys.utils.logging import setup_logging

logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict:
    """Загружает конфигурацию из YAML файла."""
    if not config_path.exists():
        raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    return config


def analyze_ab_test_results(
    events_path: Path,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Анализирует результаты A/B теста на основе событий.
    
    Args:
        events_path: Путь к файлу с событиями (CSV или Parquet)
        output_path: Путь для сохранения результатов (опционально)
    
    Returns:
        dict: Результаты анализа A/B теста
    """
    logger.info(f"Загрузка событий из: {events_path}")
    
    # Загрузка событий
    if events_path.suffix == ".parquet":
        events = pl.read_parquet(events_path)
    else:
        events = pl.read_csv(events_path)
    
    logger.info(f"Загружено событий: {len(events):,}")
    
    # Проверка наличия необходимых колонок
    required_cols = ["user_id", "item_id", "event", "model_type"]
    missing_cols = [col for col in required_cols if col not in events.columns]
    if missing_cols:
        logger.warning(f"Отсутствуют колонки: {missing_cols}. Используем доступные данные.")
    
    # Фильтрация событий по типу модели (если есть)
    if "model_type" in events.columns:
        als_events = events.filter(pl.col("model_type").is_in(["als", "rerank"]))
        baseline_events = events.filter(pl.col("model_type") == "baseline")
    else:
        logger.warning("Колонка model_type отсутствует. Используем все события.")
        als_events = events
        baseline_events = pl.DataFrame()
    
    # Вычисление метрик для ALS
    als_metrics = compute_business_metrics(als_events, "ALS")
    
    # Вычисление метрик для Baseline
    baseline_metrics = compute_business_metrics(baseline_events, "Baseline")
    
    # Сравнение метрик
    comparison = compare_metrics(als_metrics, baseline_metrics)
    
    results = {
        "als_metrics": als_metrics,
        "baseline_metrics": baseline_metrics,
        "comparison": comparison,
        "summary": generate_summary(als_metrics, baseline_metrics, comparison),
    }
    
    # Сохранение результатов
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"Результаты сохранены: {output_path}")
    
    return results


def compute_business_metrics(
    events: pl.DataFrame,
    model_name: str,
) -> dict[str, Any]:
    """Вычисляет бизнес-метрики для модели.
    
    Args:
        events: DataFrame с событиями
        model_name: Название модели
    
    Returns:
        dict: Бизнес-метрики
    """
    if len(events) == 0:
        return {
            "model": model_name,
            "impressions": 0,
            "clicks": 0,
            "addtocart": 0,
            "transactions": 0,
            "revenue": 0.0,
            "ctr": 0.0,
            "conversion_rate": 0.0,
            "addtocart_rate": 0.0,
        }
    
    # Подсчет событий
    impressions = len(events.filter(pl.col("event") == "impression")) if "event" in events.columns else 0
    clicks = len(events.filter(pl.col("event") == "click")) if "event" in events.columns else 0
    addtocart = len(events.filter(pl.col("event") == "addtocart")) if "event" in events.columns else 0
    transactions = len(events.filter(pl.col("event") == "transaction")) if "event" in events.columns else 0
    
    # Выручка (если есть колонка revenue)
    revenue = 0.0
    if "revenue" in events.columns:
        revenue = events.filter(pl.col("event") == "transaction").select(pl.col("revenue").sum()).item() or 0.0
    
    # Вычисление метрик
    ctr = clicks / impressions if impressions > 0 else 0.0
    conversion_rate = transactions / impressions if impressions > 0 else 0.0
    addtocart_rate = addtocart / impressions if impressions > 0 else 0.0
    
    return {
        "model": model_name,
        "impressions": impressions,
        "clicks": clicks,
        "addtocart": addtocart,
        "transactions": transactions,
        "revenue": revenue,
        "ctr": ctr,
        "conversion_rate": conversion_rate,
        "addtocart_rate": addtocart_rate,
    }


def compare_metrics(
    als_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Сравнивает метрики ALS и Baseline моделей.
    
    Args:
        als_metrics: Метрики ALS модели
        baseline_metrics: Метрики Baseline модели
    
    Returns:
        dict: Сравнение метрик
    """
    comparison = {}
    
    for metric in ["ctr", "conversion_rate", "addtocart_rate", "revenue"]:
        als_val = als_metrics.get(metric, 0.0)
        baseline_val = baseline_metrics.get(metric, 0.0)
        
        if baseline_val > 0:
            improvement = ((als_val - baseline_val) / baseline_val) * 100
        else:
            improvement = 100.0 if als_val > 0 else 0.0
        
        comparison[metric] = {
            "als": als_val,
            "baseline": baseline_val,
            "improvement_pct": improvement,
            "winner": "ALS" if als_val > baseline_val else "Baseline",
        }
    
    return comparison


def generate_summary(
    als_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    comparison: dict[str, Any],
) -> str:
    """Генерирует текстовое резюме результатов A/B теста.
    
    Args:
        als_metrics: Метрики ALS модели
        baseline_metrics: Метрики Baseline модели
        comparison: Сравнение метрик
    
    Returns:
        str: Текстовое резюме
    """
    summary_lines = [
        "=" * 80,
        "РЕЗУЛЬТАТЫ A/B ТЕСТИРОВАНИЯ",
        "=" * 80,
        "",
        f"ALS Модель:",
        f"  Impressions: {als_metrics['impressions']:,}",
        f"  CTR: {als_metrics['ctr']:.4f} ({als_metrics['ctr']*100:.2f}%)",
        f"  Conversion Rate: {als_metrics['conversion_rate']:.4f} ({als_metrics['conversion_rate']*100:.2f}%)",
        f"  Add to Cart Rate: {als_metrics['addtocart_rate']:.4f} ({als_metrics['addtocart_rate']*100:.2f}%)",
        f"  Revenue: {als_metrics['revenue']:.2f}",
        "",
        f"Baseline Модель:",
        f"  Impressions: {baseline_metrics['impressions']:,}",
        f"  CTR: {baseline_metrics['ctr']:.4f} ({baseline_metrics['ctr']*100:.2f}%)",
        f"  Conversion Rate: {baseline_metrics['conversion_rate']:.4f} ({baseline_metrics['conversion_rate']*100:.2f}%)",
        f"  Add to Cart Rate: {baseline_metrics['addtocart_rate']:.4f} ({baseline_metrics['addtocart_rate']*100:.2f}%)",
        f"  Revenue: {baseline_metrics['revenue']:.2f}",
        "",
        "СРАВНЕНИЕ:",
    ]
    
    for metric, comp in comparison.items():
        summary_lines.append(
            f"  {metric.upper()}: "
            f"ALS {comp['als']:.4f} vs Baseline {comp['baseline']:.4f} "
            f"({comp['improvement_pct']:+.1f}%) - Winner: {comp['winner']}"
        )
    
    summary_lines.extend([
        "",
        "=" * 80,
    ])
    
    return "\n".join(summary_lines)


def main():
    """Основная функция для запуска анализа A/B теста."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Анализ результатов A/B тестирования")
    parser.add_argument(
        "--events",
        type=Path,
        required=True,
        help="Путь к файлу с событиями (CSV или Parquet)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Путь для сохранения результатов (JSON)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Путь к конфигурационному файлу (опционально)",
    )
    
    args = parser.parse_args()
    
    # Настройка логирования
    setup_logging()
    
    # Загрузка конфигурации (если указана)
    config = {}
    if args.config:
        config = load_config(args.config)
    
    # Анализ результатов
    results = analyze_ab_test_results(
        events_path=args.events,
        output_path=args.output,
    )
    
    # Вывод резюме
    logger.info(results["summary"])
    
    return results


if __name__ == "__main__":
    main()

