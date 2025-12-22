"""Класс-обработчик для рекомендательной системы.

Создание класса-обработчика
для модульности и удобства тестирования.
"""

from __future__ import annotations

import json
import logging
import os
import pickle
from pathlib import Path
from typing import Any

import polars as pl
import scipy.sparse as sp

logger = logging.getLogger(__name__)


class RecommenderHandler:
    """Класс-обработчик для загрузки модели и генерации рекомендаций.
    
    инкапсулирует логику загрузки
    модели, валидации параметров и генерации рекомендаций.
    """

    def __init__(
        self,
        model_dir: Path | None = None,
        artifacts_dir: Path | None = None,
        enable_rerank: bool = False,
    ):
        """Инициализация обработчика.
        
        Args:
            model_dir: Директория с моделью (artifacts/model)
            artifacts_dir: Директория с parquet артефактами (artifacts)
            enable_rerank: Включить Content-Based re-ranking (требует parquet)
        """
        # Определение путей
        if model_dir is None:
            project_root = Path(__file__).resolve().parents[2]
            model_dir = project_root / "artifacts" / "model"
            artifacts_dir = artifacts_dir or project_root / "artifacts"
        
        self.model_dir = Path(model_dir)
        self.artifacts_dir = Path(artifacts_dir) if artifacts_dir else self.model_dir.parent
        self.enable_rerank = enable_rerank
        
        # Модель и артефакты
        self.model: Any | None = None
        self.user_index: dict[int, int] | None = None
        self.index_to_item: dict[int, int] | None = None
        self.user_item: sp.csr_matrix | None = None
        self.popular: list[int] = []
        
        # Покупки пользователей (для фильтрации)
        self.user_purchases: dict[int, set[int]] = {}
        
        # Parquet артефакты для re-ranking (опционально)
        self.item_metadata: pl.DataFrame | None = None
        self.category_stats: pl.DataFrame | None = None
        self.category_tree: pl.DataFrame | None = None
        
        # Статистика использования
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
            "request_rerank_count": 0,
            "request_baseline_count": 0,
            "ab_test_als_count": 0,
            "ab_test_baseline_count": 0,
        }
        
        # Бизнес-метрики (CTR, конверсия, revenue)
        self._business_metrics = {
            "impressions": 0,  # Показы рекомендаций
            "clicks": 0,  # Клики по рекомендациям
            "addtocart": 0,  # Добавления в корзину
            "transactions": 0,  # Покупки
            "revenue": 0.0,  # Выручка (если доступна)
        }
        
        # Загрузка модели и артефактов
        self.load_model()
        
        if self.enable_rerank:
            self.load_parquet_artifacts()

    def load_model(self) -> None:
        """Загружает модель и необходимые артефакты."""
        model_pkl = self.model_dir / "als_model.pkl"
        metadata_json = self.model_dir / "metadata.json"
        user_item_npz = self.model_dir / "user_item_matrix.npz"
        popular_json = self.model_dir / "popular.json"
        
        # Загрузка mappings
        if metadata_json.exists():
            meta = json.loads(metadata_json.read_text(encoding="utf-8"))
            mappings = meta.get("mappings", {})
            self.user_index = {
                int(k): int(v) for k, v in mappings.get("user_index", {}).items()
            }
            self.index_to_item = {
                int(k): int(v) for k, v in mappings.get("index_to_item", {}).items()
            }
            logger.info(f"Загружены mappings: {len(self.user_index)} пользователей, {len(self.index_to_item)} товаров")
        else:
            logger.warning(f"Метаданные не найдены: {metadata_json}")
        
        # Загрузка популярных товаров
        if popular_json.exists():
            self.popular = json.loads(popular_json.read_text(encoding="utf-8")).get("popular", [])
            self.popular = [int(x) for x in self.popular]
            logger.info(f"Загружено популярных товаров: {len(self.popular)}")
        else:
            logger.warning(f"Популярные товары не найдены: {popular_json}")
        
        # Загрузка user-item матрицы
        if user_item_npz.exists():
            self.user_item = sp.load_npz(user_item_npz).tocsr()
            logger.info(f"Загружена user-item матрица: {self.user_item.shape}")
        else:
            logger.warning(f"User-item матрица не найдена: {user_item_npz}")
        
        # Загрузка модели
        if model_pkl.exists():
            with open(model_pkl, "rb") as f:
                self.model = pickle.load(f)
            logger.info("Загружена ALS модель")
        else:
            logger.warning(f"Модель не найдена: {model_pkl}")
        
        # Загрузка покупок пользователей
        purchases_path = self.artifacts_dir / "user_purchases.parquet"
        if purchases_path.exists():
            try:
                purchases_df = pl.read_parquet(purchases_path)
                # Преобразуем в словарь для быстрого доступа
                purchases_grouped = (
                    purchases_df
                    .group_by("user_id")
                    .agg(pl.col("item_id"))
                )
                
                # Преобразуем в dict[int, set[int]]
                for row in purchases_grouped.iter_rows(named=True):
                    user_id = int(row["user_id"])
                    item_ids = row["item_id"]
                    # item_ids может быть list или Series
                    if isinstance(item_ids, list):
                        self.user_purchases[user_id] = {int(x) for x in item_ids}
                    else:
                        self.user_purchases[user_id] = {int(x) for x in item_ids.to_list()}
                
                logger.info(f"Загружены покупки пользователей: {len(self.user_purchases):,} пользователей с покупками")
            except Exception as e:
                logger.warning(f"Ошибка при загрузке покупок пользователей: {e}")
        else:
            logger.info(f"Покупки пользователей не найдены: {purchases_path} (фильтрация будет пропущена)")

    def load_parquet_artifacts(self) -> None:
        """Загружает parquet артефакты для Content-Based re-ranking."""
        try:
            # Item metadata
            item_metadata_path = self.artifacts_dir / "item_metadata.parquet"
            if item_metadata_path.exists():
                self.item_metadata = pl.read_parquet(item_metadata_path)
                logger.info(f"Загружены метаданные товаров: {len(self.item_metadata):,} товаров")
            else:
                logger.warning(f"Метаданные товаров не найдены: {item_metadata_path}")
            
            # Category stats
            category_stats_path = self.artifacts_dir / "category_stats.parquet"
            if category_stats_path.exists():
                self.category_stats = pl.read_parquet(category_stats_path)
                logger.info(f"Загружена статистика категорий: {len(self.category_stats):,} категорий")
            else:
                logger.warning(f"Статистика категорий не найдена: {category_stats_path}")
            
            # Category tree
            category_tree_path = self.artifacts_dir / "category_tree.parquet"
            if category_tree_path.exists():
                self.category_tree = pl.read_parquet(category_tree_path)
                logger.info(f"Загружено дерево категорий: {len(self.category_tree):,} узлов")
            else:
                logger.warning(f"Дерево категорий не найдено: {category_tree_path}")
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке parquet артефактов: {e}")
            self.enable_rerank = False

    def validate_params(self, user_id: int, top_k: int) -> bool:
        """Валидация параметров запроса.
        
        Args:
            user_id: ID пользователя
            top_k: Количество рекомендаций
            
        Returns:
            bool: True если параметры валидны
        """
        if top_k <= 0 or top_k > 500:
            return False
        if user_id < 0:
            return False
        return True

    def recommend(
        self,
        user_id: int,
        top_k: int = 10,
        use_rerank: bool | None = None,
        use_baseline: bool = False,
    ) -> dict[str, Any]:
        """Генерирует рекомендации для пользователя.
        
        Args:
            user_id: ID пользователя
            top_k: Количество рекомендаций
            use_rerank: Использовать re-ranking (если None, используется self.enable_rerank)
            use_baseline: Использовать baseline модель (popular items) вместо ALS
            
        Returns:
            dict: Рекомендации с полями user_id, recommendations, cold, model_type
        """
        # Валидация
        if not self.validate_params(user_id, top_k):
            raise ValueError(f"Некорректные параметры: user_id={user_id}, top_k={top_k}")
        
        # Baseline модель (popular items)
        if use_baseline:
            if not self.popular:
                raise RuntimeError("Популярные товары недоступны для baseline")
            self._stats["request_baseline_count"] += 1
            self._business_metrics["impressions"] += top_k
            return {
                "user_id": user_id,
                "recommendations": [{"item_id": item_id, "score": 0.0} for item_id in self.popular[:top_k]],
                "cold": True,
                "model_type": "baseline",
            }
        
        # Проверка готовности модели
        if (
            self.model is None
            or self.user_index is None
            or self.index_to_item is None
            or self.user_item is None
        ):
            if self.popular:
                self._stats["request_default_count"] += 1
                self._business_metrics["impressions"] += top_k
                return {
                    "user_id": user_id,
                    "recommendations": [{"item_id": item_id, "score": 0.0} for item_id in self.popular[:top_k]],
                    "cold": True,
                    "model_type": "fallback",
                }
            raise RuntimeError("Модель не готова")
        
        # Cold user - возвращаем популярные товары
        if user_id not in self.user_index:
            if self.popular:
                self._stats["request_default_count"] += 1
                self._business_metrics["impressions"] += top_k
                return {
                    "user_id": user_id,
                    "recommendations": [{"item_id": item_id, "score": 0.0} for item_id in self.popular[:top_k]],
                    "cold": True,
                    "model_type": "fallback",
                }
            raise ValueError(f"Пользователь не найден: {user_id}")
        
        # Генерация рекомендаций ALS
        # Генерируем больше кандидатов для фильтрации покупок
        n_candidates = top_k * 3 if self.user_purchases else top_k * 2 if (use_rerank or (use_rerank is None and self.enable_rerank)) else top_k
        
        uidx = self.user_index[user_id]
        ids, scores = self.model.recommend(
            uidx,
            self.user_item[uidx],
            N=n_candidates,
            filter_already_liked_items=True,
        )
        
        # Формирование рекомендаций
        recs = [
            {"item_id": self.index_to_item[int(i)], "score": float(s)}
            for i, s in zip(ids, scores)
        ]
        
        # Явная фильтрация покупок
        purchased = self.user_purchases.get(user_id, set())
        if purchased:
            recs = [r for r in recs if r["item_id"] not in purchased]
        
        # Если после фильтрации осталось меньше top_k, дополняем из оставшихся
        if len(recs) < top_k and n_candidates < len(self.index_to_item):
            # Генерируем еще кандидатов, исключая уже купленные
            additional_n = (top_k - len(recs)) * 2
            additional_ids, additional_scores = self.model.recommend(
                uidx,
                self.user_item[uidx],
                N=min(n_candidates + additional_n, len(self.index_to_item)),
                filter_already_liked_items=True,
            )
            additional_recs = [
                {"item_id": self.index_to_item[int(i)], "score": float(s)}
                for i, s in zip(additional_ids, additional_scores)
                if self.index_to_item[int(i)] not in purchased
                and self.index_to_item[int(i)] not in {r["item_id"] for r in recs}
            ]
            recs.extend(additional_recs[:top_k - len(recs)])
        
        # Применение re-ranking (если включено)
        should_rerank = use_rerank if use_rerank is not None else self.enable_rerank
        if should_rerank and self._can_rerank():
            try:
                recs = self._apply_rerank(user_id, recs, top_k)
                self._stats["request_rerank_count"] += 1
            except Exception as e:
                logger.warning(f"Re-ranking не удался, используем ALS рекомендации: {e}")
                recs = recs[:top_k]
                self._stats["request_personal_count"] += 1
        else:
            recs = recs[:top_k]
            self._stats["request_personal_count"] += 1
        
        self._business_metrics["impressions"] += len(recs)
        
        return {
            "user_id": user_id,
            "recommendations": recs,
            "cold": False,
            "model_type": "rerank" if should_rerank and self._can_rerank() else "als",
        }

    def _can_rerank(self) -> bool:
        """Проверяет, можно ли применить re-ranking."""
        return (
            self.item_metadata is not None
            and self.category_stats is not None
            and self.category_tree is not None
        )

    def _apply_rerank(
        self,
        user_id: int,
        recommendations: list[dict[str, Any]],
        top_k: int,
    ) -> list[dict[str, Any]]:
        """Применяет Content-Based re-ranking к рекомендациям.
        
        Args:
            user_id: ID пользователя
            recommendations: Список рекомендаций от ALS
            top_k: Количество рекомендаций для возврата
            
        Returns:
            Переранжированный список рекомендаций
        """
        from src.recsys.models.rerank import category_based_rerank
        
        # Преобразуем рекомендации в DataFrame
        recs_df = pl.DataFrame([
            {"user_id": user_id, "item_id": rec["item_id"], "rank": i + 1}
            for i, rec in enumerate(recommendations)
        ])
        
        # Получаем историю пользователя из user_item матрицы
        if user_id in self.user_index:
            uidx = self.user_index[user_id]
            user_items = self.user_item[uidx].indices
            user_item_ids = [self.index_to_item[int(idx)] for idx in user_items]
            
            # Создаём историю с категориями
            user_history = (
                pl.DataFrame({"item_id": user_item_ids})
                .join(
                    self.item_metadata.select(["item_id", "category_id"]),
                    on="item_id",
                    how="left",
                )
                .with_columns(pl.lit(user_id).alias("user_id"))
                .select(["user_id", "item_id", "category_id"])
            )
        else:
            user_history = pl.DataFrame({
                "user_id": [],
                "item_id": [],
                "category_id": [],
            })
        
        # Применяем re-ranking
        reranked = category_based_rerank(
            recommendations=recs_df,
            user_item_history=user_history,
            item_metadata=self.item_metadata,
            category_stats=self.category_stats,
            category_tree=self.category_tree,
            top_k=top_k,
            alpha=0.6,
            beta=0.4,
            adaptive=True,
        )
        
        # Преобразуем обратно в список словарей
        reranked_items = set(reranked["item_id"].to_list())
        reranked_recs = [
            rec for rec in recommendations
            if rec["item_id"] in reranked_items
        ]
        
        # Сохраняем порядок из reranked
        item_to_score = {rec["item_id"]: rec["score"] for rec in recommendations}
        result = [
            {"item_id": item_id, "score": item_to_score.get(item_id, 0.0)}
            for item_id in reranked["item_id"].to_list()[:top_k]
        ]
        
        return result

    def get_stats(self) -> dict[str, Any]:
        """Возвращает статистику использования.
        
        Returns:
            dict: Статистика запросов
        """
        total_requests = (
            self._stats["request_personal_count"]
            + self._stats["request_default_count"]
            + self._stats["request_rerank_count"]
            + self._stats["request_baseline_count"]
        )
        
        # Вычисление CTR
        ctr = (
            self._business_metrics["clicks"] / self._business_metrics["impressions"]
            if self._business_metrics["impressions"] > 0
            else 0.0
        )
        
        # Вычисление конверсии (transactions / impressions)
        conversion_rate = (
            self._business_metrics["transactions"] / self._business_metrics["impressions"]
            if self._business_metrics["impressions"] > 0
            else 0.0
        )
        
        return {
            "total_requests": total_requests,
            "personal_recommendations": self._stats["request_personal_count"],
            "popular_fallback": self._stats["request_default_count"],
            "rerank_applied": self._stats["request_rerank_count"],
            "baseline_recommendations": self._stats["request_baseline_count"],
            "model_loaded": self.model is not None,
            "rerank_enabled": self.enable_rerank and self._can_rerank(),
            "business_metrics": {
                "impressions": self._business_metrics["impressions"],
                "clicks": self._business_metrics["clicks"],
                "addtocart": self._business_metrics["addtocart"],
                "transactions": self._business_metrics["transactions"],
                "revenue": self._business_metrics["revenue"],
                "ctr": ctr,
                "conversion_rate": conversion_rate,
            },
        }
    
    def log_event(
        self,
        user_id: int,
        item_id: int,
        event_type: str,
        revenue: float = 0.0,
        model_type: str | None = None,
    ) -> None:
        """Логирует бизнес-событие (click, addtocart, transaction).
        
        Args:
            user_id: ID пользователя
            item_id: ID товара
            event_type: Тип события (click, addtocart, transaction)
            revenue: Выручка (для transaction)
            model_type: Тип модели (als, baseline, rerank) - опционально
        """
        if event_type == "click":
            self._business_metrics["clicks"] += 1
        elif event_type == "addtocart":
            self._business_metrics["addtocart"] += 1
        elif event_type == "transaction":
            self._business_metrics["transactions"] += 1
            self._business_metrics["revenue"] += revenue
        else:
            logger.warning(f"Неизвестный тип события: {event_type}")
        
        # Логирование события (для последующего анализа)
        import datetime
        log_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "user_id": user_id,
            "item_id": item_id,
            "event_type": event_type,
            "revenue": revenue,
            "model_type": model_type,
        }
        logger.info(f"EVENT: {log_entry}")
    
    def get_baseline_recommendations(
        self,
        user_id: int,
        top_k: int = 10,
    ) -> dict[str, Any]:
        """Генерирует рекомендации от baseline модели (popular items).
        
        Args:
            user_id: ID пользователя
            top_k: Количество рекомендаций
            
        Returns:
            dict: Рекомендации от baseline модели
        """
        if not self.popular:
            raise RuntimeError("Популярные товары недоступны")
        
        self._stats["request_baseline_count"] += 1
        self._business_metrics["impressions"] += top_k
        
        return {
            "user_id": user_id,
            "recommendations": [{"item_id": item_id, "score": 0.0} for item_id in self.popular[:top_k]],
            "cold": True,
            "model_type": "baseline",
        }

