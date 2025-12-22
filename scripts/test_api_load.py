#!/usr/bin/env python3
"""Тестовый скрипт для имитации нагрузки на Recommender API.

Генерирует различные типы запросов:
- Рекомендации ALS
- Рекомендации baseline
- Рекомендации с rerank
- События (click, addtocart, transaction)
- A/B тестирование

Проверяет работу Prometheus метрик и мониторинга.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class APILoadTester:
    """Класс для тестирования нагрузки на API."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout: int = 30,
    ):
        """Инициализация тестера.
        
        Args:
            base_url: Базовый URL API
            timeout: Таймаут запросов в секундах
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        
        # Настройка сессии с retry
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Статистика
        self.stats = {
            "requests": {"total": 0, "success": 0, "errors": 0},
            "recommendations": {"als": 0, "baseline": 0, "rerank": 0},
            "events": {"click": 0, "addtocart": 0, "transaction": 0},
            "errors": [],
        }
        
        # Популярные товары для тестирования (загружаются из API)
        self.popular_items = []
        self._load_popular_items()

    def _load_popular_items(self) -> None:
        """Загружает популярные товары из API для тестирования."""
        try:
            # Пробуем получить рекомендации для любого пользователя
            response = self.session.get(
                f"{self.base_url}/recommendations/baseline/1",
                params={"top_k": 100},
                timeout=self.timeout,
            )
            if response.status_code == 200:
                data = response.json()
                self.popular_items = [
                    rec["item_id"] for rec in data.get("recommendations", [])
                ]
                print(f"Загружено популярных товаров для тестирования: {len(self.popular_items)}")
        except Exception as e:
            print(f"Предупреждение: не удалось загрузить популярные товары: {e}")

    def check_health(self) -> bool:
        """Проверяет health endpoint API.
        
        Returns:
            bool: True если API доступен
        """
        try:
            response = self.session.get(
                f"{self.base_url}/health",
                timeout=self.timeout,
            )
            if response.status_code == 200:
                data = response.json()
                print(f"Health check: {data}")
                # API может возвращать "ok" или "healthy" в зависимости от версии
                status = data.get("status", "")
                return status in ("ok", "healthy")
            return False
        except Exception as e:
            print(f"Ошибка health check: {e}")
            return False

    def get_recommendations(
        self,
        user_id: int,
        top_k: int = 10,
        use_rerank: bool = False,
        use_baseline: bool = False,
        ab_test_group: str | None = None,
    ) -> dict[str, Any] | None:
        """Получает рекомендации для пользователя.
        
        Args:
            user_id: ID пользователя
            top_k: Количество рекомендаций
            use_rerank: Использовать rerank
            use_baseline: Использовать baseline
            ab_test_group: Группа A/B теста
            
        Returns:
            dict: Ответ API или None при ошибке
        """
        params = {
            "top_k": top_k,
        }
        if use_rerank:
            params["use_rerank"] = True
        if use_baseline:
            params["use_baseline"] = True
        if ab_test_group:
            params["ab_test_group"] = ab_test_group

        try:
            response = self.session.get(
                f"{self.base_url}/recommendations/{user_id}",
                params=params,
                timeout=self.timeout,
            )
            self.stats["requests"]["total"] += 1
            
            if response.status_code == 200:
                self.stats["requests"]["success"] += 1
                data = response.json()
                
                # Обновляем статистику по типам рекомендаций
                model_type = data.get("model_type", "unknown")
                if model_type == "baseline":
                    self.stats["recommendations"]["baseline"] += 1
                elif model_type == "rerank":
                    self.stats["recommendations"]["rerank"] += 1
                else:
                    self.stats["recommendations"]["als"] += 1
                
                return data
            else:
                self.stats["requests"]["errors"] += 1
                error_msg = f"Ошибка {response.status_code}: {response.text[:200]}"
                self.stats["errors"].append(error_msg)
                print(f"Ошибка получения рекомендаций: {error_msg}")
                return None
        except Exception as e:
            self.stats["requests"]["errors"] += 1
            error_msg = f"Исключение: {str(e)}"
            self.stats["errors"].append(error_msg)
            print(f"Ошибка получения рекомендаций: {error_msg}")
            return None

    def log_event(
        self,
        user_id: int,
        item_id: int,
        event_type: str,
        revenue: float = 0.0,
        model_type: str | None = None,
    ) -> bool:
        """Логирует событие (click, addtocart, transaction).
        
        Args:
            user_id: ID пользователя
            item_id: ID товара
            event_type: Тип события
            revenue: Выручка (для transaction)
            model_type: Тип модели
            
        Returns:
            bool: True если успешно
        """
        payload = {
            "user_id": user_id,
            "item_id": item_id,
            "event_type": event_type,
            "revenue": revenue,
        }
        if model_type:
            payload["model_type"] = model_type

        try:
            response = self.session.post(
                f"{self.base_url}/events",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )
            self.stats["requests"]["total"] += 1
            
            if response.status_code == 200:
                self.stats["requests"]["success"] += 1
                self.stats["events"][event_type] += 1
                return True
            else:
                self.stats["requests"]["errors"] += 1
                error_msg = f"Ошибка {response.status_code}: {response.text[:200]}"
                self.stats["errors"].append(error_msg)
                print(f"Ошибка логирования события: {error_msg}")
                return False
        except Exception as e:
            self.stats["requests"]["errors"] += 1
            error_msg = f"Исключение: {str(e)}"
            self.stats["errors"].append(error_msg)
            print(f"Ошибка логирования события: {error_msg}")
            return False

    def simulate_user_session(
        self,
        user_id: int,
        num_recommendations: int = 3,
        num_events: int = 5,
    ) -> None:
        """Имитирует сессию пользователя.
        
        Args:
            user_id: ID пользователя
            num_recommendations: Количество запросов рекомендаций
            num_events: Количество событий
        """
        # Получаем рекомендации
        for _ in range(num_recommendations):
            # Случайный выбор типа рекомендаций
            use_baseline = random.random() < 0.2  # 20% baseline
            use_rerank = random.random() < 0.3 if not use_baseline else False  # 30% rerank
            ab_test = "auto" if random.random() < 0.5 else None
            
            recs = self.get_recommendations(
                user_id=user_id,
                top_k=random.randint(5, 20),
                use_rerank=use_rerank,
                use_baseline=use_baseline,
                ab_test_group=ab_test,
            )
            
            if recs and recs.get("recommendations"):
                # Имитируем события на основе рекомендаций
                recommendations = recs["recommendations"]
                model_type = recs.get("model_type", "als")
                
                # Клики по рекомендациям (70% рекомендаций)
                clicked_items = random.sample(
                    recommendations,
                    min(len(recommendations), int(len(recommendations) * 0.7)),
                )
                for rec in clicked_items:
                    self.log_event(
                        user_id=user_id,
                        item_id=rec["item_id"],
                        event_type="click",
                        model_type=model_type,
                    )
                    time.sleep(0.1)  # Небольшая задержка между событиями
                
                # Добавления в корзину (30% от кликов)
                cart_items = random.sample(
                    clicked_items,
                    min(len(clicked_items), int(len(clicked_items) * 0.3)),
                )
                for rec in cart_items:
                    self.log_event(
                        user_id=user_id,
                        item_id=rec["item_id"],
                        event_type="addtocart",
                        model_type=model_type,
                    )
                    time.sleep(0.1)
                
                # Покупки (10% от добавлений в корзину)
                if cart_items:
                    purchase_items = random.sample(
                        cart_items,
                        min(len(cart_items), max(1, int(len(cart_items) * 0.1))),
                    )
                    for rec in purchase_items:
                        revenue = round(random.uniform(100, 5000), 2)
                        self.log_event(
                            user_id=user_id,
                            item_id=rec["item_id"],
                            event_type="transaction",
                            revenue=revenue,
                            model_type=model_type,
                        )
                        time.sleep(0.1)
            
            time.sleep(0.5)  # Задержка между запросами рекомендаций

    def run_load_test(
        self,
        num_users: int = 10,
        num_sessions_per_user: int = 3,
        delay_between_sessions: float = 1.0,
    ) -> None:
        """Запускает нагрузочный тест.
        
        Args:
            num_users: Количество пользователей
            num_sessions_per_user: Количество сессий на пользователя
            delay_between_sessions: Задержка между сессиями в секундах
        """
        print(f"\n{'='*80}")
        print(f"Запуск нагрузочного теста")
        print(f"Пользователей: {num_users}")
        print(f"Сессий на пользователя: {num_sessions_per_user}")
        print(f"{'='*80}\n")
        
        # Проверка health
        if not self.check_health():
            print("❌ API недоступен, прерываем тест")
            return
        
        # Генерируем случайные ID пользователей
        user_ids = [random.randint(1, 100000) for _ in range(num_users)]
        
        start_time = time.time()
        
        # Запускаем сессии для каждого пользователя
        for user_idx, user_id in enumerate(user_ids, 1):
            print(f"Пользователь {user_idx}/{num_users} (ID: {user_id})")
            for session_idx in range(1, num_sessions_per_user + 1):
                print(f"  Сессия {session_idx}/{num_sessions_per_user}")
                self.simulate_user_session(
                    user_id=user_id,
                    num_recommendations=random.randint(2, 5),
                    num_events=random.randint(3, 8),
                )
                if session_idx < num_sessions_per_user:
                    time.sleep(delay_between_sessions)
        
        elapsed_time = time.time() - start_time
        
        # Выводим статистику
        self.print_stats(elapsed_time)

    def print_stats(self, elapsed_time: float) -> None:
        """Выводит статистику тестирования.
        
        Args:
            elapsed_time: Время выполнения в секундах
        """
        print(f"\n{'='*80}")
        print("СТАТИСТИКА ТЕСТИРОВАНИЯ")
        print(f"{'='*80}\n")
        
        print(f"Время выполнения: {elapsed_time:.2f} секунд")
        print(f"Запросов в секунду: {self.stats['requests']['total'] / elapsed_time:.2f}")
        print()
        
        print("Запросы:")
        print(f"  Всего: {self.stats['requests']['total']}")
        print(f"  Успешных: {self.stats['requests']['success']}")
        print(f"  Ошибок: {self.stats['requests']['errors']}")
        if self.stats['requests']['total'] > 0:
            success_rate = (self.stats['requests']['success'] / self.stats['requests']['total']) * 100
            print(f"  Успешность: {success_rate:.1f}%")
        print()
        
        print("Рекомендации:")
        print(f"  ALS: {self.stats['recommendations']['als']}")
        print(f"  Baseline: {self.stats['recommendations']['baseline']}")
        print(f"  Rerank: {self.stats['recommendations']['rerank']}")
        print()
        
        print("События:")
        print(f"  Клики: {self.stats['events']['click']}")
        print(f"  Добавления в корзину: {self.stats['events']['addtocart']}")
        print(f"  Покупки: {self.stats['events']['transaction']}")
        print()
        
        if self.stats['errors']:
            print(f"Ошибки ({len(self.stats['errors'])}):")
            for error in self.stats['errors'][:10]:  # Показываем первые 10
                print(f"  - {error}")
            if len(self.stats['errors']) > 10:
                print(f"  ... и еще {len(self.stats['errors']) - 10} ошибок")
        
        print(f"\n{'='*80}")
        print("Проверьте метрики в Prometheus:")
        print(f"  - recsys_requests_total")
        print(f"  - recsys_request_latency_seconds")
        print(f"  - recsys_clicks_total")
        print(f"  - recsys_addtocart_total")
        print(f"  - recsys_transactions_total")
        print(f"  - recsys_revenue_total")
        print(f"{'='*80}\n")

    def get_api_stats(self) -> dict[str, Any] | None:
        """Получает статистику API.
        
        Returns:
            dict: Статистика API или None при ошибке
        """
        try:
            response = self.session.get(
                f"{self.base_url}/stats",
                timeout=self.timeout,
            )
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Ошибка получения статистики API: {e}")
            return None

    def get_ab_test_stats(self) -> dict[str, Any] | None:
        """Получает статистику A/B теста.
        
        Returns:
            dict: Статистика A/B теста или None при ошибке
        """
        try:
            response = self.session.get(
                f"{self.base_url}/ab-test/stats",
                timeout=self.timeout,
            )
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Ошибка получения статистики A/B теста: {e}")
            return None


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(
        description="Тестовый скрипт для имитации нагрузки на Recommender API"
    )
    parser.add_argument(
        "--url",
        type=str,
        default="http://localhost:8000",
        help="Базовый URL API (по умолчанию: http://localhost:8000)",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=10,
        help="Количество пользователей (по умолчанию: 10)",
    )
    parser.add_argument(
        "--sessions",
        type=int,
        default=3,
        help="Количество сессий на пользователя (по умолчанию: 3)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Задержка между сессиями в секундах (по умолчанию: 1.0)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Таймаут запросов в секундах (по умолчанию: 30)",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Только показать статистику API без нагрузки",
    )
    
    args = parser.parse_args()
    
    tester = APILoadTester(base_url=args.url, timeout=args.timeout)
    
    if args.stats_only:
        # Только статистика
        print("Получение статистики API...")
        stats = tester.get_api_stats()
        if stats:
            print(json.dumps(stats, indent=2, ensure_ascii=False))
        
        print("\nСтатистика A/B теста:")
        ab_stats = tester.get_ab_test_stats()
        if ab_stats:
            print(json.dumps(ab_stats, indent=2, ensure_ascii=False))
    else:
        # Запуск нагрузочного теста
        tester.run_load_test(
            num_users=args.users,
            num_sessions_per_user=args.sessions,
            delay_between_sessions=args.delay,
        )
        
        # Показываем статистику API после теста
        print("\nСтатистика API после теста:")
        stats = tester.get_api_stats()
        if stats:
            print(json.dumps(stats, indent=2, ensure_ascii=False))
        
        print("\nСтатистика A/B теста:")
        ab_stats = tester.get_ab_test_stats()
        if ab_stats:
            print(json.dumps(ab_stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

