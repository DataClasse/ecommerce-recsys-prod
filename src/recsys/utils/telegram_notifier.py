"""
Утилита для отправки уведомлений в Telegram.

Использует прямые HTTP запросы к Telegram Bot API.
"""

import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any
import requests

logger = logging.getLogger(__name__)

# Загружаем .env файл при импорте модуля
try:
    from dotenv import load_dotenv
    # Ищем .env файл в корне проекта
    project_root = Path(__file__).parent.parent.parent.parent
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass


class TelegramNotifier:
    """Класс для отправки уведомлений в Telegram через Bot API."""
    
    API_URL = "https://api.telegram.org/bot{token}/sendMessage"
    
    def __init__(self, token: Optional[str] = None, chat_id: Optional[str] = None):
        """
        Инициализация Telegram notifier.
        
        Args:
            token: Telegram Bot Token (из .env: TELEGRAM_BOT_TOKEN)
            chat_id: Telegram Chat ID (из .env: TELEGRAM_CHAT_ID)
        """
        # Загружаем из переменных окружения, если не переданы
        self.token = token or os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("MLE_TELEGRAM_TOKEN")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID") or os.getenv("MLE_TELEGRAM_CHAT_ID")
        
        if not self.token or not self.chat_id:
            logger.warning("Telegram credentials не настроены в .env")
            self.enabled = False
        else:
            self.enabled = True
            logger.info("Telegram notifier инициализирован")

    def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """
        Отправить сообщение в Telegram.
        
        Args:
            text: Текст сообщения
            parse_mode: Режим парсинга (HTML, Markdown)
            
        Returns:
            bool: True если успешно, False иначе
        """
        if not self.enabled:
            return False
        
        try:
            url = self.API_URL.format(token=self.token)
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info("Telegram уведомление отправлено успешно")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка отправки Telegram уведомления: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке Telegram уведомления: {e}")
            return False

    def send_task_success(
        self,
        task_id: str,
        dag_id: str,
        run_id: str,
        details: Optional[str] = None,
        is_first_task: bool = False
    ) -> bool:
        """
        Отправить уведомление об успешном выполнении задачи.
        
        Args:
            task_id: ID задачи
            dag_id: ID DAG
            run_id: ID запуска
            details: Дополнительные детали (опционально)
            is_first_task: Если True, добавляет заголовок блока для первого сообщения
        """
        # Если это первая задача, добавляем заголовок блока
        if is_first_task:
            message = (
                f"🔄 <b>НОВЫЙ ЗАПУСК DAG</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            )
        else:
            message = ""
        
        message += (
            f"✅ <b>Задача выполнена успешно</b>\n\n"
            f"DAG: <code>{dag_id}</code>\n"
            f"Задача: <code>{task_id}</code>\n"
            f"Run ID: <code>{run_id}</code>"
        )
        
        if details:
            message += f"\n\n{details}"
        
        return self.send_message(message)

    def send_task_failure(
        self,
        task_id: str,
        dag_id: str,
        run_id: str,
        error: Optional[str] = None,
        details: Optional[str] = None
    ) -> bool:
        """
        Отправить уведомление об ошибке выполнения задачи.
        
        Args:
            task_id: ID задачи
            dag_id: ID DAG
            run_id: ID запуска
            error: Сообщение об ошибке (опционально)
            details: Дополнительные детали (опционально)
        """
        message = (
            f"❌ <b>Ошибка выполнения задачи</b>\n\n"
            f"DAG: <code>{dag_id}</code>\n"
            f"Задача: <code>{task_id}</code>\n"
            f"Run ID: <code>{run_id}</code>"
        )
        
        if error:
            # Ограничиваем длину сообщения об ошибке
            error_short = error[:300] + "..." if len(error) > 300 else error
            message += f"\n\nОшибка: <code>{error_short}</code>"
        
        if details:
            message += f"\n\n{details}"
        
        return self.send_message(message)

    def send_training_complete(
        self,
        dag_id: str,
        run_id: str,
        metrics: Optional[Dict[str, Any]] = None,
        n_users: Optional[int] = None,
        n_items: Optional[int] = None
    ) -> bool:
        """
        Отправить уведомление об успешном обучении модели.
        
        Args:
            dag_id: ID DAG
            run_id: ID запуска
            metrics: Метрики модели (опционально)
            n_users: Количество пользователей (опционально)
            n_items: Количество товаров (опционально)
        """
        message = (
            f"✅ <b>Обучение модели завершено</b>\n\n"
            f"DAG: <code>{dag_id}</code>\n"
            f"Run ID: <code>{run_id}</code>"
        )
        
        if n_users:
            message += f"\nПользователей: {n_users:,}"
        if n_items:
            message += f"\nТоваров: {n_items:,}"
        
        if metrics:
            # Форматируем основные метрики для production модели
            main_metrics = []
            metric_names = {
                'precision@5': 'Precision@5',
                'precision@k': 'Precision@k',
                'recall@5': 'Recall@5',
                'recall@20': 'Recall@20',
                'recall@k': 'Recall@k',
                'ndcg@10': 'nDCG@10',
                'ndcg@k': 'nDCG@k',
                'hit_rate@5': 'Hit Rate@5',
                'hit_rate@k': 'Hit Rate@k',
                'coverage@k': 'Coverage@k',
                'novelty@k': 'Novelty@k',
                'diversity@k': 'Diversity@k',
                'cart_prediction_rate@5': 'Cart Prediction Rate@5',
            }
            
            # Приоритетные метрики для отображения
            priority_metrics = [
                'recall@20', 'precision@5', 'ndcg@10', 'hit_rate@5',
                'recall@k', 'precision@k', 'ndcg@k', 'hit_rate@k',
                'coverage@k', 'novelty@k', 'diversity@k', 'cart_prediction_rate@5'
            ]
            
            for key in priority_metrics:
                if key in metrics:
                    value = metrics[key]
                    if isinstance(value, (int, float)):
                        # Форматируем как проценты для precision, recall, hit_rate, coverage, novelty, cart_prediction_rate
                        if key in ['precision@5', 'precision@k', 'recall@5', 'recall@20', 'recall@k', 
                                   'hit_rate@5', 'hit_rate@k', 'coverage@k', 'novelty@k', 'diversity@k',
                                   'cart_prediction_rate@5']:
                            main_metrics.append(f"{metric_names.get(key, key)}: {value*100:.2f}%")
                        else:
                            # Для nDCG - десятичная форма
                            main_metrics.append(f"{metric_names.get(key, key)}: {value:.4f}")
            
            if main_metrics:
                message += f"\n\n<b>Метрики:</b>\n" + "\n".join(main_metrics)
        
        return self.send_message(message)


def get_notifier() -> TelegramNotifier:
    """
    Получить экземпляр TelegramNotifier с кредами из .env.
    
    Returns:
        TelegramNotifier: Экземпляр notifier
    """
    return TelegramNotifier()


def send_success_callback(context: Dict[str, Any]) -> None:
    """
    Callback для успешного выполнения задачи Airflow.
    
    Используется в on_success_callback для задач.
    
    Args:
        context: Контекст выполнения задачи Airflow
    """
    logger.info(f"send_success_callback ВЫЗВАН для задачи")
    try:
        notifier = get_notifier()
        logger.info(f"Notifier получен, enabled={notifier.enabled}")
        
        task_instance = context.get('task_instance')
        dag = context.get('dag')
        run_id = context.get('run_id', 'unknown')
        
        task_id = task_instance.task_id if task_instance else "unknown"
        dag_id = dag.dag_id if dag else "unknown"
        
        # Получаем детали из XCom, если есть
        details = None
        if task_instance:
            try:
                result = task_instance.xcom_pull(task_ids=task_id)
                if isinstance(result, dict):
                    if 'interactions_count' in result:
                        details = f"Обработано взаимодействий: {result['interactions_count']:,}"
                    elif 'metrics' in result:
                        metrics = result.get('metrics', {})
                        if metrics:
                            # Форматируем все метрики как в ноутбуке
                            metric_lines = []
                            metric_lines.append(f"Precision@5: {metrics.get('precision@5', 0)*100:.2f}%")
                            metric_lines.append(f"Recall@5: {metrics.get('recall@5', 0)*100:.2f}%")
                            metric_lines.append(f"MAP@5: {metrics.get('map@5', 0):.4f}")
                            metric_lines.append(f"nDCG@5: {metrics.get('ndcg@5', 0):.4f}")
                            metric_lines.append(f"Coverage: {metrics.get('coverage', 0)*100:.2f}%")
                            metric_lines.append(f"Novelty@5: {metrics.get('novelty@5', 0)*100:.2f}%")
                            details = "Метрики:\n" + "\n".join(metric_lines)
            except Exception:
                pass
        
        notifier.send_task_success(
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            details=details
        )
    except Exception as e:
        logger.error(f"Ошибка в send_success_callback: {e}", exc_info=True)


def send_failure_callback(context: Dict[str, Any]) -> None:
    """
    Callback для ошибки выполнения задачи Airflow.
    
    Используется в on_failure_callback для задач.
    
    Args:
        context: Контекст выполнения задачи Airflow
    """
    try:
        notifier = get_notifier()
        
        task_instance = context.get('task_instance')
        dag = context.get('dag')
        run_id = context.get('run_id', 'unknown')
        exception = context.get('exception')
        
        task_id = task_instance.task_id if task_instance else "unknown"
        dag_id = dag.dag_id if dag else "unknown"
        error_message = str(exception) if exception else "Unknown error"
        
        notifier.send_task_failure(
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            error=error_message
        )
    except Exception as e:
        logger.error(f"Ошибка в send_failure_callback: {e}", exc_info=True)


def send_dag_success_callback(context: Dict[str, Any]) -> None:
    """
    Callback для успешного выполнения всего DAG.
    
    Используется в on_success_callback для DAG.
    
    Args:
        context: Контекст выполнения DAG
    """
    logger.info("=" * 80)
    logger.info("send_dag_success_callback ВЫЗВАН")
    logger.info("=" * 80)
    try:
        notifier = get_notifier()
        logger.info(f"Notifier получен, enabled={notifier.enabled}")
        
        dag = context.get('dag')
        run_id = context.get('run_id', 'unknown')
        
        dag_id = dag.dag_id if dag else "unknown"
        
        # Пытаемся получить метрики из последней задачи
        metrics = None
        n_users = None
        n_items = None
        
        try:
            task_instance = context.get('task_instance')
            dag_run = context.get('dag_run')
            
            if task_instance and dag_run:
                # Ищем метрики в XCom из всех задач
                # Для recsys_train_daily основная задача - train_production_model
                for task_id in ['train_production_model', 'calculate_metrics', 'train_als']:
                    try:
                        result = task_instance.xcom_pull(task_ids=task_id, dag_id=dag_id, include_prior_dates=False)
                        if isinstance(result, dict):
                            if 'metrics' in result:
                                metrics = result['metrics']
                            if 'n_users' in result:
                                n_users = result['n_users']
                            if 'n_items' in result:
                                n_items = result['n_items']
                            # Если получили результат, можно прервать поиск
                            if metrics or n_users or n_items:
                                break
                    except Exception:
                        continue
        except Exception:
            pass
        
        logger.info(f"Отправка уведомления: dag_id={dag_id}, run_id={run_id}")
        result = notifier.send_training_complete(
            dag_id=dag_id,
            run_id=run_id,
            metrics=metrics,
            n_users=n_users,
            n_items=n_items
        )
        logger.info(f"Результат отправки уведомления: {result}")
    except Exception as e:
        logger.error(f"Ошибка в send_dag_success_callback: {e}", exc_info=True)
