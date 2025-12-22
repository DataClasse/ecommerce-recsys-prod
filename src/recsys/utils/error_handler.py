"""Утилиты для централизованной обработки ошибок.

Предоставляет декораторы для автоматической обработки ошибок и отправки уведомлений.

МОЖНО ИСПОЛЬЗОВАТЬ для:
- Простых задач без специфичной логики
- Будущих задач, где не нужна детальная информация
- Упрощения кода в некритичных местах
"""

import logging
from functools import wraps
from typing import Callable, Optional

logger = logging.getLogger(__name__)


def handle_task_errors(
    send_notification: bool = True,
    reraise: bool = True,
    log_error: bool = True,
    exception_class: Optional[type] = None
) -> Callable:
    """Декоратор для обработки ошибок в задачах.
    
    Args:
        send_notification: Отправлять ли Telegram уведомление при ошибке
        reraise: Пробрасывать ли исключение после обработки
        log_error: Логировать ли ошибку
        exception_class: Класс исключения для проброса (по умолчанию - исходное исключение)
    
    Returns:
        Декорированная функция
    
    Пример использования:
        @handle_task_errors(send_notification=True, reraise=True)
        def my_task(**kwargs):
            # код задачи
            pass
        
        # Для Airflow задач
        from airflow.exceptions import AirflowException
        @handle_task_errors(exception_class=AirflowException)
        def airflow_task(**kwargs):
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            task_name = func.__name__
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                error_msg = f"Ошибка в задаче '{task_name}': {str(e)}"
                
                if log_error:
                    logger.error(error_msg, exc_info=True)
                
                if send_notification:
                    try:
                        from src.recsys.utils.telegram_notifier import get_notifier
                        notifier = get_notifier()
                        if notifier.enabled:
                            # Пытаемся получить контекст из kwargs (для Airflow)
                            dag_id = kwargs.get('dag').dag_id if kwargs.get('dag') else "unknown"
                            run_id = kwargs.get('run_id', 'unknown')
                            
                            notifier.send_task_failure(
                                task_id=task_name,
                                dag_id=dag_id,
                                run_id=run_id,
                                error=str(e),
                                details=error_msg
                            )
                    except Exception as notify_error:
                        logger.warning(f"Не удалось отправить уведомление: {notify_error}")
                
                if reraise:
                    if exception_class:
                        raise exception_class(error_msg) from e
                    else:
                        raise
                
                return None
        
        return wrapper
    return decorator


def safe_notify(func: Callable) -> Callable:
    """Декоратор для безопасной отправки уведомлений.
    
    Оборачивает функцию в try/except, чтобы ошибки отправки уведомлений
    не прерывали выполнение основной задачи.
    
    Args:
        func: Функция для оборачивания
    
    Returns:
        Декорированная функция
    
    Пример использования:
        @safe_notify
        def send_telegram_message(message):
            # отправка сообщения
            pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Ошибка в {func.__name__}: {e}")
            return None
    
    return wrapper


def handle_errors(
    log_error: bool = True,
    reraise: bool = True,
    exception_class: Optional[type] = None
) -> Callable:
    """Универсальный декоратор для обработки ошибок (без уведомлений).
    
    Args:
        log_error: Логировать ли ошибку
        reraise: Пробрасывать ли исключение после обработки
        exception_class: Класс исключения для проброса (по умолчанию - исходное исключение)
    
    Returns:
        Декорированная функция
    
    Пример использования:
        @handle_errors(reraise=True)
        def process_data(data):
            # обработка данных
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_msg = f"Ошибка в '{func.__name__}': {str(e)}"
                
                if log_error:
                    logger.error(error_msg, exc_info=True)
                
                if reraise:
                    if exception_class:
                        raise exception_class(error_msg) from e
                    else:
                        raise
                
                return None
        
        return wrapper
    return decorator

