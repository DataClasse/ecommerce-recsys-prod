"""
DAG для ежедневного переобучения recommendation system.

Полный пайплайн включает:
1. Создание артефактов из сырых данных (events.csv, item_properties_*.csv, category_tree.csv)
   - Валидация всех данных
   - Создание предобработанных артефактов (item_metadata.parquet, category_stats.parquet и т.д.)
2. Обучение production модели по алгоритму из финального эксперимента
   - Загрузка параметров из configs/config.yaml
   - ALS с оптимизированными весами + Content-Based Re-ranking
   - Обучение на всех данных

Отправляет уведомления в Telegram о всех этапах выполнения (успех/ошибка).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Определение корня проекта: относительно расположения DAG файла или из переменной окружения
_DAG_DIR = Path(__file__).parent.resolve()  # airflow/dags/
_AIRFLOW_DIR = _DAG_DIR.parent  # airflow/
_PROJECT_ROOT_FROM_DAG = _AIRFLOW_DIR.parent  # project_root/ (на уровень выше airflow/)

# Использование переменной окружения, если установлена, иначе - относительный путь
PROJECT_ROOT = Path(
    os.getenv("RECSYS_PROJECT_ROOT", str(_PROJECT_ROOT_FROM_DAG))
).resolve()

# Пути можно переопределить через переменные окружения для гибкости
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
DATA_DIR = Path(
    os.getenv("RECSYS_DATA_DIR", str(PROJECT_ROOT / "data" / "raw"))
).resolve()
ARTIFACTS_DIR = Path(
    os.getenv("RECSYS_ARTIFACTS_DIR", str(PROJECT_ROOT / "artifacts"))
).resolve()
CONFIG_PATH = Path(
    os.getenv("RECSYS_CONFIG_PATH", str(PROJECT_ROOT / "configs" / "config.yaml"))
).resolve()

# Проверка существования критических путей
if not PROJECT_ROOT.exists():
    raise RuntimeError(
        f"Корневая директория проекта не найдена: {PROJECT_ROOT}\n"
        f"Установите переменную окружения RECSYS_PROJECT_ROOT или проверьте структуру проекта"
    )
if not DATA_DIR.exists():
    raise RuntimeError(
        f"Директория с данными не найдена: {DATA_DIR}\n"
        f"Установите переменную окружения RECSYS_DATA_DIR или проверьте наличие data/raw"
    )
if not CONFIG_PATH.exists():
    raise RuntimeError(
        f"Конфигурационный файл не найден: {CONFIG_PATH}\n"
        f"Установите переменную окружения RECSYS_CONFIG_PATH или проверьте наличие configs/config.yaml"
    )

import sys
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Добавление plugins в PYTHONPATH для импорта модуля steps
plugins_path = PROJECT_ROOT / "airflow" / "plugins"
if str(plugins_path) not in sys.path:
    sys.path.insert(0, str(plugins_path))

# Установка MLFLOW_TRACKING_URI для логирования метрик
# Если переменная не установлена в окружении, используем значение по умолчанию
if os.getenv("MLFLOW_TRACKING_URI") is None:
    os.environ["MLFLOW_TRACKING_URI"] = os.getenv(
        "MLFLOW_TRACKING_URI_DEFAULT", 
        "http://127.0.0.1:5000"
    )
    logger.info(f"Установлен MLFLOW_TRACKING_URI: {os.environ['MLFLOW_TRACKING_URI']}")
else:
    logger.info(f"Используется MLFLOW_TRACKING_URI из окружения: {os.getenv('MLFLOW_TRACKING_URI')}")

try:
    from src.recsys.utils.telegram_notifier import (
        send_success_callback,
        send_failure_callback,
        send_dag_success_callback,
    )
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False

# Импорт функций задач из отдельного модуля
from steps.recsys_tasks import (
    validate_data,
    create_artifacts,
    train_production_model,
    validate_model,
)

default_args = {
    "owner": "recsys_ecommerce",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    # Обратные вызовы для Telegram уведомлений
    "on_success_callback": send_success_callback if TELEGRAM_AVAILABLE else None,
    "on_failure_callback": send_failure_callback if TELEGRAM_AVAILABLE else None,
}

with DAG(
    dag_id="recsys_train_daily",
    description=(
        "Ежедневное переобучение ALS recommendation system. "
        "Включает создание артефактов из сырых данных и обучение модели."
    ),
    schedule="0 3 * * *",  # Ежедневно в 3:00
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "training", "production"],
    default_args=default_args,
    on_success_callback=send_dag_success_callback if TELEGRAM_AVAILABLE else None,
    doc_md="""
    # DAG для ежедневного переобучения Recommendation System
    
    ## Описание
    Полный пайплайн переобучения модели рекомендательной системы включает:
    
    1. **Валидация данных** - проверка наличия и корректности исходных файлов
    2. **Создание артефактов** - предобработка данных и создание parquet артефактов
    3. **Обучение модели** - обучение ALS модели на всех данных
    4. **Валидация модели** - проверка качества и обнаружение деградации
    
    ## Расписание
    Ежедневно в 3:00 UTC
    
    ## Зависимости
    - `events.csv` - события пользователей
    - `item_properties_part1.csv`, `item_properties_part2.csv` - свойства товаров
    - `category_tree.csv` - иерархия категорий
    
    ## Артефакты
    - `als_model.pkl` - обученная модель
    - `user_item_matrix.npz` - матрица взаимодействий
    - `user_purchases.parquet` - история покупок для фильтрации
    - `item_metadata.parquet` - метаданные товаров
    - `category_stats.parquet` - статистика категорий
    - `category_tree.parquet` - дерево категорий
    
    ## Уведомления
    Отправляет Telegram уведомления о успехе/ошибке выполнения задач.
    
    ## Валидация модели
    При обнаружении деградации метрик >10% отправляет алерт и останавливает DAG.
    """,
) as dag:
    """DAG для ежедневного переобучения модели рекомендательной системы."""
    
    # Установка переменных окружения для всех задач
    os.environ["RECSYS_PROJECT_ROOT"] = str(PROJECT_ROOT)
    os.environ["RECSYS_DATA_DIR"] = str(DATA_DIR)
    os.environ["RECSYS_ARTIFACTS_DIR"] = str(ARTIFACTS_DIR)
    os.environ["RECSYS_CONFIG_PATH"] = str(CONFIG_PATH)
    
    # Этап 1: Валидация исходных данных
    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True,
    )
    
    # Этап 2: Создание артефактов из сырых данных
    create_artifacts_task = PythonOperator(
        task_id="create_artifacts",
        python_callable=create_artifacts,
        provide_context=True,
    )
    
    # Этап 3: Обучение production модели
    train_production_model_task = PythonOperator(
        task_id="train_production_model",
        python_callable=train_production_model,
        provide_context=True,
    )
    
    # Этап 4: Валидация обученной модели
    validate_model_task = PythonOperator(
        task_id="validate_model",
        python_callable=validate_model,
        provide_context=True,
    )
    
    # Оркестрация задач: последовательное выполнение
    validate_data_task >> create_artifacts_task >> train_production_model_task >> validate_model_task
