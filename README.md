# E-commerce Recommender System

**Продакшен-готовый e-commerce рекомендательный сервис:** ALS, re-ranking, мониторинг и переобучение по расписанию.

## Описание проекта

**Цель:** Разработка рекомендательной системы для электронной коммерции.

**Задача:** Предсказать, какие товары предложить пользователю интернет-магазина, с фокусом на добавления товаров в корзину (add-to-cart).

**Используемые технологии:**
- **Алгоритмы:** ALS (Alternating Least Squares), популярные товары как fallback
- **Инфраструктура:** MLflow, FastAPI, Airflow, Prometheus, Grafana
- **Библиотеки:** Polars, implicit, scipy.sparse

---

## Клонирование репозитория

```bash
git clone https://github.com/DataClasse/recsys-production.git
cd recsys-production
```

---

## Структура проекта

```
recsys-production/
├── data/raw/              # Исходные данные
├── src/recsys/            # Библиотечный код
├── services/recommender_api/  # FastAPI сервис
├── airflow/dags/          # Airflow DAG для переобучения
├── configs/               # Конфигурации (Prometheus, Grafana)
├── artifacts/             # Артефакты (модели, метрики)
├── docs/                  # Документация
├── requirements.txt       # Зависимости Python
├── rms.sh                 # Скрипт запуска MLflow
├── docker-compose.yml     # Docker Compose конфигурация
└── README.md              # Этот файл
```

---

## Быстрый старт

### 1. Установка зависимостей

```bash
cd recsys-production
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

### 2. Подготовка данных

Данные должны быть размещены в `data/raw/`:
- `events.csv` — логи событий
- `item_properties_part1.csv`, `item_properties_part2.csv` — свойства товаров
- `category_tree.csv` — дерево категорий

### 3. Запуск инфраструктуры

**MLflow:**
```bash
./rms.sh
```

**Airflow:**
```bash
export AIRFLOW_HOME=~/airflow
airflow webserver --port 8080 --daemon
airflow scheduler --daemon
```

**Docker Compose (API, Prometheus, Grafana):**
```bash
docker compose up -d --build
```

**Доступ:**
- MLflow: http://127.0.0.1:5000
- Airflow: http://localhost:8080
- API: http://localhost:8000
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

---

## Основные команды

### Обучение модели

```bash
export PYTHONPATH=$(pwd)/src
python3 -m src.recsys.scripts.train_production_model \
  --config configs/config.yaml \
  --data-dir data/raw \
  --artifacts-dir artifacts
```

### Запуск API

```bash
export PYTHONPATH=$(pwd)/src
uvicorn services.recommender_api.app:app --host 0.0.0.0 --port 8000
```

### Тестирование API

```bash
python scripts/test_api_load.py --users 10 --sessions 3
```

### DVC (версионирование данных и моделей)

```bash
# Инициализация
pip install dvc[s3]
dvc init --no-scm
dvc cache dir ~/.cache/dvc

# Настройка S3 (если нужно)
source .env
dvc remote add -d storage "s3://${S3_BUCKET_NAME}/dvc"
dvc remote modify storage endpointurl "$S3_ENDPOINT_URL"

# Запуск пайплайна
dvc repro
```

---

## Документация

- **Мониторинг:** [`docs/Monitoring.md`](docs/Monitoring.md) — описание метрик и дашбордов

---

## Ключевые метрики

**Offline (обучение):**
- Precision@5, Recall@20, NDCG@10, Hit Rate@5
- Coverage@5, Novelty@5

**Online (API):**
- CTR (Click-Through Rate)
- Conversion Rate
- Revenue
- Latency (p50, p95, p99)
- Error Rate

Подробнее: [`docs/Monitoring.md`](docs/Monitoring.md)

---

## Руководство по проекту

### Трансляция бизнес-задачи в техническую задачу

**Бизнес-задача:** Предсказать, какие товары предложить пользователю интернет-магазина для максимизации добавлений товаров в корзину (add-to-cart).

**Техническая задача:** Построить рекомендательную систему, которая:
- Генерирует персонализированные рекомендации для каждого пользователя
- Учитывает историю взаимодействий (просмотры, добавления в корзину, покупки)
- Использует метаданные товаров (категории) для улучшения качества
- Обрабатывает cold-start проблему (новые пользователи и товары)

**Метрики качества:**
- **Offline (обучение):** Recall@20, Precision@5, NDCG@10, Hit Rate@5 — для оценки качества модели на исторических данных
- **Online (API):** CTR, Conversion Rate, Revenue — для оценки бизнес-эффективности в продакшене

**Подход к решению:**
1. **Collaborative Filtering (ALS)** — для warm users на основе истории взаимодействий
2. **Content-Based Re-ranking** — улучшение рекомендаций на основе категорий товаров
3. **Popular Items Fallback** — для cold users и товаров без истории

---

### Разворачивание инфраструктуры обучения модели

**MLflow** используется для логирования экспериментов, метрик и артефактов.

**Запуск MLflow:**
```bash
./rms.sh
```

Скрипт `rms.sh` выполняет:
1. Загрузку переменных окружения из `.env` (PostgreSQL, S3 credentials)
2. Проверку подключения к PostgreSQL
3. Запуск MLflow сервера с:
   - Backend store: PostgreSQL (для метаданных экспериментов)
   - Artifact store: Yandex S3 (для артефактов: модели, метрики)
   - Логирование в `logs/mlflow/mlflow.log`

**Доступ:** http://127.0.0.1:5000

**Использование:**
- Все скрипты обучения (`create_artifacts.py`, `train_production_model.py`, `validate_model.py`) автоматически логируют метрики в MLflow
- Эксперимент: `recsys_production`
- Параметры, метрики и артефакты сохраняются для каждого запуска

---

### Проведение EDA

Полный анализ данных выполнен в [`notebooks/eda_processing.ipynb`](notebooks/eda_processing.ipynb).

**Ключевые выводы:**

1. **Разреженность данных:**
   - 99.9992% разреженность матрицы взаимодействий
   - Медиана: 1.0 событие на пользователя (50% пользователей имеют только 1 событие)
   - Экстремальная cold-start проблема

2. **Распределение событий:**
   - 96.68% views (слабый сигнал)
   - 2.50% addtocart (средний сигнал)
   - 0.81% transaction (сильный сигнал)
   - **Вывод:** Необходимы правильные веса событий (view=1.0, addtocart=4.0, transaction=8.0)

3. **Покрытие метаданными:**
   - 78.81% товаров из событий имеют категории
   - Все товары с метаданными имеют категории
   - **Вывод:** Можно использовать категории для content-based подходов

4. **Дерево категорий:**
   - 1,668 категорий, максимальная глубина: 6 уровней
   - Средняя глубина: 3.40 уровня
   - **Вывод:** Необходима рекурсивная обработка иерархии категорий

5. **Рекомендации для модели:**
   - Обязательный fallback для cold users (popular items)
   - Использование категорий для re-ranking и cold-start стратегий
   - Мониторинг Coverage метрики (много редких товаров)

---

### Генерация признаков и обучение модели

**Признаки:**
1. **User-Item взаимодействия:**
   - Взвешенные события: `weight_view * views + weight_addtocart * addtocart + weight_transaction * transactions`
   - Фильтрация: `min_user_interactions=5`, `min_item_interactions=5`

2. **Метаданные товаров:**
   - Категория товара (`category_id`)
   - Уровень категории в дереве (`level`)
   - Доступность товара (`available_value`)
   - Популярность товара (`n_events`, `n_users`, `weighted_score`)

3. **Статистика категорий:**
   - Количество событий, пользователей, товаров по категориям
   - Используется для re-ranking

**Эксперименты:**
Проведено 3 эксперимента (см. [`notebooks/experiments_processing.ipynb`](notebooks/experiments_processing.ipynb)):

1. **Baseline:** Popular Items (топ-100 по addtocart/transaction)
2. **Промежуточный:** ALS с оптимизированными весами (factors=256, alpha=4.0)
3. **Финальный:** ALS + Content-Based Re-ranking (alpha=0.6, beta=0.4)

**Результаты финального эксперимента:**
- Recall@20: 0.250670
- Precision@5: 0.033069
- NDCG@10: 0.118260
- Hit Rate@5: 0.156436
- Улучшение vs Baseline: 1132% по Recall@20

**В MLflow можно увидеть:**
- Параметры модели (factors, iterations, regularization, alpha)
- Веса событий (view, addtocart, transaction)
- Метрики валидации (Recall@20, Precision@5, NDCG@10, Hit Rate@5, Coverage@5, Novelty@5, Diversity@5)
- Артефакты: обученная модель, метрики, конфигурация

---

### Разворачивание инфраструктуры применения модели

**Docker Compose** оркестрирует все сервисы:
- **Recommender API** (FastAPI) — генерация рекомендаций
- **Prometheus** — сбор метрик
- **Grafana** — визуализация и дашборды

**Запуск:**
```bash
docker compose up -d --build
```

**Автоматическая настройка:**
- Grafana provisioning: datasource и dashboards загружаются автоматически
- Prometheus scraping: конфигурация из `configs/prometheus/prometheus.yml`
- Health checks: автоматическая проверка работоспособности сервисов

**Доступ:**
- API: http://localhost:8000
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

**Airflow DAG** для автоматического переобучения:
- Расписание: ежедневно в 3:00 UTC
- Этапы: валидация данных → создание артефактов → обучение модели → валидация модели
- Уведомления: Telegram при успехе/ошибке
- Версионирование: DVC для артефактов и модели

**Мониторинг:**
- Golden Signals: Latency, Traffic, Errors, Saturation
- Бизнес-метрики: CTR, Conversion Rate, Revenue
- ML-метрики: Cold Users Rate, Re-ranking Usage, Recommendations Count
- Алерты: автоматические уведомления при превышении порогов

Подробнее: [`docs/Monitoring.md`](docs/Monitoring.md)

---

## Воспроизводимость

- **Python:** 3.11
- **Зависимости:** `requirements.txt`
- **Random seed:** `random_state=42`
- **Окружение:** Рекомендуется виртуальное окружение (`.venv`)

---

## Контакты

Исполнитель Дмитрий Щербаков
