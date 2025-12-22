# Мониторинг рекомендательной системы

## Обзор

Система мониторинга включает:
- **Prometheus** — сбор метрик из API
- **Grafana** — визуализация метрик и дашборды
- **MLflow** — логирование метрик обучения и валидации
- **Алерты** — автоматические уведомления о проблемах

Все метрики отправляются из кода проекта через `prometheus_client`.

**Запуск:**
```bash
docker compose up -d
```

**Доступ:**
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
- API Metrics: http://localhost:8000/metrics

---

## Метрики Online (API)

### Golden Signals

#### 1. Latency
- **Метрика:** `recsys_request_latency_seconds` (Histogram)
- **Перцентили:** p50, p95, p99
- **Целевые значения:** p50 < 100ms, p95 < 500ms, p99 < 1s

#### 2. Traffic
- **Метрика:** `recsys_requests_total` (Counter)
- **Лейблы:** `model_type`, `status_code`, `endpoint`
- **Использование:** Отслеживание нагрузки на систему

#### 3. Errors
- **Метрика:** `recsys_errors_total` (Counter)
- **Лейблы:** `status_code`, `endpoint`, `model_type`
- **Использование:** Отслеживание ошибок 4xx/5xx

#### 4. Saturation
- **Метрика:** Косвенно через latency и error rate
- **Индикаторы:** Рост latency при увеличении трафика, рост ошибок

### Бизнес-метрики

#### Click-Through Rate (CTR)
- **Формула:** `sum(rate(recsys_clicks_total[5m])) / sum(rate(recsys_requests_total[5m]))`
- **Целевое значение:** > 0.5 (50% пользователей кликают)

#### Conversion Rate
- **Формула:** `sum(rate(recsys_transactions_total[5m])) / sum(rate(recsys_requests_total[5m]))`
- **Целевое значение:** > 0.05 (5% конверсия)

#### Revenue
- **Метрика:** `recsys_revenue_total` (Counter)
- **Использование:** Отслеживание финансового эффекта рекомендаций

#### Add-to-Cart Rate
- **Формула:** `sum(rate(recsys_addtocart_total[5m])) / sum(rate(recsys_requests_total[5m]))`
- **Целевое значение:** > 0.1 (10% добавлений в корзину)

### ML-метрики реального времени

#### Recommendations Count
- **Метрика:** `recsys_recommendations_count` (Histogram)
- **Использование:** Отслеживание распределения количества рекомендаций

#### Items Distribution
- **Метрика:** `recsys_items_distribution` (Histogram)
- **Использование:** Отслеживание разнообразия рекомендаций (Coverage proxy)

#### Cold Users Rate
- **Формула:** `sum by (model_type) (rate(recsys_cold_users_total[15m])) / on() group_left() sum(rate(recsys_requests_total[15m])) * 100`
- **Целевое значение:** < 10%

#### Re-ranking Usage
- **Метрика:** `recsys_rerank_usage_total` (Counter)
- **Использование:** Отслеживание использования Content-Based re-ranking

---

## Метрики Offline (Обучение)

### Логирование в MLflow

Все метрики обучения и валидации логируются в MLflow при наличии `MLFLOW_TRACKING_URI`.

#### Метрики качества модели

**Primary метрики (максимизировать):**
- `precision_5` — Precision@5
- `recall_20` — Recall@20
- `ndcg_10` — NDCG@10
- `hit_rate_5` — Hit Rate@5

**Secondary метрики (guardrails):**
- `coverage_5` — Coverage@5
- `novelty_5` — Novelty@5

**Целевые значения (относительно baseline):**
- Precision@5: +3-10%
- Hit Rate@5: +1-3 п.п.
- Coverage@5 и Novelty@5: не хуже baseline

#### Параметры и артефакты

Логируются в MLflow:
- Параметры модели (factors, iterations, regularization, alpha, веса событий)
- Статистика данных (train_users, train_items, train_interactions, val_users, val_interactions)
- Артефакты: `als_model.pkl`, `metadata.json`, `validation_metrics.json`

---

## Guardrails (Защитные метрики)

### Coverage@k
- **Описание:** Доля уникальных товаров в рекомендациях
- **Цель:** Обеспечить разнообразие рекомендаций
- **Метрика:** Логируется в MLflow при валидации
- **Целевое значение:** Не хуже baseline

### Novelty@k
- **Описание:** Доля "не виденных" товаров в train (новизна)
- **Цель:** Обеспечить новизну рекомендаций
- **Метрика:** Логируется в MLflow при валидации
- **Целевое значение:** Не хуже baseline

### Cold Users Rate
- **Описание:** Доля запросов от холодных пользователей (fallback на popular)
- **Метрика:** `recsys_cold_users_total` / `recsys_requests_total` (online)
- **Целевое значение:** < 10%

### Model Degradation
- **Описание:** Снижение метрик качества модели при валидации
- **Порог:** 10% снижение (`degradation_threshold: 0.1`)
- **Действие:** Блокировка деплоя при деградации (Airflow DAG)

---

## Grafana Dashboards

### Recommender System Dashboard

**Расположение:** `configs/grafana/dashboards/recommender-system.json`

**Панели (10):**
1. CTR (Click-Through Rate)
2. Conversion Rate
3. Revenue
4. Requests by Model Type
5. Request Rate
6. Request Latency (95th percentile)
7. Error Rate
8. Recommendations Count (Median, P95)
9. Cold Users Rate
10. Re-ranking Usage

**Доступ:** http://localhost:3000 (admin/admin)

**Автоматическая настройка:**
- Datasource (Prometheus) настраивается автоматически через provisioning
- Дашборды загружаются автоматически при старте Grafana

---

## Prometheus Alerts

**Расположение:** `configs/prometheus/alerts.yml`

### Критические алерты
- **High Error Rate** — Error rate превышает 10% за последние 5 минут
- **API Unavailable** — Recommender API не отвечает более 1 минуты
- **Revenue Drop** — Revenue упал более чем на 20% по сравнению с предыдущим часом

### Предупреждающие алерты
- **High Latency** — 95-й перцентиль latency превышает 1 секунду
- **CTR Drop** — CTR упал более чем на 20% по сравнению с предыдущим часом
- **Conversion Rate Drop** — Conversion rate упал более чем на 20%
- **Low Request Rate** — Request rate менее 0.1 запросов в секунду за последние 10 минут

---

## Интеграция с кодом

### Экспорт метрик из API

Метрики экспортируются через endpoint `/metrics` в формате OpenMetrics:

```python
from prometheus_client import Counter, Histogram

REQ_TOTAL = Counter("recsys_requests_total", "Всего запросов к API", 
                    ["model_type", "status_code", "endpoint"])
REQ_LAT = Histogram("recsys_request_latency_seconds", "Задержка запросов", 
                    ["endpoint"])
CLICKS_TOTAL = Counter("recsys_clicks_total", "Всего кликов по рекомендациям")

@app.get("/recommendations/{user_id}")
async def recommend(...):
    with REQ_LAT.labels(endpoint="/recommendations/{user_id}").time():
        # ... обработка запроса ...
        REQ_TOTAL.labels(model_type="als", status_code="200", 
                        endpoint="/recommendations/{user_id}").inc()
```

### Логирование в MLflow

```python
import mlflow

with mlflow.start_run(experiment_id=experiment_id):
    mlflow.log_metric("precision_5", precision_at_5)
    mlflow.log_metric("recall_20", recall_at_20)
    mlflow.log_metric("ndcg_10", ndcg_at_10)
    mlflow.log_metric("hit_rate_5", hit_rate_at_5)
```

---

## Проверка работы мониторинга

```bash
# Проверка экспорта метрик
curl http://localhost:8000/metrics

# Проверка Prometheus
curl http://localhost:9090/api/v1/status/config

# Генерация тестового трафика
python scripts/test_api_load.py --users 10 --sessions 3
```

---

## Резюме

### Реализованные метрики

✅ **Golden Signals:** Latency, Traffic, Errors, Saturation
✅ **Бизнес-метрики:** CTR, Conversion Rate, Revenue, Add-to-Cart Rate
✅ **ML-метрики реального времени:** Recommendations Count, Items Distribution, Cold Users Rate, Re-ranking Usage
✅ **Offline метрики (MLflow):** Precision@5, Recall@20, NDCG@10, Hit Rate@5, Coverage@5, Novelty@5
✅ **Guardrails:** Coverage@k, Novelty@k, Cold Users Rate, Model Degradation Detection

### Инфраструктура

✅ **Prometheus** — сбор и хранение метрик
✅ **Grafana** — визуализация и дашборды (10 панелей)
✅ **MLflow** — логирование метрик обучения
✅ **Алерты** — автоматические уведомления (7 алертов)

### Соответствие ТЗ

✅ Все метрики отправляются из кода проекта
✅ Все сервисы контролируются метриками
✅ Документация в `.md`-файле
✅ Интеграция с Prometheus и Grafana
✅ Алерты для критических ситуаций
