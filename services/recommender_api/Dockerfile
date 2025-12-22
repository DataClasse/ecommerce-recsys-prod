FROM python:3.11-slim

# Установка системных зависимостей для implicit и scipy
RUN apt-get update && apt-get install -y \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    MODEL_DIR=/app/artifacts/model \
    ARTIFACTS_DIR=/app/artifacts \
    ENABLE_RERANK=false \
    LOG_LEVEL=INFO

# Установка зависимостей
COPY services/recommender_api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Копирование кода
COPY src /app/src
COPY services /app/services

# Удаление старых файлов (если есть от предыдущих сборок)
RUN rm -f /app/app.py /app/recommender.py /app/events_store.py 2>/dev/null || true

# Копирование артефактов модели (опционально, можно монтировать как volume)
COPY artifacts /app/artifacts

# Создание директории для логов
RUN mkdir -p /app/logs

EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health', timeout=5)" || exit 1

CMD ["uvicorn", "services.recommender_api.app:app", "--host", "0.0.0.0", "--port", "8000"]


