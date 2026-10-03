# syntax=docker/dockerfile:1
# Build context: services/ (the consumer reuses the gateway's prediction and event modules).
FROM python:3.11.17-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/api/src:/app/consumer/src

WORKDIR /app
COPY api/requirements.txt .
RUN pip install -r requirements.txt \
    && useradd --create-home --uid 10001 app

COPY api/src ./api/src
COPY consumer/src ./consumer/src
USER app
EXPOSE 9102

CMD ["python", "-m", "streampredict_consumer"]
