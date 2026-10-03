# syntax=docker/dockerfile:1
FROM python:3.11.17-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src \
    SERVING_MODEL_REPOSITORY=/models

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt \
    && useradd --create-home --uid 10001 app

COPY src ./src
# The committed demo repository is baked in; mount another repository at /models to override.
COPY --chown=app model_repository /models
USER app
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/v2/health/ready', timeout=2)"]

CMD ["uvicorn", "--factory", "streampredict_serving.server:app_factory", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--timeout-graceful-shutdown", "10", "--no-access-log"]
