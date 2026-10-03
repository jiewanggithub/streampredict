# syntax=docker/dockerfile:1
# Build context: repository root (the controller ships the shared ml.registry code and the seed
# artifacts used by the registry-seed job).
FROM python:3.11.17-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app:/app/src \
    MLFLOW_DISABLE_AGENT_HINT=1 \
    CONTROLLER_DEPLOYED_REPOSITORY=/models

WORKDIR /app
COPY services/model-controller/requirements.txt .
RUN pip install -r requirements.txt \
    && useradd --create-home --uid 10001 app \
    && mkdir /models && chown app /models

COPY ml/__init__.py ml/__init__.py
COPY ml/registry ml/registry
COPY ml/artifacts ml/artifacts
COPY services/model-controller/src src
USER app
EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=30 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=2)"]

CMD ["uvicorn", "--factory", "streampredict_controller.api:app_factory", \
     "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
