# syntax=docker/dockerfile:1
# MLflow tracking server + model registry: Postgres metadata, S3-compatible (SeaweedFS) artifacts.
FROM ghcr.io/mlflow/mlflow:v3.16.1

RUN pip install --no-cache-dir psycopg2-binary==2.9.13 boto3==1.43.108
COPY init_bucket.py /opt/init_bucket.py
