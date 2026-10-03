"""Create the artifact bucket before MLflow starts (S3-compatible stores do not auto-create it)."""

import os
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

bucket = os.environ["MLFLOW_ARTIFACT_BUCKET"]
s3 = boto3.client("s3", endpoint_url=os.environ["MLFLOW_S3_ENDPOINT_URL"])
for _attempt in range(60):
    try:
        s3.head_bucket(Bucket=bucket)
        break
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"created bucket {bucket}")
        break
    except EndpointConnectionError:
        time.sleep(2)
else:
    raise SystemExit("object storage did not become reachable")
