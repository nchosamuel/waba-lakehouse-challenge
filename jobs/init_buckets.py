import os

os.chown("/catalog", 1000, 1000)
for directory in ("/airflow-state", "/airflow-logs"):
    if os.path.isdir(directory):
        os.chown(directory, 50000, 0)
import time
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

client = boto3.client("s3", endpoint_url="http://minio:9000")
for attempt in range(60):
    try:
        existing = {b["Name"] for b in client.list_buckets()["Buckets"]}
        for name in ("raw-landing", "lakehouse", "archive"):
            if name not in existing:
                client.create_bucket(Bucket=name)
        print("Buckets ready")
        break
    except (ClientError, EndpointConnectionError):
        if attempt == 59:
            raise
        time.sleep(2)
