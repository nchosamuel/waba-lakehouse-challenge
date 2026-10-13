"""Materialize separate immutable-key Bronze tables, including legacy raw history."""

import json
import boto3
from pyspark.sql import functions as F
from waba.spark_common import session, selected, merge
from waba.contracts import KEYS, REFERENCES
from waba.pipeline_config import scope

spark = session("waba-bronze")
for name, key in KEYS.items():
    frame = selected(
        spark.table(f"iceberg.raw.{name}"),
        "timestamp" if name not in REFERENCES else None,
    )
    if "ingestion_date" not in frame.columns:
        frame = frame.withColumn("ingestion_date", F.current_date())
    frame = frame.withColumn("bronze_loaded_at", F.current_timestamp())
    merge(frame, f"iceberg.bronze.{name}", ["country_code", key], "ingestion_date")

contracts = {
    "loan_positions": "country_code string, entity_type string, snapshot_date date, loan_account_id string, customer_id string, loan_type string, outstanding_amount decimal(20,3), currency string, days_past_due int, default_flag boolean, model_version string",
    "accounting_context": "event_id string, country_code string, entity_type string, event_kind string, earned_premium decimal(20,3), interest_received decimal(20,3), coverage_start date, coverage_end date, claim_id string, claim_submitted_date date, claim_paid_date date, product_id string, model_version string",
}
s3 = boto3.client("s3", endpoint_url="http://minio:9000")
keys = [
    obj["Key"]
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="raw-landing")
    for obj in page.get("Contents", [])
]
for name, contract in contracts.items():
    payload = []
    for key in sorted(keys):
        parts = key.split("/")
        if (
            len(parts) == 4
            and parts[1] == "business_context"
            and parts[0] in scope()[0]
            and parts[3] == name + ".json"
        ):
            records = json.loads(
                s3.get_object(Bucket="raw-landing", Key=key)["Body"].read()
            )
            payload.extend(json.dumps(row) for row in records)
    if not payload:
        if not spark.catalog.tableExists(f"iceberg.bronze.{name}"):
            raise ValueError(
                "Business context missing: seed the documented scenario first"
            )
        continue
    frame = spark.read.schema(contract).json(spark.sparkContext.parallelize(payload, 2))
    identity = (
        ["country_code", "loan_account_id", "snapshot_date"]
        if name == "loan_positions"
        else ["country_code", "event_id"]
    )
    if frame.groupBy(*identity).count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate business context key")
    if (
        frame.filter(
            "country_code IS NULL OR entity_type IS NULL OR model_version IS NULL"
        )
        .limit(1)
        .count()
    ):
        raise ValueError("Invalid business context schema")
    frame = frame.withColumn("ingestion_date", F.current_date())
    merge(frame, f"iceberg.bronze.{name}", identity, "ingestion_date")
spark.stop()
