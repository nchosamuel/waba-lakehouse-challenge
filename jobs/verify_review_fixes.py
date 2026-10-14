"""Isolated Iceberg regression fixtures: interruption/replay and late-event rules."""
import json
from datetime import datetime, timezone
import boto3
from pyspark.sql import functions as F
from waba.spark_common import session
from waba.lambda_snapshot import publish
from waba.streaming_rules import high_value_windows, individual_alerts

spark = session("waba-review-regression")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
batch_table = "iceberg.gold.review_lambda_batch"
stream_table = "iceberg.gold.review_lambda_stream"
# Dedicated synthetic fixtures, never customer tables.
batch_schema = "country_code string,entity_type string,transaction_id string,txn_date date,transaction_type string,transaction_status string,amount_eur double"
before = spark.createDataFrame([
    ("CI", "BANK", "review-old", datetime(2026, 10, 13).date(), "TRANSFER", "SUCCESS", 10.0),
    ("GH", "BANK", "review-gh", datetime(2026, 10, 13).date(), "TRANSFER", "SUCCESS", 7.0),
], batch_schema)
publish(before, batch_table)
old_snapshot = spark.sql(f"SELECT snapshot_id FROM {batch_table}.snapshots ORDER BY committed_at DESC").first()[0]
stream = spark.createDataFrame([
    ("CI", "BANK", "review-new", "2026-10-13T10:00:00Z", "TRANSFER", "SUCCESS", 20.0, 0, 1),
    ("CI", "BANK", "review-new", "2026-10-13T10:00:00Z", "TRANSFER", "SUCCESS", 20.0, 0, 2),
], "country_code string,entity_type string,event_id string,event_time string,transaction_type string,transaction_status string,amount_eur double,_partition_id int,_partition_offset long")
stream.writeTo(stream_table).using("iceberg").tableProperty("format-version", "2").createOrReplace()
after = before.unionByName(spark.createDataFrame([
    ("CI", "BANK", "review-new", datetime(2026, 10, 13).date(), "TRANSFER", "SUCCESS", 20.0),
], batch_schema))
try:
    publish(after, batch_table)
    raise RuntimeError("Injected interruption before independent daily aggregate write")
except RuntimeError as exc:
    assert str(exc).startswith("Injected interruption")
after_snapshot = spark.sql(f"SELECT snapshot_id FROM {batch_table}.snapshots ORDER BY committed_at DESC").first()[0]
publish(after, batch_table)  # A retry must not duplicate facts.
assert spark.table(batch_table).count() == 3

schema = "country_code string,entity_type string,event_id string,event_time timestamp,source_topic string,subject_id string,amount double,amount_eur double,currency string,status string,transaction_type string,operation_type string,sender_country string,sender_home_country string,annual_premium double"
def row(key, minute, topic, amount, status="SUCCESS", **kw):
    return ("CI", kw.get("entity", "BANK"), key, datetime(2026,10,13,10,minute), topic, "review-subject", float(amount), amount/655.957, "XOF", status, "TRANSFER", kw.get("operation"), kw.get("sender"), kw.get("home"), kw.get("premium"))
old = row("review-aml", 0, "silver-bank-transactions", 1200000)
neighbor = row("review-neighbor", 1, "silver-bank-transactions", 600000)
mobile = row("review-mobile", 2, "silver-mobile-money", 100, entity="MOBILE_MONEY", sender="CI", home="SN")
claim = row("review-claim", 3, "silver-insurance-operations", 400, status="PAID", entity="INSURANCE", operation="CLAIM_PAYMENT", premium=100.0)
new = list(row("review-latest", 4, "silver-bank-transactions", 100));new[3]=datetime(2026,10,13,11,0)
incoming = spark.createDataFrame([old, mobile, claim, tuple(new)], schema).withColumn("txn_date", F.to_date("event_time"))
history = incoming.unionByName(spark.createDataFrame([neighbor],schema).withColumn("txn_date", F.to_date("event_time")))
alerts = individual_alerts(incoming)
counts = {key: frame.count() for key, frame in alerts.items()}
assert all(value == 1 for value in counts.values()), counts
windows = high_value_windows(history, incoming)
assert windows.count() > 0
assert windows.agg(F.max("event_count")).first()[0] == 2
# No high-value event in a microbatch is a valid no-op, not a failing aggregation.
assert high_value_windows(history, incoming.filter("event_id='review-latest'")) is None
report = {"verified_at_utc": datetime.now(timezone.utc).isoformat(),
    "batch_table": batch_table, "stream_table": stream_table,
    "old_snapshot_id": old_snapshot, "after_interruption_snapshot_id": after_snapshot,
    "expected_unified_total_eur": 37.0, "retry_fact_count": 3,
    "late_individual_rule_counts": counts, "late_high_value_windows": windows.count(),
    "global_time_span_minutes": 60, "injected_interruption": True}
boto3.client("s3",endpoint_url="http://minio.ingestion:9000").put_object(
    Bucket="archive",Key="verification/review-fixes.json",Body=json.dumps(report).encode())
print(json.dumps({"event":"review_regression_completed",**report}),flush=True)
spark.stop()
