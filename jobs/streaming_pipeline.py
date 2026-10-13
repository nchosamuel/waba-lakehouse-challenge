"""Two checkpointed Kafka Structured Streaming jobs with durable keyed sinks."""

import json
import threading
import os
import time
from pathlib import Path
from datetime import datetime, timezone
from datetime import timedelta
from pyspark import StorageLevel
from pyspark.sql import functions as F
from waba.spark_common import session, merge
from waba.contracts import FLOW_PREFIX, KEYS
from waba.streaming_enrichment import parse_and_validate, enrich, align_and_merge
from waba.streaming_io import queue, flush

write_lock = threading.RLock()
spark = session("waba-lambda-streaming")
RAW_TOPICS = {name: "raw-" + name.replace("_", "-") for name in FLOW_PREFIX}
SILVER_TOPICS = {
    "bank_transactions": "silver-bank-transactions",
    "insurance_operations": "silver-insurance-operations",
    "mobile_money_payments": "silver-mobile-money",
    "loan_repayments": "silver-loan-repayments",
}


def read(topics):
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 250)
        .option("failOnDataLoss", "true")
        .load()
    )
    stream = stream.select(
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("_broker_timestamp"),
        F.col("value").cast("string").alias("raw_json"),
    )
    business = F.coalesce(
        *[F.get_json_object("raw_json", "$.{}".format(KEYS[n])) for n in FLOW_PREFIX]
    )
    stream = stream.withColumn(
        "_event_id",
        F.coalesce(
            business,
            F.concat_ws("/", F.col("topic"), F.col("partition"), F.col("offset")),
        ),
    )
    stream = stream.withColumn(
        "_event_time",
        F.coalesce(
            F.when(
                F.to_timestamp(F.get_json_object("raw_json", "$.timestamp"))
                <= F.col("_broker_timestamp") + F.expr("INTERVAL 5 MINUTES"),
                F.to_timestamp(F.get_json_object("raw_json", "$.timestamp")),
            ),
            F.col("_broker_timestamp"),
        ),
    )
    return stream.withWatermark(
        "_event_time", "10 minutes"
    ).dropDuplicatesWithinWatermark(["topic", "_event_id"])


def dlq(frame, spark, reason_column="reject_reason"):
    if not frame.limit(1).count():
        return
    rejected = frame.select(
        F.coalesce("country_code", F.lit("UNKNOWN")).alias("country_code"),
        F.col("entity_type"),
        "topic",
        "partition",
        "offset",
        "raw_json",
        F.col(reason_column).alias("reason"),
    )
    rejected = rejected.withColumn(
        "message_id",
        F.sha2(
            F.concat_ws("/", F.col("topic"), F.col("partition"), F.col("offset")), 256
        ),
    )
    queue(rejected, "dlq-financial-events", "message_id", spark)


def raw_to_silver_impl(batch, batch_id):
    if not batch.limit(1).count():
        flush(spark)
        return
    for name, topic in RAW_TOPICS.items():
        source = batch.filter(F.col("topic") == topic)
        if not source.limit(1).count():
            continue
        parsed = parse_and_validate(source, name, spark).persist(StorageLevel.DISK_ONLY)
        dlq(parsed.filter("length(reject_reason)>0"), spark)
        good = parsed.filter("length(reject_reason)=0")
        if good.limit(1).count():
            enriched = enrich(good, name, spark).persist(StorageLevel.DISK_ONLY)
            invalid = enriched.filter("NOT coalesce(_fx_valid,false)").select(
                KEYS[name], "country_code"
            )
            bad_fx = good.join(
                invalid, [KEYS[name], "country_code"], "inner"
            ).withColumn("reject_reason", F.lit("Missing dated FX rate"))
            dlq(bad_fx, spark)
            accepted = enriched.filter("_fx_valid=true")
            if accepted.limit(1).count():
                canonical = align_and_merge(accepted, name, spark)
                queue(canonical, SILVER_TOPICS[name], KEYS[name], spark)
            enriched.unpersist()
        parsed.unpersist()
    flush(spark)
    print(
        json.dumps({"event": "raw_stream_batch_committed", "batch_id": batch_id}),
        flush=True,
    )


def gold_alert(frame, table, topic):
    if not frame.limit(1).count():
        return
    frame = frame.withColumn(
        "alert_id",
        F.sha2(
            F.concat_ws(
                "/",
                F.col("rule"),
                F.col("country_code"),
                F.col("subject_id"),
                F.col("window_start").cast("string"),
            ),
            256,
        ),
    ).withColumn("detected_at", F.current_timestamp())
    merge(frame, "iceberg.gold." + table, ["country_code", "alert_id"], "txn_date")
    queue(frame, topic, "alert_id", spark)


def silver_to_gold_impl(batch, batch_id):
    if not batch.limit(1).count():
        flush(spark)
        return
    jsoncol = lambda name: F.get_json_object("raw_json", "$.{}".format(name))
    ledger = batch.select(
        jsoncol("country_code").alias("country_code"),
        jsoncol("entity_type").alias("entity_type"),
        F.col("_event_id").alias("event_id"),
        F.col("_event_time").alias("event_time"),
        F.col("topic").alias("source_topic"),
        F.col("raw_json").alias("payload"),
        F.coalesce(
            jsoncol("account_id"), jsoncol("loan_account_id"), jsoncol("sender_id")
        ).alias("subject_id"),
        jsoncol("amount").cast("decimal(20,3)").alias("amount"),
        jsoncol("amount_eur").cast("decimal(20,6)").alias("amount_eur"),
        jsoncol("currency").alias("currency"),
        F.coalesce(
            jsoncol("transaction_status"),
            jsoncol("status"),
            jsoncol("claim_status"),
            jsoncol("repayment_status"),
        ).alias("status"),
        jsoncol("transaction_type").alias("transaction_type"),
        jsoncol("operation_type").alias("operation_type"),
        jsoncol("sender_country").alias("sender_country"),
        jsoncol("sender_home_country").alias("sender_home_country"),
        jsoncol("annual_premium").cast("decimal(20,3)").alias("annual_premium"),
    )
    ledger = ledger.withColumn("txn_date", F.to_date("event_time")).dropDuplicates(
        ["country_code", "event_id"]
    )
    merge(
        ledger,
        "iceberg.gold.streaming_event_ledger",
        ["country_code", "event_id"],
        "txn_date",
    )
    history = spark.table("iceberg.gold.streaming_event_ledger")
    maximum = history.agg(F.max("event_time")).first()[0]
    recent = history.filter(
        (F.col("txn_date") >= F.lit((maximum - timedelta(minutes=10)).date()))
        & (F.col("event_time") >= F.lit(maximum - timedelta(minutes=10)))
    )
    bank = recent.filter("source_topic='silver-bank-transactions' AND status='SUCCESS'")
    large = bank.filter("currency='XOF' AND amount>500000")
    windows = large.groupBy(
        "country_code",
        "entity_type",
        "subject_id",
        F.window("event_time", "5 minutes", "1 minute"),
    ).agg(F.count("*").alias("event_count"), F.sum("amount_eur").alias("amount_eur"))
    windows = windows.filter("event_count>=2").select(
        "country_code",
        "entity_type",
        "subject_id",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "event_count",
        "amount_eur",
    )
    windows = windows.withColumn("txn_date", F.to_date("window_start")).withColumn(
        "rule", F.lit("MULTIPLE_HIGH_VALUE_TRANSFERS")
    )
    gold_alert(windows, "fraud_alerts", "gold-fraud-alerts")
    for rule, condition in (
        (
            "UNUSUAL_COUNTRY_FOR_PROFILE",
            "source_topic='silver-mobile-money' AND status='SUCCESS' AND sender_country<>sender_home_country",
        ),
        (
            "CLAIM_OVER_THREE_ANNUAL_PREMIUMS",
            "source_topic='silver-insurance-operations' AND operation_type='CLAIM_PAYMENT' AND status='PAID' AND annual_premium>0 AND amount>3*annual_premium",
        ),
    ):
        selected = recent.filter(condition).select(
            "country_code",
            "entity_type",
            "subject_id",
            F.col("event_time").alias("window_start"),
            F.col("event_time").alias("window_end"),
            F.lit(1).alias("event_count"),
            "amount_eur",
            "txn_date",
        )
        selected = selected.withColumn(
            "subject_id",
            F.concat_ws("/", F.col("subject_id"), F.col("window_start").cast("string")),
        ).withColumn("rule", F.lit(rule))
        gold_alert(selected, "fraud_alerts", "gold-fraud-alerts")
    aml = bank.filter(
        "transaction_type IN ('TRANSFER','INTERNATIONAL_WIRE') AND ((currency='XOF' AND amount>1000000) OR (currency='GHS' AND amount>5000))"
    )
    aml = aml.select(
        "country_code",
        "entity_type",
        F.col("event_id").alias("subject_id"),
        F.col("event_time").alias("window_start"),
        F.col("event_time").alias("window_end"),
        F.lit(1).alias("event_count"),
        "amount_eur",
        "txn_date",
    ).withColumn("rule", F.lit("CHALLENGE_AML_THRESHOLD"))
    gold_alert(aml, "aml_events", "gold-aml-events")
    opening = (
        spark.table("iceberg.silver.accounts")
        .filter(
            "entity_type='BANK' AND account_type IN ('CURRENT','SAVINGS') AND status='ACTIVE'"
        )
        .groupBy("country_code")
        .agg(F.sum("balance").alias("opening_balance"))
    )
    # Durable ledger totals prevent a replay from debiting the same transfer twice.
    movements = (
        history.filter(
            "source_topic='silver-bank-transactions' AND status='SUCCESS' AND entity_type='BANK'"
        )
        .groupBy("country_code", "entity_type")
        .agg(
            F.sum(
                F.when(F.col("transaction_type") == "DEPOSIT", F.col("amount"))
                .when(
                    F.col("transaction_type").isin("WITHDRAWAL"),
                    -F.col("amount"),
                )
                .otherwise(0)
            ).alias("net_movement")
        )
    )
    liquidity = (
        movements.join(opening, "country_code")
        .withColumn(
            "remaining_liquidity", F.col("opening_balance") + F.col("net_movement")
        )
        .withColumn(
            "coverage_ratio", F.col("remaining_liquidity") / F.col("opening_balance")
        )
    )
    liquidity = (
        liquidity.filter("coverage_ratio<0.10")
        .withColumn("subject_id", F.lit("country-cash"))
        .withColumn("window_start", F.lit(maximum).cast("timestamp"))
        .withColumn("window_end", F.lit(maximum).cast("timestamp"))
        .withColumn("txn_date", F.to_date("window_start"))
        .withColumn("event_count", F.lit(1))
        .withColumn("amount_eur", F.lit(None).cast("decimal(20,6)"))
        .withColumn("rule", F.lit("DEMO_LIQUIDITY_COVERAGE_BELOW_10_PERCENT"))
    )
    # Use an identical schema for notifications; audit balance details remain on ledger/source accounts.
    gold_alert(
        liquidity.select(
            "country_code",
            "entity_type",
            "subject_id",
            "window_start",
            "window_end",
            "txn_date",
            "event_count",
            "amount_eur",
            "rule",
        ),
        "liquidity_alerts",
        "gold-liquidity-alerts",
    )
    flush(spark)
    print(
        json.dumps({"event": "gold_stream_batch_committed", "batch_id": batch_id}),
        flush=True,
    )


def raw_to_silver(batch, batch_id):
    with write_lock:
        return raw_to_silver_impl(batch, batch_id)


def silver_to_gold(batch, batch_id):
    with write_lock:
        return silver_to_gold_impl(batch, batch_id)


flush(spark)  # Recover any acknowledged/unpublished outbox from an interrupted batch.

raw_query = (
    read(list(RAW_TOPICS.values()))
    .writeStream.queryName("raw_to_silver")
    .foreachBatch(raw_to_silver)
    .option("checkpointLocation", "/checkpoints/raw-to-silver")
    .trigger(processingTime="10 seconds")
    .start()
)
gold_query = (
    read(list(SILVER_TOPICS.values()))
    .writeStream.queryName("silver_to_gold")
    .foreachBatch(silver_to_gold)
    .option("checkpointLocation", "/checkpoints/silver-to-gold")
    .trigger(processingTime="10 seconds")
    .start()
)
while True:
    queries = [raw_query, gold_query]
    status = {
        "heartbeat_utc": datetime.now(timezone.utc).isoformat(),
        "queries": [
            {
                "name": q.name,
                "active": q.isActive,
                "recent_progress": q.recentProgress[-5:],
            }
            for q in queries
        ],
    }
    path = Path("/checkpoints/stream-health.json")
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(status))
    os.replace(str(temp), str(path))
    spark.streams.awaitAnyTermination(15)
