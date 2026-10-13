"""Bounded read-only topic inspection for the integration verification."""

import json, time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from waba.publisher import client

topics = (
    "raw-bank-transactions",
    "raw-insurance-operations",
    "raw-mobile-money-payments",
    "raw-loan-repayments",
    "silver-bank-transactions",
    "silver-insurance-operations",
    "silver-mobile-money",
    "silver-loan-repayments",
    "gold-fraud-alerts",
    "gold-aml-events",
    "gold-liquidity-alerts",
    "dlq-financial-events",
)
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    api_version=(2, 6, 0),
    consumer_timeout_ms=3000,
)
fixture = json.loads(
    client()
    .get_object(Bucket="archive", Key="reports/streaming/latest-fixture.json")["Body"]
    .read()
)
fixture_ids = set(fixture["event_ids"].values())
published = (
    datetime.fromisoformat(fixture["published_at_utc"])
    .replace(tzinfo=timezone.utc)
    .timestamp()
)
counts = {t: 0 for t in topics}
events = {}
raw_latencies = {}
file_latencies = {}
try:
    for message in consumer:
        counts[message.topic] += 1
        try:
            value = json.loads(message.value)
        except json.JSONDecodeError:
            value = {}
        identifier = next(
            (
                value.get(k)
                for k in (
                    "transaction_id",
                    "operation_id",
                    "payment_id",
                    "repayment_id",
                    "alert_id",
                    "message_id",
                )
                if value.get(k)
            ),
            None,
        )
        events.setdefault(message.topic, []).append(identifier)
        if message.topic.startswith("raw-") and identifier in fixture_ids:
            key = (message.topic, identifier)
            elapsed = message.timestamp / 1000 - published
            file_latencies[key] = min(file_latencies.get(key, elapsed), elapsed)
        if (
            message.topic.startswith("raw-")
            and identifier in fixture_ids
            and value.get("ingestion_timestamp")
        ):
            ingested = datetime.fromisoformat(
                value["ingestion_timestamp"].replace("Z", "+00:00")
            )
            key = (message.topic, identifier)
            elapsed = message.timestamp / 1000 - ingested.timestamp()
            raw_latencies[key] = min(raw_latencies.get(key, elapsed), elapsed)
        if sum(counts.values()) >= 5000:
            break
finally:
    consumer.close()
print(
    json.dumps(
        {
            "topic_counts": counts,
            "event_ids": events,
            "nifi_to_kafka_latency_max_seconds": (
                max(raw_latencies.values()) if raw_latencies else None
            ),
            "file_to_kafka_latency_max_seconds": (
                max(file_latencies.values()) if file_latencies else None
            ),
        }
    )
)
