"""Create topics idempotently without deleting any broker data."""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json

names = (
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
admin = KafkaAdminClient(bootstrap_servers="kafka:9092", api_version=(2, 6, 0))
try:
    present = set(admin.list_topics())
    missing = [
        NewTopic(
            name,
            num_partitions=3,
            replication_factor=1,
            topic_configs={"retention.ms": "604800000"},
        )
        for name in names
        if name not in present
    ]
    if missing:
        admin.create_topics(missing)
finally:
    admin.close()
print(json.dumps({"event": "kafka_topics_ready", "topics": names}))
