"""Rules on every accepted event; only multi-event fraud uses sliding windows."""
from datetime import timedelta
from pyspark.sql import functions as F


def high_value_windows(history, incoming):
    large_incoming = incoming.filter(
        "source_topic='silver-bank-transactions' AND status='SUCCESS' AND currency='XOF' AND amount>500000"
    )
    bounds = large_incoming.agg(F.min("event_time"), F.max("event_time")).first()
    if bounds[0] is None:
        return None
    keys = ["country_code", "entity_type", "subject_id", "window"]
    affected = large_incoming.select(
        "country_code", "entity_type", "subject_id",
        F.window("event_time", "5 minutes", "1 minute").alias("window"),
    ).distinct()
    # Include neighbours of accepted late events, independent of the global max.
    candidates = history.filter(
        (F.col("event_time") >= F.lit(bounds[0] - timedelta(minutes=5)))
        & (F.col("event_time") <= F.lit(bounds[1] + timedelta(minutes=5)))
    ).filter(
        "source_topic='silver-bank-transactions' AND status='SUCCESS' AND currency='XOF' AND amount>500000"
    )
    return candidates.withColumn(
        "window", F.window("event_time", "5 minutes", "1 minute")
    ).join(affected, keys, "inner").groupBy(*keys).agg(
        F.count("*").alias("event_count"), F.sum("amount_eur").alias("amount_eur")
    ).filter("event_count>=2")


def individual_alerts(incoming):
    conditions = {
        "UNUSUAL_COUNTRY_FOR_PROFILE": "source_topic='silver-mobile-money' AND status='SUCCESS' AND sender_country<>sender_home_country",
        "CLAIM_OVER_THREE_ANNUAL_PREMIUMS": "source_topic='silver-insurance-operations' AND operation_type='CLAIM_PAYMENT' AND status='PAID' AND annual_premium>0 AND amount>3*annual_premium",
        "CHALLENGE_AML_THRESHOLD": "source_topic='silver-bank-transactions' AND status='SUCCESS' AND transaction_type IN ('TRANSFER','INTERNATIONAL_WIRE') AND ((currency='XOF' AND amount>1000000) OR (currency='GHS' AND amount>5000))",
    }
    return {
        rule: incoming.filter(condition).select(
            "country_code", "entity_type", F.col("event_id").alias("subject_id"),
            F.col("event_time").alias("window_start"),
            F.col("event_time").alias("window_end"),
            F.lit(1).alias("event_count"), "amount_eur", "txn_date",
        ).withColumn("rule", F.lit(rule))
        for rule, condition in conditions.items()
    }
