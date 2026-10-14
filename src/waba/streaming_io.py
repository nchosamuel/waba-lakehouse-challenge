"""Durable outbox with acknowledged Kafka publishing and idempotent Iceberg keys.

Kafka publishing is at least once across a crash between Kafka acknowledgment
and the outbox commit. Downstream consumers and Lambda reads deduplicate by key.
"""

import json
from kafka import KafkaProducer
from pyspark.sql import functions as F
from waba.spark_common import create

OUTBOX = "iceberg.silver.streaming_outbox"


def queue(frame, topic, key, spark):
    records = frame.select(
        "country_code",
        "entity_type",
        F.lit(topic).alias("topic"),
        F.col(key).cast("string").alias("message_id"),
        F.to_json(F.struct(*[F.col(c) for c in frame.columns])).alias("payload"),
    )
    records = records.withColumn("created_at", F.current_timestamp()).withColumn(
        "published", F.lit(False)
    )
    create(records, OUTBOX)
    if "entity_type" not in spark.table(OUTBOX).columns:
        spark.sql(f"ALTER TABLE {OUTBOX} ADD COLUMN entity_type string")
    records.createOrReplaceTempView("new_outbox")
    frame.sparkSession.sql(
        f"MERGE INTO {OUTBOX} t USING new_outbox s ON t.topic=s.topic AND t.country_code=s.country_code AND t.message_id=s.message_id WHEN NOT MATCHED THEN INSERT *"
    )


def flush(spark):
    if not spark.catalog.tableExists(OUTBOX):
        return
    spark.catalog.refreshTable(OUTBOX)
    pending = spark.table(OUTBOX).filter("published=false")
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        acks="all",
        retries=5,
        key_serializer=lambda v: v.encode(),
        value_serializer=lambda v: v.encode(),
    )
    sent = []
    try:
        for row in pending.toLocalIterator():
            producer.send(row.topic, key=row.message_id, value=row.payload).get(
                timeout=30
            )
            sent.append((row.country_code, row.topic, row.message_id))
        producer.flush(timeout=30)
        if sent:
            complete = spark.createDataFrame(
                sent, "country_code string,topic string,message_id string"
            )
            complete.createOrReplaceTempView("sent_outbox")
            spark.sql(
                f"MERGE INTO {OUTBOX} t USING sent_outbox s ON t.country_code=s.country_code AND t.topic=s.topic AND t.message_id=s.message_id WHEN MATCHED THEN UPDATE SET t.published=true"
            )
    finally:
        producer.close()
    print(
        json.dumps({"event": "outbox_flushed", "acknowledged_messages": len(sent)}),
        flush=True,
    )
