"""Capture the event keys covered by batch Gold for overlap-free Lambda queries.

Run once before live streaming, with batch and stream writers stopped. Later
Gold batch recomputations update the membership from the same Silver input.
"""

from pyspark.sql import functions as F
from waba.spark_common import session, replace_countries

spark = session("waba-lambda-baseline")
bank = spark.table("iceberg.silver.bank_transactions")
# Stream rows must not be stamped as batch-covered until Gold recomputes them.
if "stream_event_id" in bank.columns:
    bank = bank.filter("stream_event_id IS NULL")
expected = (
    bank.filter("transaction_status='SUCCESS'").agg(F.sum("amount_eur")).first()[0]
)
actual = (
    spark.table("iceberg.gold.daily_transaction_volume")
    .agg(F.sum("total_amount_eur"))
    .first()[0]
)
if abs(expected - actual) > 0.001:
    raise ValueError("Batch Gold not aligned with baseline Silver")
keys = bank.select("country_code", "entity_type", "transaction_id", "txn_date")
replace_countries(keys, "iceberg.gold.batch_transaction_membership", "txn_date")
spark.stop()
