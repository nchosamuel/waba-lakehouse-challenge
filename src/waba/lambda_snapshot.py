"""Atomic batch facts: Lambda totals and event coverage share one Iceberg commit."""

TABLE = "iceberg.gold.lambda_batch_transactions"
COLUMNS = (
    "country_code", "entity_type", "transaction_id", "txn_date",
    "transaction_type", "transaction_status", "amount_eur",
)


def publish(bank, table=TABLE):
    from waba.spark_common import replace_countries
    replace_countries(bank.select(*COLUMNS), table, "txn_date")


def lambda_sql(batch_table=TABLE, stream_table="kafka.default.silver_bank_transactions"):
    # One batch-table scan: no independently committed aggregate/coverage tables.
    return f"""WITH unique_stream AS (
 SELECT *, row_number() OVER (PARTITION BY country_code,event_id ORDER BY _partition_id,_partition_offset) AS rn
 FROM {stream_table}
), facts AS (
 SELECT COALESCE(b.country_code,s.country_code) country_code,
 COALESCE(b.entity_type,s.entity_type) entity_type,
 COALESCE(b.txn_date,CAST(from_iso8601_timestamp(s.event_time) AS DATE)) txn_date,
 COALESCE(b.transaction_type,s.transaction_type) transaction_type,
 CASE WHEN b.transaction_id IS NOT NULL THEN b.transaction_status ELSE s.transaction_status END transaction_status,
 CASE WHEN b.transaction_id IS NOT NULL THEN CAST(b.amount_eur AS DOUBLE) ELSE s.amount_eur END amount_eur
 FROM {batch_table} b FULL OUTER JOIN (SELECT * FROM unique_stream WHERE rn=1) s
 ON b.country_code=s.country_code AND b.transaction_id=s.event_id
)
SELECT country_code,entity_type,txn_date,transaction_type,count(*) transaction_count,
 sum(amount_eur) total_amount_eur FROM facts WHERE transaction_status='SUCCESS' GROUP BY 1,2,3,4"""
