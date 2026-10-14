"""Gold batch totals plus unique Kafka events not covered by the batch snapshot."""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sql = """CREATE OR REPLACE VIEW iceberg.gold.lambda_daily_transaction_volume AS
WITH unique_stream AS (
 SELECT *, row_number() OVER (PARTITION BY country_code,event_id ORDER BY _partition_id,_partition_offset) AS rn
 FROM kafka.default.silver_bank_transactions WHERE transaction_status='SUCCESS'
), fresh AS (
 SELECT s.country_code,s.entity_type,CAST(from_iso8601_timestamp(s.event_time) AS DATE) AS txn_date,
 s.transaction_type,count(*) AS transaction_count,sum(s.amount_eur) AS total_amount_eur
 FROM unique_stream s LEFT JOIN iceberg.gold.batch_transaction_membership m
 ON m.country_code=s.country_code AND m.transaction_id=s.event_id
 WHERE s.rn=1 AND m.transaction_id IS NULL GROUP BY 1,2,3,4
), combined AS (
 SELECT country_code,entity_type,txn_date,transaction_type,transaction_count,CAST(total_amount_eur AS DOUBLE) AS total_amount_eur FROM iceberg.gold.daily_transaction_volume
 UNION ALL SELECT country_code,entity_type,txn_date,transaction_type,transaction_count,total_amount_eur FROM fresh
)
SELECT country_code,entity_type,txn_date,transaction_type,sum(transaction_count) AS transaction_count,sum(total_amount_eur) AS total_amount_eur
FROM combined GROUP BY 1,2,3,4"""
from verification_runtime import query

query(sql)
print("Lambda view created.")
