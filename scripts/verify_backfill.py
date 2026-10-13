"""Country/month replay must preserve business values and all other countries."""

import csv
import io
import json
import subprocess
from pathlib import Path
from run_pipeline import run

ROOT = Path(__file__).resolve().parents[1]
TABLES = [
    "daily_transaction_volume",
    "npl_ratio_by_country",
    "customer_arpu_monthly",
    "loss_ratio_by_product",
    "claims_processing_time",
    "mobile_money_daily_flow",
    "cross_border_transfers",
]


def query(sql):
    data = subprocess.check_output(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "trino",
            "trino",
            "--output-format",
            "CSV",
            "--execute",
            sql,
        ],
        cwd=ROOT,
        text=True,
        stderr=subprocess.DEVNULL,
    )
    return list(csv.reader(io.StringIO(data)))


def fingerprint():
    result = {}
    for table in TABLES:
        columns = [row[0] for row in query("SHOW COLUMNS FROM iceberg.gold." + table)]
        fields = ",".join('"' + field + '"' for field in columns)
        result[table] = query(
            f"SELECT country_code,count(*),to_hex(checksum(ROW({fields}))) FROM iceberg.gold.{table} GROUP BY 1 ORDER BY 1"
        )
    return result


before = fingerprint()
# Replay original immutable CI events; preserve the source and the original IDs.
code = """from waba.publisher import client
s3=client()
copied=0
for page in s3.get_paginator('list_objects_v2').paginate(Bucket='archive',Prefix='CI/'):
    for obj in page.get('Contents',[]):
        key=obj['Key']
        if key.endswith('scenario_v1_CI_2026-02-01.csv'):
            s3.copy_object(Bucket='raw-landing',Key=key,CopySource={'Bucket':'archive','Key':key})
            copied+=1
assert copied>=3
print('Archived CI scenario files copied for replay')
"""
subprocess.run(
    ["docker", "compose", "exec", "-T", "app", "python", "-c", code],
    cwd=ROOT,
    check=True,
)
for name in ("ingest_raw", "raw_to_bronze", "bronze_to_silver", "silver_to_gold"):
    run(
        name,
        {"country_codes": ["CI"], "date_from": "2026-02-01", "date_to": "2026-02-28"},
    )
after = fingerprint()
assert before == after, {"before": before, "after": after}
# Partial J+1 report cannot recognize a full month's premium or future paid claims.
run("regulatory_report", {"country_codes": ["CI"], "report_date": "2026-02-10"})
partial = query(
    "SELECT sum(earned_premiums_eur),sum(paid_claims_eur) FROM iceberg.gold.regulatory_cima_daily WHERE country_code='CI' AND report_date=DATE '2026-02-10'"
)[0]
assert (
    abs(float(partial[0]) - 10000 * 10 / 28) < 0.01 and float(partial[1]) == 0
), partial
assert (
    int(
        query(
            "SELECT count(*) FROM iceberg.gold.regulatory_bceao_daily WHERE country_code='CI' AND report_date=DATE '2026-02-10'"
        )[0][0]
    )
    == 0
)
result = {
    "country_month_replay_verified": True,
    "scope": {
        "country_codes": ["CI"],
        "date_from": "2026-02-01",
        "date_to": "2026-02-28",
    },
    "all_seven_gold_tables_unchanged": True,
    "other_countries_preserved": True,
    "midmonth_earned_premiums_eur": float(partial[0]),
    "midmonth_paid_claims_eur": float(partial[1]),
    "future_loan_snapshot_excluded": True,
}
(ROOT / "docs" / "backfill-verification.json").write_text(
    json.dumps(result, indent=2) + "\n"
)
print(json.dumps(result))
