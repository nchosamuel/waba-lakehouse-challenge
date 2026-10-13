"""Independent SQL checks of the positive/negative NiFi/Kafka/Spark fixtures."""

import csv, io, json, subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT.parent.parent / "work"
fixture = json.loads(
    subprocess.check_output(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "app",
            "python",
            "-c",
            "from waba.publisher import client; print(client().get_object(Bucket='archive',Key='reports/streaming/latest-fixture.json')['Body'].read().decode())",
        ],
        cwd=ROOT,
        text=True,
    )
)
ids = fixture["event_ids"]


def query(sql):
    output = subprocess.check_output(
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
    return list(csv.reader(io.StringIO(output)))


def count(sql):
    return int(query(sql)[0][0])


all_ids = ",".join("'" + value + "'" for value in ids.values())
actual = count(
    f"SELECT count(*) FROM iceberg.gold.streaming_event_ledger WHERE event_id IN ({all_ids})"
)
unique = count(
    f"SELECT count(DISTINCT ROW(country_code,event_id)) FROM iceberg.gold.streaming_event_ledger WHERE event_id IN ({all_ids})"
)
assert actual == unique == 11, (actual, unique)
assert (
    count(
        f"SELECT count(*) FROM iceberg.silver.bank_transactions WHERE transaction_id IN ('{ids['invalid_amount']}','{ids['invalid_account']}')"
    )
    == 0
)
assert (
    count(
        "SELECT count(*) FROM iceberg.silver.streaming_outbox WHERE topic='dlq-financial-events' AND published=true"
    )
    >= 2
)
rules = {
    row[0]: int(row[1])
    for row in query("SELECT rule,count(*) FROM iceberg.gold.fraud_alerts GROUP BY 1")
}
for rule in (
    "MULTIPLE_HIGH_VALUE_TRANSFERS",
    "UNUSUAL_COUNTRY_FOR_PROFILE",
    "CLAIM_OVER_THREE_ANNUAL_PREMIUMS",
):
    assert rules.get(rule, 0) > 0, rule
for key in ("aml_xof", "aml_ghs"):
    assert (
        count(
            f"SELECT count(*) FROM iceberg.gold.aml_events WHERE subject_id='{ids[key]}'"
        )
        == 1
    ), key
assert (
    count(
        f"SELECT count(*) FROM iceberg.gold.aml_events WHERE subject_id='{ids['below_aml']}'"
    )
    == 0
)
assert (
    count(
        "SELECT count(*) FROM iceberg.gold.fraud_alerts WHERE rule='UNUSUAL_COUNTRY_FOR_PROFILE' AND country_code='CI'"
    )
    == 0
)
assert (
    count(
        f"SELECT count(*) FROM iceberg.gold.fraud_alerts a JOIN iceberg.silver.insurance_operations s ON a.country_code=s.country_code AND a.window_start=s.timestamp AND starts_with(a.subject_id,s.account_id||'/') WHERE a.rule='CLAIM_OVER_THREE_ANNUAL_PREMIUMS' AND s.operation_id='{ids['claim_small']}'"
    )
    == 0
)
assert count("SELECT count(*) FROM iceberg.gold.liquidity_alerts") >= 1
assert (
    count("SELECT count(*) FROM iceberg.silver.streaming_outbox WHERE published=false")
    == 0
)
# Lambda sum equals the batch snapshot plus fresh successful events exactly once.
batch = float(
    query("SELECT sum(total_amount_eur) FROM iceberg.gold.daily_transaction_volume")[0][
        0
    ]
)
fresh = float(
    query(
        """SELECT sum(s.amount_eur) FROM iceberg.silver.bank_transactions s LEFT JOIN iceberg.gold.batch_transaction_membership m ON s.country_code=m.country_code AND s.transaction_id=m.transaction_id WHERE s.transaction_status='SUCCESS' AND m.transaction_id IS NULL"""
    )[0][0]
)
lambda_total = float(
    query(
        "SELECT sum(total_amount_eur) FROM iceberg.gold.lambda_daily_transaction_volume"
    )[0][0]
)
assert abs(lambda_total - batch - fresh) < 0.01, (lambda_total, batch, fresh)
raw = subprocess.check_output(
    [
        "docker",
        "compose",
        "-f",
        "compose.yaml",
        "-f",
        "compose.streaming.yaml",
        "--profile",
        "streaming",
        "exec",
        "-T",
        "streaming",
        "python3",
        "/app/jobs/inspect_stream_kafka.py",
    ],
    cwd=ROOT,
    text=True,
)
kafka = json.loads(raw)
for topic in (
    "raw-bank-transactions",
    "raw-insurance-operations",
    "raw-mobile-money-payments",
    "raw-loan-repayments",
    "silver-bank-transactions",
    "silver-insurance-operations",
    "silver-mobile-money",
    "gold-fraud-alerts",
    "gold-aml-events",
    "gold-liquidity-alerts",
    "dlq-financial-events",
):
    assert kafka["topic_counts"].get(topic, 0) > 0, topic
assert (
    kafka["file_to_kafka_latency_max_seconds"] is not None
    and kafka["file_to_kafka_latency_max_seconds"] < 30
), kafka["file_to_kafka_latency_max_seconds"]
result = {
    "streaming_end_to_end_verified": True,
    "fixture_batch": fixture["batch_id"],
    "accepted_events": actual,
    "rejected_fixture_events": 2,
    "fraud_rule_counts": rules,
    "aml_xof_and_ghs_verified": True,
    "negative_cases_verified": True,
    "liquidity_alert_verified": True,
    "lambda_no_overlap_verified": True,
    "lambda_total_eur": lambda_total,
    "kafka": kafka,
}
(ROOT / "docs" / "streaming-verification.json").write_text(
    json.dumps(result, indent=2) + "\n"
)
print(json.dumps(result))
