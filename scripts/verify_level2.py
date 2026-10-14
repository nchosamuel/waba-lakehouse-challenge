"""Independently verify materialized tables and accounting outcomes through Trino."""

import csv
import io
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


from verification_runtime import query, report_path


checks = {}
raw_keys = {
    "customers": "customer_id",
    "accounts": "account_id",
    "branches": "branch_id",
    "products": "product_id",
    "bank_transactions": "transaction_id",
    "insurance_operations": "operation_id",
    "mobile_money_payments": "payment_id",
    "loan_repayments": "repayment_id",
}
for name, key in raw_keys.items():
    values = {}
    for zone in ("raw", "bronze", "silver"):
        count, unique, countries = map(
            int,
            query(
                f"SELECT count(*),count(DISTINCT ROW(country_code,{key})),count(DISTINCT country_code) FROM iceberg.{zone}.{name}"
            )[0],
        )
        assert count == unique and countries == 8, (
            zone,
            name,
            count,
            unique,
            countries,
        )
        values[zone] = count
    assert len(set(values.values())) == 1, (name, values)
    locations = [
        query(f'SELECT file_path FROM iceberg.{z}."{name}$files" LIMIT 1')[0][0]
        for z in ("raw", "bronze", "silver")
    ]
    assert len(set(locations)) == 3, locations
    checks[name] = values
npl = query(
    """SELECT country_code,sum(default_outstanding_eur)/sum(total_outstanding_eur)
             FROM iceberg.gold.npl_ratio_by_country WHERE snapshot_date=DATE '2026-02-28' GROUP BY 1 ORDER BY 1"""
)
expected = dict(
    zip(("CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"), (3, 4, 5, 6, 7, 8, 4, 5))
)
assert len(npl) == 8
for country, value in npl:
    assert abs(float(value) - expected[country] / 100) < 0.000001, (country, value)
loss = query(
    "SELECT country_code,min(loss_ratio),max(loss_ratio) FROM iceberg.gold.loss_ratio_by_product WHERE month=DATE '2026-02-01' GROUP BY 1 ORDER BY 1"
)
expected_loss = dict(
    zip(
        ("CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"),
        (0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85),
    )
)
assert len(loss) == 8
for country, low, high in loss:
    assert (
        abs(float(low) - expected_loss[country]) < 0.00001
        and abs(float(high) - expected_loss[country]) < 0.00001
    ), (country, low, high)
assert [float(value) for value in query(
    "SELECT min(avg_business_processing_days),max(avg_business_processing_days) FROM iceberg.gold.claims_processing_time"
)[0]] == [10.0, 10.0]
assert (
    int(query("SELECT count(*) FROM iceberg.gold.cross_border_transfers")[0][0]) >= 120
)
assert (
    int(
        query(
            "SELECT count(*) FROM iceberg.silver.mobile_money_payments WHERE sender_country <> receiver_country AND receiver_segment IS NULL"
        )[0][0]
    )
    == 0
)
assert (
    int(
        query(
            "SELECT count(*) FROM iceberg.silver.bank_transactions WHERE abs(amount_eur * local_per_eur - amount) > 0.001"
        )[0][0]
    )
    == 0
)
# Independent success-only total, not a comparison against the transformation expression.
bank_total = float(
    query(
        "SELECT sum(amount_eur) FROM iceberg.silver.bank_transactions WHERE transaction_status='SUCCESS'"
    )[0][0]
)
gold_total = float(
    query("SELECT sum(total_amount_eur) FROM iceberg.gold.daily_transaction_volume")[0][
        0
    ]
)
assert abs(bank_total - gold_total) < 0.001
for table in (
    "daily_transaction_volume",
    "customer_arpu_monthly",
    "mobile_money_daily_flow",
    "regulatory_bceao_daily",
    "regulatory_cima_daily",
):
    assert int(query(f"SELECT count(*) FROM iceberg.gold.{table}")[0][0]) > 0, table
assert (
    int(
        query(
            "SELECT count(*) FROM iceberg.gold.regulatory_bceao_daily WHERE snapshot_date > report_date"
        )[0][0]
    )
    == 0
)
result = {
    "medallion_verified": True,
    "row_counts": checks,
    "npl": npl,
    "loss_ratio_by_country": loss,
    "business_processing_days": 10,
    "no_future_snapshot_in_reporting": True,
    "bank_success_amount_matches_gold": True,
}
report_path("level2-verification.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
