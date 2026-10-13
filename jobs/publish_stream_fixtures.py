"""Positive and negative business fixtures for the real NiFi/Kafka pipeline."""

import copy, json, tempfile, uuid
from datetime import datetime, timezone, timedelta
from waba.generator import generate, event_uuid
from waba.publisher import client, csv_bytes

batch = uuid.uuid4().hex
now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
with tempfile.TemporaryDirectory() as folder:
    ci = generate(folder, "CI", 1)
    gh = generate(folder, "GH", 1)
    sn = generate(folder, "SN", 1)
rows = {
    name: []
    for name in (
        "bank_transactions",
        "insurance_operations",
        "mobile_money_payments",
        "loan_repayments",
    )
}


def append(name, base, **values):
    row = copy.deepcopy(base)
    key = {
        "bank_transactions": "transaction_id",
        "insurance_operations": "operation_id",
        "mobile_money_payments": "payment_id",
        "loan_repayments": "repayment_id",
    }[name]
    row[key] = str(event_uuid(batch + "/" + name + "/" + str(len(rows[name]))))
    row["timestamp"] = (now + timedelta(seconds=len(rows[name]))).isoformat()
    row.update(values)
    rows[name].append(row)
    return row[key]


ids = {}
for i in range(2):
    ids["large_" + str(i)] = append(
        "bank_transactions",
        ci["bank_transactions"][0],
        account_id="WABA-CI-A-0000000",
        customer_id="WABA-CI-C-000000",
        branch_id="WABA-CI-B-000",
        beneficiary_account="WABA-CI-A-0000001",
        amount=600000,
        transaction_type="TRANSFER",
        transaction_status="SUCCESS",
        fee_amount=600,
    )
ids["aml_xof"] = append(
    "bank_transactions",
    ci["bank_transactions"][0],
    amount=1100000,
    transaction_type="TRANSFER",
    transaction_status="SUCCESS",
)
ids["aml_ghs"] = append(
    "bank_transactions",
    gh["bank_transactions"][0],
    amount=6000,
    transaction_type="TRANSFER",
    transaction_status="SUCCESS",
)
ids["below_aml"] = append(
    "bank_transactions",
    gh["bank_transactions"][0],
    amount=5000,
    transaction_type="TRANSFER",
    transaction_status="SUCCESS",
)
ids["liquidity"] = append(
    "bank_transactions",
    ci["bank_transactions"][0],
    amount=12000000000,
    transaction_type="WITHDRAWAL",
    transaction_status="SUCCESS",
)
ids["invalid_amount"] = append(
    "bank_transactions", ci["bank_transactions"][0], amount=-10
)
ids["invalid_account"] = append(
    "bank_transactions", ci["bank_transactions"][0], account_id="NONEXISTENT"
)
ids["unusual_country"] = append(
    "mobile_money_payments",
    sn["mobile_money_payments"][0],
    sender_id="WABA-CI-MC-000000",
    sender_country="SN",
    country_code="SN",
    status="SUCCESS",
)
ids["ordinary_country"] = append(
    "mobile_money_payments",
    ci["mobile_money_payments"][0],
    sender_id="WABA-CI-MC-000000",
    sender_country="CI",
    country_code="CI",
    status="SUCCESS",
)
for amount, label in ((3000000, "claim_large"), (1000, "claim_small")):
    ids[label] = append(
        "insurance_operations",
        ci["insurance_operations"][0],
        customer_id="WABA-CI-IC-000000",
        account_id="WABA-CI-IA-0000000",
        operation_type="CLAIM_PAYMENT",
        product_line="VIE",
        amount=amount,
        claim_status="PAID",
        processing_days=10,
    )
loan = ci["loan_repayments"][0]
ids["loan"] = append(
    "loan_repayments",
    loan,
    repayment_status="ON_TIME",
    amount_paid=loan["amount_due"],
    days_overdue=0,
    due_date=now.date().isoformat(),
    payment_date=now.date().isoformat(),
)
s3 = client()
files = []
for name, events in rows.items():
    for country in {r["country_code"] for r in events}:
        selected = [r for r in events if r["country_code"] == country]
        key = f"{country}/{name}/{now.date()}/stream_fixture_{batch}.csv"
        s3.put_object(Bucket="raw-landing", Key=key, Body=csv_bytes(selected))
        files.append(key)
report = {
    "batch_id": batch,
    "published_at_utc": now.isoformat(),
    "files": files,
    "event_ids": ids,
    "input_events": sum(map(len, rows.values())),
}
s3.put_object(
    Bucket="archive",
    Key="reports/streaming/latest-fixture.json",
    Body=json.dumps(report).encode(),
)
print(json.dumps(report))
