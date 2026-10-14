"""Publish references first and immutable per-day banking batches to MinIO."""

import csv
import io
import json
import os
import secrets
import tempfile
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import boto3
from botocore.exceptions import ClientError
from waba.generator import generate
from waba.contracts import FLOW_PREFIX, REFERENCES, KEYS


def client():
    return boto3.client(
        "s3", endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000")
    )


def csv_bytes(records):
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=list(records[0]))
    writer.writeheader()
    writer.writerows(records)
    return stream.getvalue().encode("utf-8")


def publish(
    countries,
    rows,
    start,
    end,
    batch_id=None,
    seed=None,
    dataset="bank_transactions",
    entity_type=None,
    realtime=False,
):
    if dataset not in (*FLOW_PREFIX, "references"):
        raise ValueError("Unknown dataset")
    allowed = {
        "bank_transactions": ("BANK", "MICROFINANCE"),
        "insurance_operations": ("INSURANCE",),
        "mobile_money_payments": ("MOBILE_MONEY",),
        "loan_repayments": ("MICROFINANCE",),
        "references": ("ALL",),
    }
    entity_type = entity_type or allowed[dataset][0]
    if entity_type not in allowed[dataset]:
        raise ValueError("Entity is incompatible with selected dataset")
    if not countries:
        raise ValueError("Select at least one country")
    batch_id = batch_id or uuid.uuid4().hex
    seed = seed if seed is not None else secrets.randbits(32)
    s3 = client()
    keys = []
    reference_rows = 0
    with tempfile.TemporaryDirectory() as folder:
        for country in countries:
            full_profile = True
            for ref_name in REFERENCES:
                try:
                    meta = s3.head_object(
                        Bucket="raw-landing", Key=f"{country}/references/{ref_name}.csv"
                    ).get("Metadata", {})
                    full_profile = full_profile and meta.get("profile") == "full-v1"
                except ClientError as exc:
                    if exc.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                        raise
                    full_profile = False
            data = generate(
                folder,
                country,
                rows if dataset != "references" else 1,
                seed,
                start,
                end,
                batch_id,
                datasets_to_generate=[dataset],
                reference_profile="full" if full_profile else "demo",
                bank_entity=entity_type if dataset == "bank_transactions" else "BANK",
            )
            if realtime and dataset != "references":
                current = datetime.now(timezone.utc).replace(tzinfo=None)
                for event in data[dataset]:
                    event["timestamp"] = current.isoformat()
                    if dataset == "loan_repayments":
                        event["due_date"] = (
                            current.date() - timedelta(days=event["days_overdue"])
                        ).isoformat()
                        event["payment_date"] = (
                            ""
                            if event["repayment_status"] == "DEFAULT"
                            else current.date().isoformat()
                        )
            for name in REFERENCES:
                key = f"{country}/references/{name}.csv"
                try:
                    meta = s3.head_object(Bucket="raw-landing", Key=key).get(
                        "Metadata", {}
                    )
                    if meta.get("profile") == "full-v1":
                        reference_rows += int(meta["rows"])
                        continue  # Full references contain and preserve every demo row.
                except ClientError as exc:
                    if exc.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                        raise
                reference_rows += len(data[name])
                payload = csv_bytes(data[name])
                try:
                    existing = s3.get_object(Bucket="raw-landing", Key=key)[
                        "Body"
                    ].read()
                    incoming = {
                        r[KEYS[name]]: r
                        for r in csv.DictReader(io.StringIO(payload.decode()))
                    }
                    for row in csv.DictReader(io.StringIO(existing.decode())):
                        if incoming.get(row[KEYS[name]]) != row:
                            raise ValueError(f"Reference conflict: {key}")
                    if existing != payload:
                        s3.put_object(Bucket="raw-landing", Key=key, Body=payload)
                except ClientError as exc:
                    if exc.response["Error"]["Code"] != "NoSuchKey":
                        raise
                    s3.put_object(Bucket="raw-landing", Key=key, Body=payload)
            if dataset == "references":
                keys.extend(f"{country}/references/{name}.csv" for name in REFERENCES)
                continue
            by_day = defaultdict(list)
            for row in data[dataset]:
                by_day[row["timestamp"][:10]].append(row)
            for day, records in by_day.items():
                key = f'{country}/{dataset}/{day}/{FLOW_PREFIX[dataset]}_{country}_{day.replace("-", "")}_{batch_id}.csv'
                s3.put_object(Bucket="raw-landing", Key=key, Body=csv_bytes(records))
                keys.append(key)
    return {
        "batch_id": batch_id,
        "rows": (reference_rows if dataset == "references" else rows * len(countries)),
        "files": keys,
    }


if __name__ == "__main__":
    print(
        json.dumps(
            publish(
                ["CI"],
                1000,
                datetime(2026, 1, 1),
                datetime(2026, 1, 1),
                batch_id="integration-v2",
                seed=42,
            )
        )
    )
