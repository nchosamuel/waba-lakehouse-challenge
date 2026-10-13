"""Synthetic accounting inputs and coherent events for the Level 2 demonstration.

These are declared simulation assumptions, not inferred customer facts or live FX.
"""

import calendar
from collections import defaultdict
import json
from datetime import date, timedelta
from waba.generator import COUNTRIES, event_uuid
from waba.reference_scale import reference_row, count_for

NPL_TARGET = dict(zip(COUNTRIES, (3, 4, 5, 6, 7, 8, 4, 5)))
LOSS_TARGET = dict(zip(COUNTRIES, (0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85)))
LINES = ("VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE", "PREVOYANCE")


def accounts(country, entity, size=100):
    result = []
    for index in range(count_for("accounts", country)):
        row = reference_row("accounts", country, index)
        if row["entity_type"] == entity:
            result.append(row)
            if len(result) == size:
                return result
    raise ValueError("Full references are required")


def scenario(month="2026-02-01"):
    first = date.fromisoformat(month)
    if first.day != 1:
        raise ValueError("Month must start on day 1")
    last = first.replace(day=calendar.monthrange(first.year, first.month)[1])
    positions, accounting, flows = (
        [],
        [],
        {
            name: []
            for name in (
                "insurance_operations",
                "loan_repayments",
                "mobile_money_payments",
            )
        },
    )
    for ci, country in enumerate(COUNTRIES):
        currency = "GHS" if country == "GH" else "XOF"
        # Same EUR exposure per loan, explicit outstanding snapshot, not instalments.
        fx = 12.5 if currency == "GHS" else 655.957
        for i, loan in enumerate(accounts(country, "MICROFINANCE")):
            default = i < NPL_TARGET[country]
            loan_type = "MICROCREDIT" if i % 2 == 0 else "AGRICULTURAL"
            positions.append(
                dict(
                    country_code=country,
                    entity_type="MICROFINANCE",
                    snapshot_date=last.isoformat(),
                    loan_account_id=loan["account_id"],
                    customer_id=loan["customer_id"],
                    loan_type=loan_type,
                    outstanding_amount=round(1000 * fx, 3),
                    currency=currency,
                    days_past_due=100 if default else 0,
                    default_flag=default,
                    model_version="scenario-v1",
                )
            )
            rid = str(event_uuid(f"scenario-v1/{month}/{country}/repayment/{i}"))
            due = last - timedelta(days=100) if default else first + timedelta(days=9)
            amount = round(100 * fx, 3)
            flows["loan_repayments"].append(
                dict(
                    repayment_id=rid,
                    timestamp=f"{last if default else due}T12:00:00",
                    loan_account_id=loan["account_id"],
                    customer_id=loan["customer_id"],
                    country_code=country,
                    amount_due=amount,
                    amount_paid=0 if default else amount,
                    currency=currency,
                    due_date=due.isoformat(),
                    payment_date="" if default else due.isoformat(),
                    days_overdue=100 if default else 0,
                    loan_type=loan_type,
                    repayment_status="DEFAULT" if default else "ON_TIME",
                    entity_type="MICROFINANCE",
                )
            )
            accounting.append(
                dict(
                    event_id=rid,
                    country_code=country,
                    entity_type="MICROFINANCE",
                    event_kind="loan_repayments",
                    earned_premium=0.0,
                    interest_received=0.0 if default else round(amount * 0.10, 3),
                    coverage_start=None,
                    coverage_end=None,
                    claim_id=None,
                    claim_submitted_date=None,
                    claim_paid_date=None,
                    product_id="MICROFINANCE-STANDARD",
                    model_version="scenario-v1",
                )
            )
        for i, policy in enumerate(accounts(country, "INSURANCE")):
            line = LINES[i % len(LINES)]
            premium = round(100 * fx, 3)
            for kind in ("PREMIUM_PAYMENT", "CLAIM_SUBMISSION", "CLAIM_PAYMENT"):
                oid = str(
                    event_uuid(f"scenario-v1/{month}/{country}/policy/{i}/{kind}")
                )
                claim = kind.startswith("CLAIM")
                # One paid claim per policy; common 10-business-day lifecycle.
                submitted = first + timedelta(days=2)
                paid = submitted
                business_days = 0
                while business_days < 10:
                    paid += timedelta(days=1)
                    business_days += paid.weekday() < 5
                day = (
                    submitted
                    if kind == "CLAIM_SUBMISSION"
                    else paid if claim else first
                )
                claim_amount = round(premium * LOSS_TARGET[country], 3)
                flows["insurance_operations"].append(
                    dict(
                        operation_id=oid,
                        timestamp=f"{day}T12:00:00",
                        customer_id=policy["customer_id"],
                        account_id=policy["account_id"],
                        country_code=country,
                        operation_type=kind,
                        product_line=line,
                        amount=claim_amount if claim else premium,
                        currency=currency,
                        claim_status=(
                            ("PAID" if kind == "CLAIM_PAYMENT" else "PENDING")
                            if claim
                            else ""
                        ),
                        processing_days=(
                            10 if kind == "CLAIM_PAYMENT" else 0 if claim else ""
                        ),
                        entity_type="INSURANCE",
                    )
                )
                accounting.append(
                    dict(
                        event_id=oid,
                        country_code=country,
                        entity_type="INSURANCE",
                        event_kind="insurance_operations",
                        earned_premium=premium if not claim else 0.0,
                        interest_received=0.0,
                        coverage_start=first.isoformat(),
                        coverage_end=last.isoformat(),
                        claim_id=f"{country}/{month}/{i}" if claim else None,
                        claim_submitted_date=submitted.isoformat() if claim else None,
                        claim_paid_date=(
                            paid.isoformat() if kind == "CLAIM_PAYMENT" else None
                        ),
                        product_id="INSURANCE-STANDARD",
                        model_version="scenario-v1",
                    )
                )
        # Actual UEMOA corridors; the challenge's GN/XOF currency convention is retained elsewhere.
        if country not in ("GH", "GN"):
            peers = [cc for cc in COUNTRIES if cc not in (country, "GH", "GN")]
            for i, receiver_country in enumerate(peers):
                sender = accounts(country, "MOBILE_MONEY", 1)[0]
                receiver = accounts(receiver_country, "MOBILE_MONEY", 1)[0]
                for week in range(4):
                    day = first + timedelta(days=week * 7)
                    flows["mobile_money_payments"].append(
                        dict(
                            payment_id=str(
                                event_uuid(
                                    f"scenario-v1/{month}/{country}/{receiver_country}/{week}"
                                )
                            ),
                            timestamp=f"{day}T14:00:00",
                            sender_id=sender["customer_id"],
                            receiver_id=receiver["customer_id"],
                            sender_country=country,
                            receiver_country=receiver_country,
                            country_code=country,
                            amount=10000 + week * 1000,
                            currency="XOF",
                            payment_type="CROSS_BORDER_TRANSFER",
                            operator="WABA_PAY",
                            status="SUCCESS",
                            fee_amount=100,
                            entity_type="MOBILE_MONEY",
                        )
                    )
    return {
        "loan_positions": positions,
        "accounting_context": accounting,
        "flows": flows,
        "month": month,
        "npl_targets": NPL_TARGET,
        "loss_targets": LOSS_TARGET,
    }


def publish_scenario(month="2026-02-01"):
    from waba.publisher import client, csv_bytes

    s3 = client()
    for country in COUNTRIES:
        for name in ("customers", "accounts", "branches", "products"):
            head = s3.head_object(
                Bucket="raw-landing", Key=f"{country}/references/{name}.csv"
            )
            if head.get("Metadata", {}).get("profile") != "full-v1":
                raise ValueError(
                    "Generate full references before publishing the business scenario"
                )
    data = scenario(month)
    for country in COUNTRIES:
        for name in ("loan_positions", "accounting_context"):
            records = [r for r in data[name] if r["country_code"] == country]
            s3.put_object(
                Bucket="raw-landing",
                Key=f"{country}/business_context/{month}/{name}.json",
                Body=json.dumps(records).encode(),
            )
        for name, rows in data["flows"].items():
            records = [r for r in rows if r["country_code"] == country]
            by_day = defaultdict(list)
            for record in records:
                by_day[record["timestamp"][:10]].append(record)
            for day, daily in by_day.items():
                # Stable IDs and day-partitioned keys make selective replay safe.
                s3.put_object(
                    Bucket="raw-landing",
                    Key=f"{country}/{name}/{day}/scenario_v1_{country}_{month}.csv",
                    Body=csv_bytes(daily),
                )
    result = {
        "month": month,
        "events": {k: len(v) for k, v in data["flows"].items()},
        "loan_positions": len(data["loan_positions"]),
        "accounting_context": len(data["accounting_context"]),
    }
    s3.put_object(
        Bucket="archive",
        Key=f"reports/scenarios/{month}.json",
        Body=json.dumps(result).encode(),
    )
    return result
