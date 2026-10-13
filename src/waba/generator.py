"""Deterministic synthetic banking data. No real personal data is used."""

import argparse
import csv
import hashlib
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

COUNTRIES = ("CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH")


def event_uuid(value):
    """Reproducible UUIDv4 bit layout; random batch IDs provide lot uniqueness."""
    return uuid.UUID(bytes=hashlib.sha256(value.encode()).digest()[:16], version=4)


def generate(
    destination,
    country="CI",
    rows=1000,
    seed=42,
    start=None,
    end=None,
    batch_id="demo",
    datasets_to_generate=None,
    reference_profile="demo",
    bank_entity="BANK",
):
    if bank_entity not in ("BANK", "MICROFINANCE"):
        raise ValueError("Unsupported banking entity")
    if country not in COUNTRIES or rows < 1:
        raise ValueError("Valid country and positive row count required")
    start = start or datetime(2026, 1, 1)
    end = end or datetime(2026, 1, 1)
    if end < start:
        raise ValueError("End date must follow start date")
    seconds = int((end + timedelta(days=1) - start).total_seconds())
    requested = set(
        datasets_to_generate
        or (
            "bank_transactions",
            "insurance_operations",
            "mobile_money_payments",
            "loan_repayments",
        )
    )
    rng = random.Random(seed)
    folder = Path(destination) / country
    folder.mkdir(parents=True, exist_ok=True)
    currency = "GHS" if country == "GH" else "XOF"
    customers = [
        dict(
            customer_id=f"WABA-{country}-C-{i:06d}",
            country_code=country,
            entity_type="BANK",
            segment="RETAIL",
            kyc_level="STANDARD",
            onboarding_date="2025-01-01",
            region=country,
            is_active=True,
        )
        for i in range(100)
    ]
    accounts = [
        dict(
            account_id=f"WABA-{country}-A-{i:07d}",
            customer_id=customers[i % 100]["customer_id"],
            country_code=country,
            entity_type="BANK",
            account_type="CURRENT",
            currency=currency,
            balance=100000,
            credit_limit=0,
            opened_date="2025-01-01",
            status="ACTIVE",
        )
        for i in range(200)
    ]
    branches = [
        dict(
            branch_id=f"WABA-{country}-B-{i:03d}",
            country_code=country,
            entity_type="BANK",
            city=country,
            region=country,
            branch_type="FULL_SERVICE",
            is_active=True,
        )
        for i in range(5)
    ]
    products = [
        dict(
            product_id="BANK-CURRENT",
            country_code=country,
            entity_type="BANK",
            product_name="Compte courant",
            product_type="CURRENT",
        )
    ]
    # Extend references without changing the existing banking keys or rows.
    for entity, tag, size in [
        ("INSURANCE", "I", 50),
        ("MOBILE_MONEY", "M", 30),
        ("MICROFINANCE", "F", 20),
    ]:
        for i in range(size):
            customer_id = f"WABA-{country}-{tag}C-{i:06d}"
            customers.append(
                dict(
                    customer_id=customer_id,
                    country_code=country,
                    entity_type=entity,
                    segment="RETAIL",
                    kyc_level="STANDARD",
                    onboarding_date="2025-01-01",
                    region=country,
                    is_active=True,
                )
            )
            accounts.append(
                dict(
                    account_id=f"WABA-{country}-{tag}A-{i:07d}",
                    customer_id=customer_id,
                    country_code=country,
                    entity_type=entity,
                    account_type={
                        "I": "INSURANCE_POLICY",
                        "M": "MOBILE_WALLET",
                        "F": "LOAN",
                    }[tag],
                    currency=currency,
                    balance=100000,
                    credit_limit=0,
                    opened_date="2025-01-01",
                    status="ACTIVE",
                )
            )
        branches.append(
            dict(
                branch_id=f"WABA-{country}-{tag}B-001",
                country_code=country,
                entity_type=entity,
                city=country,
                region=country,
                branch_type="DIGITAL_ONLY",
                is_active=True,
            )
        )
        products.append(
            dict(
                product_id=f"{entity}-STANDARD",
                country_code=country,
                entity_type=entity,
                product_name=f"Produit {entity}",
                product_type=entity,
            )
        )
    bank_accounts = [r for r in accounts if r["entity_type"] == "BANK"]
    bank_branches = [r for r in branches if r["entity_type"] == bank_entity]
    if reference_profile not in ("demo", "full"):
        raise ValueError("Unknown reference profile")

    def pick_account(entity):
        if reference_profile == "demo":
            return rng.choice([r for r in accounts if r["entity_type"] == entity])
        from waba.reference_scale import reference_row, count_for

        while True:
            account = reference_row(
                "accounts", country, rng.randrange(count_for("accounts", country))
            )
            if account["entity_type"] == entity:
                return account

    def pick_branch():
        if reference_profile == "demo":
            return rng.choice(bank_branches)
        from waba.reference_scale import reference_row, count_for

        while True:
            branch = reference_row(
                "branches", country, rng.randrange(count_for("branches", country))
            )
            if branch["entity_type"] == bank_entity:
                return branch

    transactions = []
    for i in range(rows if "bank_transactions" in requested else 0):
        account = pick_account(bank_entity)
        amount = max(500, int(rng.lognormvariate(12, 1.5)))
        status = rng.choices(["SUCCESS", "FAILED", "REVERSED"], [92, 5, 3])[0]
        transactions.append(
            dict(
                transaction_id=str(
                    event_uuid(
                        ("MICROFINANCE/" if bank_entity == "MICROFINANCE" else "")
                        + f"{seed}/{country}/{batch_id}/{start.isoformat()}/{end.isoformat()}/{i}"
                    )
                ),
                timestamp=(
                    start + timedelta(seconds=rng.randrange(seconds))
                ).isoformat(),
                account_id=account["account_id"],
                customer_id=account["customer_id"],
                beneficiary_account=pick_account(bank_entity)["account_id"],
                branch_id=pick_branch()["branch_id"],
                country_code=country,
                transaction_type=rng.choice(
                    ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT"]
                ),
                amount=amount,
                currency=currency,
                channel="MOBILE_APP",
                transaction_status=status,
                fee_amount=round(amount * 0.001, 2) if status == "SUCCESS" else 0,
                entity_type=bank_entity,
            )
        )
    datasets = dict(
        customers=customers,
        accounts=accounts,
        branches=branches,
        products=products,
        bank_transactions=transactions,
    )
    insurance = []
    mobile = []
    repayments = []
    policies = [r for r in accounts if r["account_type"] == "INSURANCE_POLICY"]
    wallets = [r for r in accounts if r["account_type"] == "MOBILE_WALLET"]
    loans = [r for r in accounts if r["account_type"] == "LOAN"]
    for i in range(rows):
        timestamp = (start + timedelta(seconds=rng.randrange(seconds))).isoformat()
        if "insurance_operations" in requested:
            policy = pick_account("INSURANCE")
            operation = rng.choice(
                [
                    "PREMIUM_PAYMENT",
                    "CLAIM_SUBMISSION",
                    "CLAIM_PAYMENT",
                    "POLICY_RENEWAL",
                    "POLICY_CANCELLATION",
                ]
            )
            claim = operation.startswith("CLAIM")
            insurance.append(
                dict(
                    operation_id=str(
                        event_uuid(
                            f"{seed}/{country}/{batch_id}/insurance/{start}/{end}/{i}"
                        )
                    ),
                    timestamp=timestamp,
                    customer_id=policy["customer_id"],
                    account_id=policy["account_id"],
                    country_code=country,
                    operation_type=operation,
                    product_line=rng.choice(
                        [
                            "VIE",
                            "IARD_AUTO",
                            "IARD_HABITATION",
                            "IARD_SANTE",
                            "PREVOYANCE",
                        ]
                    ),
                    amount=rng.randint(1000, 500000),
                    currency=currency,
                    claim_status=(
                        ("PAID" if operation == "CLAIM_PAYMENT" else "PENDING")
                        if claim
                        else ""
                    ),
                    processing_days=rng.randint(1, 30) if claim else "",
                    entity_type="INSURANCE",
                )
            )
        if "mobile_money_payments" in requested:
            sender, receiver = pick_account("MOBILE_MONEY"), pick_account(
                "MOBILE_MONEY"
            )
            mobile.append(
                dict(
                    payment_id=str(
                        event_uuid(
                            f"{seed}/{country}/{batch_id}/mobile/{start}/{end}/{i}"
                        )
                    ),
                    timestamp=timestamp,
                    sender_id=sender["customer_id"],
                    receiver_id=receiver["customer_id"],
                    sender_country=country,
                    receiver_country=country,
                    country_code=country,
                    amount=rng.randint(500, 200000),
                    currency=currency,
                    payment_type=rng.choice(
                        ["P2P", "MERCHANT_PAYMENT", "BILL_PAYMENT", "AIRTIME"]
                    ),
                    operator=rng.choice(
                        ["WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER"]
                    ),
                    status=rng.choices(["SUCCESS", "FAILED", "PENDING"], [94, 4, 2])[0],
                    fee_amount=100,
                    entity_type="MOBILE_MONEY",
                )
            )
        if "loan_repayments" in requested:
            loan = pick_account("MICROFINANCE")
            state = rng.choices(["ON_TIME", "LATE", "DEFAULT"], [82, 12, 6])[0]
            due = datetime.fromisoformat(timestamp).date()
            delay = (
                0
                if state == "ON_TIME"
                else rng.randint(1, 30) if state == "LATE" else rng.randint(90, 180)
            )
            amount = rng.randint(1000, 50000)
            repayments.append(
                dict(
                    repayment_id=str(
                        event_uuid(
                            f"{seed}/{country}/{batch_id}/loan/{start}/{end}/{i}"
                        )
                    ),
                    timestamp=timestamp,
                    loan_account_id=loan["account_id"],
                    customer_id=loan["customer_id"],
                    country_code=country,
                    amount_due=amount,
                    amount_paid=0 if state == "DEFAULT" else amount,
                    currency=currency,
                    due_date=due.isoformat(),
                    payment_date=(
                        ""
                        if state == "DEFAULT"
                        else (due + timedelta(days=delay)).isoformat()
                    ),
                    days_overdue=delay,
                    loan_type=rng.choice(["AGRICULTURAL", "MICROCREDIT"]),
                    repayment_status=state,
                    entity_type=loan["entity_type"],
                )
            )
    datasets.update(
        insurance_operations=insurance,
        mobile_money_payments=mobile,
        loan_repayments=repayments,
    )
    for name, records in datasets.items():
        if not records:
            continue
        filename = (
            f"bank_txn_{country}_20260101_01.csv"
            if name == "bank_transactions"
            else f"{name}.csv"
        )
        with (folder / filename).open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
    return datasets


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--destination", default="/data")
    parser.add_argument("--country", choices=COUNTRIES, default="CI")
    parser.add_argument("--rows", type=int, default=1000)
    args = parser.parse_args()
    generate(args.destination, args.country, args.rows)
