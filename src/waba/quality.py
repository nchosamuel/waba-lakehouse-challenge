"""Shared row validation for batch and streaming inputs."""

from pyspark.sql import functions as F
from waba.contracts import KEYS, FLOW_PREFIX
from waba.generator import COUNTRIES


def add_reason(frame, condition, reason):
    previous = F.when(F.length("reject_reason") > 0, F.col("reject_reason"))
    return frame.withColumn(
        "reject_reason",
        F.when(F.coalesce(condition, F.lit(False)), F.col("reject_reason")).otherwise(
            F.concat_ws("; ", previous, F.lit(reason))
        ),
    )


def validate(frame, name, country, spark):
    frame = frame.withColumn("reject_reason", F.lit(""))

    def check(condition, reason):
        nonlocal frame
        frame = add_reason(frame, condition, reason)

    def link(reference, columns, reason):
        nonlocal frame
        marker = "_reference_exists"
        reference = (
            reference.select(*columns).distinct().withColumn(marker, F.lit(True))
        )
        frame = frame.join(reference, columns, "left")
        frame = add_reason(frame, F.col(marker).isNotNull(), reason).drop(marker)

    check(
        F.coalesce(F.col("_structure_error"), F.lit("")) == "",
        "Wrong number of CSV columns",
    )
    check(F.col("_corrupt_record").isNull(), "Malformed record or incompatible type")
    check(
        (
            F.col("country_code").isin(*COUNTRIES)
            if country is None
            else F.col("country_code") == country
        )
        & F.col(KEYS[name]).isNotNull()
        & (F.length(F.col(KEYS[name])) > 0)
        & F.col("entity_type").isin(
            "BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"
        ),
        "Invalid country, key or entity",
    )
    if "currency" in frame.columns:
        check(
            F.col("currency")
            == (
                F.when(F.col("country_code") == "GH", "GHS").otherwise("XOF")
                if country is None
                else F.lit("GHS" if country == "GH" else "XOF")
            ),
            "Invalid currency",
        )
    customers = lambda: spark.table("iceberg.raw.customers").filter(
        F.col("country_code").isin(*COUNTRIES)
        if country is None
        else F.col("country_code") == country
    )
    accounts = lambda: spark.table("iceberg.raw.accounts").filter(
        F.col("country_code").isin(*COUNTRIES)
        if country is None
        else F.col("country_code") == country
    )
    if name == "accounts":
        link(
            customers(),
            ["customer_id", "country_code", "entity_type"],
            "Orphan account customer",
        )
    if name not in FLOW_PREFIX:
        return frame
    check(F.col("timestamp").isNotNull(), "Missing event timestamp")
    amount = "amount_due" if name == "loan_repayments" else "amount"
    check(
        (F.col(amount) > 0) & ~F.isnan(amount) & (F.abs(F.col(amount)) != float("inf")),
        "Invalid amount",
    )
    for monetary in ("fee_amount", "amount_paid"):
        if monetary in frame.columns:
            check(
                (F.col(monetary) >= 0)
                & ~F.isnan(monetary)
                & (F.abs(F.col(monetary)) != float("inf")),
                "Invalid finite " + monetary,
            )
    if name == "bank_transactions":
        check(
            F.col("transaction_status").isin("SUCCESS", "FAILED", "REVERSED")
            & (F.col("fee_amount") >= 0)
            & F.col("entity_type").isin("BANK", "MICROFINANCE"),
            "Invalid banking status",
        )
        link(
            accounts(),
            ["account_id", "customer_id", "country_code", "entity_type"],
            "Bank account/customer mismatch",
        )
        beneficiaries = accounts().select(
            F.col("account_id").alias("beneficiary_account"), "country_code"
        )
        link(
            beneficiaries, ["beneficiary_account", "country_code"], "Orphan beneficiary"
        )
        link(
            spark.table("iceberg.raw.branches"),
            ["branch_id", "country_code", "entity_type"],
            "Orphan branch",
        )
    elif name == "insurance_operations":
        check(
            (F.col("entity_type") == "INSURANCE")
            & F.col("operation_type").isin(
                "PREMIUM_PAYMENT",
                "CLAIM_SUBMISSION",
                "CLAIM_PAYMENT",
                "POLICY_RENEWAL",
                "POLICY_CANCELLATION",
            ),
            "Invalid insurance operation",
        )
        link(
            accounts().filter("account_type = 'INSURANCE_POLICY'"),
            ["account_id", "customer_id", "country_code", "entity_type"],
            "Invalid policy account/customer",
        )
        claim = F.col("operation_type").startswith("CLAIM")
        check(
            (
                ~claim
                & F.col("claim_status").isNull()
                & F.col("processing_days").isNull()
            )
            | (
                claim
                & F.col("claim_status").isin("PENDING", "APPROVED", "REJECTED", "PAID")
                & (F.col("processing_days") >= 0)
            ),
            "Invalid claim fields",
        )
    elif name == "mobile_money_payments":
        check(
            (F.col("entity_type") == "MOBILE_MONEY")
            & (
                F.col("sender_country") == F.col("country_code")
                if country is None
                else F.col("sender_country") == country
            )
            & F.col("status").isin("SUCCESS", "FAILED", "PENDING")
            & (F.col("fee_amount") >= 0),
            "Invalid mobile payment",
        )
        refs = spark.table("iceberg.raw.customers").filter(
            "entity_type = 'MOBILE_MONEY'"
        )
        link(
            refs.select(
                F.col("customer_id").alias("sender_id"),
                F.col("country_code").alias("sender_country"),
            ),
            ["sender_id"],
            "Orphan sender",
        )
        link(
            refs.select(
                F.col("customer_id").alias("receiver_id"),
                F.col("country_code").alias("receiver_country"),
            ),
            ["receiver_id", "receiver_country"],
            "Orphan receiver",
        )
    else:
        check(
            F.col("repayment_status").isin("ON_TIME", "LATE", "DEFAULT")
            & (F.col("amount_paid") >= 0)
            & (F.col("days_overdue") >= 0)
            & F.col("due_date").isNotNull(),
            "Invalid loan repayment",
        )
        refs = (
            accounts()
            .filter("account_type = 'LOAN'")
            .withColumnRenamed("account_id", "loan_account_id")
        )
        link(
            refs,
            ["loan_account_id", "customer_id", "country_code", "entity_type"],
            "Invalid loan account",
        )
        check(
            (
                (F.col("repayment_status") == "DEFAULT")
                & F.col("payment_date").isNull()
                & (F.col("amount_paid") == 0)
            )
            | (
                (F.col("repayment_status") != "DEFAULT")
                & F.col("payment_date").isNotNull()
            ),
            "Invalid repayment dates/status",
        )

    return frame
