"""Normalize types, validate enrichment and expose explicit accounting coverage."""

from decimal import Decimal
from pyspark.sql import functions as F, Window
from waba.spark_common import session, selected, merge, replace_countries
from waba.contracts import KEYS, REFERENCES

spark = session("waba-silver")
# Scenario FX, intentionally not a live market-data feed. Every row records its source.
rates = spark.createDataFrame(
    [
        (
            "SIM-XOF-2026",
            "XOF",
            Decimal("655.957000"),
            "2026-01-01",
            "2026-12-31",
            "SIMULATED_CONFIG",
        ),
        (
            "SIM-GHS-2026",
            "GHS",
            Decimal("12.500000"),
            "2026-01-01",
            "2026-12-31",
            "SIMULATED_CONFIG",
        ),
    ],
    "rate_id string, currency string, local_per_eur decimal(18,6), valid_from string, valid_to string, fx_source string",
)
rates = rates.withColumn("valid_from", F.to_date("valid_from")).withColumn(
    "valid_to", F.to_date("valid_to")
)
# Country replication makes FX lineage conform to the multi-country/entity contract.
countries = spark.createDataFrame(
    [
        (c, e)
        for c in __import__("waba.pipeline_config", fromlist=["scope"]).scope()[0]
        for e in ("BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE")
    ],
    "country_code string, entity_type string",
)
replace_countries(rates.crossJoin(countries), "iceberg.silver.fx_rates", "valid_from")


def clean(frame, key):
    for field in frame.schema.fields:
        if field.dataType.simpleString() == "string":
            frame = frame.withColumn(
                field.name, F.when(F.length(F.trim(field.name)) > 0, F.trim(field.name))
            )
    window = Window.partitionBy("country_code", key).orderBy(
        F.col("bronze_loaded_at").desc_nulls_last()
    )
    return (
        frame.withColumn("_rank", F.row_number().over(window))
        .filter("_rank = 1")
        .drop("_rank")
    )


for name in REFERENCES:
    frame = clean(selected(spark.table(f"iceberg.bronze.{name}")), KEYS[name])
    if name == "accounts":
        frame = frame.withColumn("balance", F.col("balance").cast("decimal(20,3)"))
        frame = frame.withColumn(
            "credit_limit", F.col("credit_limit").cast("decimal(20,3)")
        )
    merge(
        frame, f"iceberg.silver.{name}", ["country_code", KEYS[name]], "ingestion_date"
    )

customers = spark.table("iceberg.silver.customers")
accounts = spark.table("iceberg.silver.accounts")
products = spark.table("iceberg.silver.products").select(
    "country_code", "entity_type", "product_id", "product_name"
)
# Use the shared standard product present in the original catalogue; keep product_line separately.
products = products.filter(
    F.col("product_id").isin(
        "BANK-CURRENT",
        "INSURANCE-STANDARD",
        "MOBILE_MONEY-STANDARD",
        "MICROFINANCE-STANDARD",
    )
)
context = spark.table("iceberg.bronze.accounting_context").select(
    "event_id",
    "country_code",
    "earned_premium",
    "interest_received",
    "coverage_start",
    "coverage_end",
    "claim_id",
    "claim_submitted_date",
    "claim_paid_date",
    F.col("model_version").alias("accounting_model"),
)


def fx(frame, columns):
    frame = frame.join(
        rates,
        (frame.currency == rates.currency)
        & (F.to_date(frame.timestamp) >= rates.valid_from)
        & (F.to_date(frame.timestamp) <= rates.valid_to),
        "left",
    ).drop(rates.currency)
    if frame.filter("local_per_eur IS NULL").limit(1).count():
        raise ValueError("FX rate missing for an event date; no silent fallback")
    for column in columns:
        frame = frame.withColumn(column, F.col(column).cast("decimal(20,3)"))
        frame = frame.withColumn(
            column + "_eur",
            (F.col(column) / F.col("local_per_eur")).cast("decimal(20,6)"),
        )
    return frame.withColumn("txn_date", F.to_date("timestamp")).withColumn(
        "month", F.trunc("txn_date", "month")
    )


def require(frame, columns):
    condition = None
    for name in columns:
        condition = (
            F.col(name).isNull()
            if condition is None
            else condition | F.col(name).isNull()
        )
    if frame.filter(condition).limit(1).count():
        raise ValueError("Missing enrichment: " + ",".join(columns))
    return frame


for name in (
    "bank_transactions",
    "insurance_operations",
    "mobile_money_payments",
    "loan_repayments",
):
    key = KEYS[name]
    frame = clean(selected(spark.table(f"iceberg.bronze.{name}"), "timestamp"), key)
    if name == "mobile_money_payments":
        sender = customers.select(
            F.col("customer_id").alias("sender_id"),
            F.col("country_code").alias("sender_country"),
            F.col("segment").alias("customer_segment"),
        )
        receiver = customers.select(
            F.col("customer_id").alias("receiver_id"),
            F.col("country_code").alias("receiver_country"),
            F.col("segment").alias("receiver_segment"),
        )
        frame = frame.join(sender, ["sender_id", "sender_country"], "left").join(
            receiver, ["receiver_id", "receiver_country"], "left"
        )
        frame = require(frame, ["customer_segment", "receiver_segment"])
        frame = fx(frame, ["amount", "fee_amount"])
    else:
        customer_dim = customers.select(
            "country_code",
            "customer_id",
            "entity_type",
            F.col("segment").alias("customer_segment"),
            F.col("region").alias("customer_region"),
        )
        frame = frame.join(
            customer_dim, ["country_code", "customer_id", "entity_type"], "left"
        )
        account_key = "loan_account_id" if name == "loan_repayments" else "account_id"
        account_dim = accounts.select(
            "country_code",
            "entity_type",
            "customer_id",
            F.col("account_id").alias(account_key),
            "account_type",
        )
        frame = frame.join(
            account_dim,
            ["country_code", "entity_type", "customer_id", account_key],
            "left",
        )
        frame = require(frame, ["customer_segment", "account_type"])
        if name == "bank_transactions":
            branches = spark.table("iceberg.silver.branches").select(
                "country_code",
                "entity_type",
                "branch_id",
                F.col("city").alias("branch_city"),
            )
            frame = require(
                frame.join(
                    branches, ["country_code", "entity_type", "branch_id"], "left"
                ),
                ["branch_city"],
            )
            frame = fx(frame, ["amount", "fee_amount"])
        else:
            frame = (
                frame.join(
                    context,
                    (frame[key] == context.event_id)
                    & (frame.country_code == context.country_code),
                    "left",
                )
                .drop(context.country_code)
                .drop("event_id")
            )
            frame = frame.withColumn(
                "accounting_model",
                F.coalesce("accounting_model", F.lit("UNMODELLED_LEGACY")),
            )
            if name == "insurance_operations":
                frame = fx(frame, ["amount", "earned_premium"])
                # Weekday calendar, excludes weekends; public holidays are not supplied.
                days = F.expr(
                    "filter(sequence(claim_submitted_date, claim_paid_date), d -> dayofweek(d) NOT IN (1,7) AND d > claim_submitted_date)"
                )
                frame = frame.withColumn(
                    "business_processing_days",
                    F.when(F.col("claim_paid_date").isNotNull(), F.size(days)),
                )
            else:
                frame = fx(frame, ["amount_due", "amount_paid", "interest_received"])
    frame = require(
        frame.join(products, ["country_code", "entity_type"], "left"), ["product_id"]
    )
    merge(frame, f"iceberg.silver.{name}", ["country_code", key], "txn_date")

positions = selected(spark.table("iceberg.bronze.loan_positions"), "snapshot_date")
positions = positions.join(
    accounts.select(
        "country_code",
        "entity_type",
        "customer_id",
        F.col("account_id").alias("loan_account_id"),
        "account_type",
    ),
    ["country_code", "entity_type", "customer_id", "loan_account_id"],
    "left",
)
positions = require(positions, ["account_type"])
if positions.filter("account_type != 'LOAN'").limit(1).count():
    raise ValueError("Loan snapshot references a non-loan account")
if (
    positions.filter(
        "outstanding_amount <= 0 OR days_past_due < 0 OR default_flag IS NULL"
    )
    .limit(1)
    .count()
):
    raise ValueError("Invalid loan snapshot")
positions = positions.join(
    rates,
    (positions.currency == rates.currency)
    & (positions.snapshot_date >= rates.valid_from)
    & (positions.snapshot_date <= rates.valid_to),
    "left",
).drop(rates.currency)
positions = require(positions, ["local_per_eur"]).drop(
    "rate_id", "valid_from", "valid_to"
)
positions = positions.withColumn(
    "outstanding_amount_eur",
    (F.col("outstanding_amount") / F.col("local_per_eur")).cast("decimal(20,6)"),
)
merge(
    positions,
    "iceberg.silver.loan_positions",
    ["country_code", "loan_account_id", "snapshot_date"],
    "snapshot_date",
)
spark.stop()
