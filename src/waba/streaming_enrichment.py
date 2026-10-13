"""Shared streaming enrichment and alignment with existing batch Silver schemas."""

from pyspark.sql import functions as F
from waba.contracts import SCHEMAS, KEYS
from waba.quality import validate, add_reason
from waba.spark_common import create


def parse_and_validate(raw, name, spark):
    string_schema = ", ".join(
        part.split(":")[0] + " string" for part in SCHEMAS[name].split()
    )
    string_schema += (
        ", source_file string, ingestion_timestamp string, _corrupt_record string"
    )
    parsed = raw.select(
        "*", F.from_json("raw_json", string_schema).alias("event")
    ).select(
        "topic",
        "partition",
        "offset",
        "raw_json",
        "_event_time",
        "_broker_timestamp",
        "event.*",
    )
    for part in SCHEMAS[name].split():
        field, kind = part.split(":")
        parsed = parsed.withColumn(field, F.col(field).cast(kind))
    parsed = parsed.withColumn("_structure_error", F.lit(""))
    parsed = validate(parsed, name, None, spark)
    parsed = add_reason(
        parsed, F.col("source_file").isNotNull(), "Missing source provenance"
    )
    parsed = add_reason(
        parsed,
        (F.split("source_file", "/").getItem(0) == F.col("country_code"))
        & (F.split("source_file", "/").getItem(1) == F.lit(name)),
        "Source path/country/dataset mismatch",
    )
    parsed = add_reason(
        parsed,
        F.col("timestamp") <= F.col("_broker_timestamp") + F.expr("INTERVAL 5 MINUTES"),
        "Event timestamp too far in the future",
    )
    return parsed


def enrich(frame, name, spark):
    frame = frame.drop(
        "reject_reason",
        "_corrupt_record",
        "_structure_error",
        "topic",
        "partition",
        "offset",
        "raw_json",
        "_event_time",
        "_broker_timestamp",
    )
    frame = frame.withColumn("txn_date", F.to_date("timestamp")).withColumn(
        "month", F.trunc("txn_date", "month")
    )
    customers = spark.table("iceberg.silver.customers")
    accounts = spark.table("iceberg.silver.accounts")
    if name == "mobile_money_payments":
        sender = customers.select(
            F.col("customer_id").alias("sender_id"),
            F.col("country_code").alias("sender_home_country"),
            F.col("segment").alias("customer_segment"),
        )
        receiver = customers.select(
            F.col("customer_id").alias("receiver_id"),
            F.col("country_code").alias("receiver_country"),
            F.col("segment").alias("receiver_segment"),
        )
        frame = frame.join(sender, ["sender_id"], "left").join(
            receiver, ["receiver_id", "receiver_country"], "left"
        )
    else:
        dimension = customers.select(
            "country_code",
            "entity_type",
            "customer_id",
            F.col("segment").alias("customer_segment"),
            F.col("region").alias("customer_region"),
        )
        frame = frame.join(
            dimension, ["country_code", "entity_type", "customer_id"], "left"
        )
        accountkey = "loan_account_id" if name == "loan_repayments" else "account_id"
        dimension = accounts.select(
            "country_code",
            "entity_type",
            "customer_id",
            F.col("account_id").alias(accountkey),
            "account_type",
        )
        frame = frame.join(
            dimension,
            ["country_code", "entity_type", "customer_id", accountkey],
            "left",
        )
        if name == "bank_transactions":
            branches = spark.table("iceberg.silver.branches").select(
                "country_code",
                "entity_type",
                "branch_id",
                F.col("city").alias("branch_city"),
            )
            frame = frame.join(
                branches, ["country_code", "entity_type", "branch_id"], "left"
            )
        if name == "insurance_operations":
            contracts = spark.table("iceberg.silver.insurance_risk_contracts").select(
                "country_code",
                "entity_type",
                "account_id",
                "annual_premium",
                "risk_model",
            )
            frame = frame.join(
                contracts, ["country_code", "entity_type", "account_id"], "left"
            )
    products = (
        spark.table("iceberg.silver.products")
        .filter(
            F.col("product_id").isin(
                "BANK-CURRENT",
                "INSURANCE-STANDARD",
                "MOBILE_MONEY-STANDARD",
                "MICROFINANCE-STANDARD",
            )
        )
        .select("country_code", "entity_type", "product_id", "product_name")
    )
    frame = frame.join(products, ["country_code", "entity_type"], "left")
    rates = spark.table("iceberg.silver.fx_rates").select(
        "country_code",
        "entity_type",
        "currency",
        "local_per_eur",
        "rate_id",
        "valid_from",
        "valid_to",
        "fx_source",
    )
    frame = frame.join(rates, ["country_code", "entity_type", "currency"], "left")
    frame = frame.withColumn(
        "_fx_valid",
        (F.col("txn_date") >= F.col("valid_from"))
        & (F.col("txn_date") <= F.col("valid_to")),
    )
    for column in (
        "amount",
        "fee_amount",
        "amount_due",
        "amount_paid",
        "annual_premium",
    ):
        if column in frame.columns:
            frame = frame.withColumn(column, F.col(column).cast("decimal(20,3)"))
            frame = frame.withColumn(
                column + "_eur",
                (F.col(column) / F.col("local_per_eur")).cast("decimal(20,6)"),
            )
    if name in ("insurance_operations", "loan_repayments"):
        frame = frame.withColumn("accounting_model", F.lit("UNMODELLED_STREAM"))
    return frame.withColumn("stream_received_at", F.current_timestamp()).withColumn(
        "stream_event_id", F.col(KEYS[name])
    )


def align_and_merge(frame, name, spark):
    table = "iceberg.silver." + name
    existing = spark.table(table)
    # Add provenance/risk columns without changing any existing column's meaning.
    for field in frame.schema.fields:
        if field.name not in existing.columns and field.name != "_fx_valid":
            spark.sql(
                f"ALTER TABLE {table} ADD COLUMN {field.name} {field.dataType.simpleString()}"
            )
    target = spark.table(table)
    for field in target.schema.fields:
        if field.name not in frame.columns:
            frame = frame.withColumn(field.name, F.lit(None).cast(field.dataType))
    frame = frame.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in target.schema.fields]
    )
    frame.createOrReplaceTempView("incoming_stream")
    # Preserve the canonical batch row when the same event later arrives by streaming.
    frame.sparkSession.sql(
        f"MERGE INTO {table} t USING incoming_stream s ON t.country_code=s.country_code AND t.{KEYS[name]}=s.{KEYS[name]} WHEN NOT MATCHED THEN INSERT *"
    )
    return frame
