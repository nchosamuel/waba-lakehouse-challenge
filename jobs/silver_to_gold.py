"""Financial KPIs: explicit denominators, calendar and accounting coverage."""

from pyspark.sql import functions as F, Window
from waba.spark_common import session, selected, replace_countries

spark = session("waba-gold")


def source(name):
    # Full country history is required to recompute complete monthly denominators.
    return selected(spark.table("iceberg.silver." + name), use_dates=False)


def write(frame, name, date):
    replace_countries(frame, "iceberg.gold." + name, date)


def ratio(numerator, denominator):
    return F.when(F.col(denominator) > 0, F.col(numerator) / F.col(denominator))


bank = source("bank_transactions")
write(
    bank.select("country_code", "entity_type", "transaction_id", "txn_date"),
    "batch_transaction_membership",
    "txn_date",
)
daily = bank.groupBy("country_code", "entity_type", "txn_date", "transaction_type").agg(
    F.count("*").alias("attempted_count"),
    F.sum(F.when(F.col("transaction_status") == "SUCCESS", 1).otherwise(0)).alias(
        "transaction_count"
    ),
    F.sum(
        F.when(F.col("transaction_status") == "SUCCESS", F.col("amount_eur")).otherwise(
            0
        )
    ).alias("total_amount_eur"),
    F.sum(
        F.when(
            F.col("transaction_status") == "SUCCESS", F.col("fee_amount_eur")
        ).otherwise(0)
    ).alias("fee_revenue_eur"),
)
write(daily, "daily_transaction_volume", "txn_date")

loans = source("loan_positions")
npl = loans.groupBy("country_code", "entity_type", "snapshot_date", "loan_type").agg(
    F.sum("outstanding_amount_eur").alias("total_outstanding_eur"),
    F.sum(
        F.when(
            F.col("default_flag") | (F.col("days_past_due") >= 90),
            F.col("outstanding_amount_eur"),
        ).otherwise(0)
    ).alias("default_outstanding_eur"),
    F.count("*").alias("loan_count"),
)
npl = npl.withColumn(
    "npl_ratio", ratio("default_outstanding_eur", "total_outstanding_eur")
).withColumn("above_challenge_threshold", F.col("npl_ratio") >= 0.05)
write(npl, "npl_ratio_by_country", "snapshot_date")

repayments = source("loan_repayments")
revenue = bank.select(
    "country_code",
    "entity_type",
    "month",
    "customer_id",
    "customer_segment",
    F.when(F.col("transaction_status") == "SUCCESS", F.col("fee_amount_eur"))
    .otherwise(0)
    .alias("revenue_eur"),
    F.lit(0).alias("unknown_interest_events"),
)
interest = repayments.select(
    "country_code",
    "entity_type",
    "month",
    "customer_id",
    "customer_segment",
    F.coalesce("interest_received_eur", F.lit(0)).alias("revenue_eur"),
    F.when(F.col("interest_received_eur").isNull(), 1)
    .otherwise(0)
    .alias("unknown_interest_events"),
)
arpu = (
    revenue.unionByName(interest)
    .groupBy("country_code", "entity_type", "month", "customer_segment")
    .agg(
        F.sum("revenue_eur").alias("revenue_eur"),
        F.countDistinct("customer_id").alias("active_customers"),
        F.sum("unknown_interest_events").alias("unknown_interest_events"),
    )
)
arpu = arpu.withColumn("arpc_eur", ratio("revenue_eur", "active_customers")).withColumn(
    "revenue_scope", F.lit("OBSERVED_FEES_PLUS_MODELLED_INTEREST")
)
write(arpu, "customer_arpu_monthly", "month")

insurance = source("insurance_operations")
modelled = insurance.filter("accounting_model = 'scenario-v1'")
loss = modelled.groupBy(
    "country_code", "entity_type", "month", "product_id", "product_line"
).agg(
    F.sum("earned_premium_eur").alias("earned_premiums_eur"),
    F.sum(
        F.when(
            (F.col("operation_type") == "CLAIM_PAYMENT")
            & (F.col("claim_status") == "PAID"),
            F.col("amount_eur"),
        ).otherwise(0)
    ).alias("paid_claims_eur"),
    F.count("*").alias("modelled_events"),
)
loss = loss.withColumn(
    "loss_ratio", ratio("paid_claims_eur", "earned_premiums_eur")
).withColumn("above_challenge_threshold", F.col("loss_ratio") > 0.70)
write(loss, "loss_ratio_by_product", "month")
claims = modelled.filter(
    "operation_type = 'CLAIM_PAYMENT' AND claim_status = 'PAID'"
).withColumn(
    "insurance_line",
    F.when(F.col("product_line").startswith("IARD"), "IARD").otherwise("VIE"),
)
claims = claims.groupBy("country_code", "entity_type", "month", "insurance_line").agg(
    F.countDistinct("claim_id").alias("paid_claim_count"),
    F.avg("business_processing_days").alias("avg_business_processing_days"),
)
write(claims, "claims_processing_time", "month")
coverage = insurance.groupBy("country_code", "entity_type", "month").agg(
    F.count("*").alias("total_events"),
    F.sum(
        F.when(
            (F.col("accounting_model") != "scenario-v1")
            | F.col("accounting_model").isNull(),
            1,
        ).otherwise(0)
    ).alias("unmodelled_events"),
)
write(coverage, "insurance_accounting_coverage", "month")

mobile = source("mobile_money_payments")
flow = mobile.groupBy("country_code", "entity_type", "txn_date").agg(
    F.count("*").alias("attempted_count"),
    F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias(
        "successful_count"
    ),
    F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
    F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount_eur")).otherwise(0)).alias(
        "total_amount_eur"
    ),
)
# Active users include both successful senders and receivers, deduplicated per owning country.
all_mobile = spark.table("iceberg.silver.mobile_money_payments")
senders = all_mobile.filter("status = 'SUCCESS'").select(
    F.col("sender_country").alias("country_code"),
    "entity_type",
    "txn_date",
    F.col("sender_id").alias("customer_id"),
)
receivers = all_mobile.filter("status = 'SUCCESS'").select(
    F.col("receiver_country").alias("country_code"),
    "entity_type",
    "txn_date",
    F.col("receiver_id").alias("customer_id"),
)
active = (
    senders.unionByName(receivers)
    .groupBy("country_code", "entity_type", "txn_date")
    .agg(F.countDistinct("customer_id").alias("active_users"))
)
flow = (
    flow.join(active, ["country_code", "entity_type", "txn_date"], "left")
    .fillna(0, ["active_users"])
    .withColumn("failure_rate", ratio("failed_count", "attempted_count"))
)
write(flow, "mobile_money_daily_flow", "txn_date")
# Real UEMOA subset of the eight supplied countries; GN uses XOF only as a challenge convention.
uemoa = ("CI", "SN", "ML", "BF", "TG", "BJ")
cross = mobile.filter(
    (F.col("sender_country") != F.col("receiver_country"))
    & F.col("sender_country").isin(*uemoa)
    & F.col("receiver_country").isin(*uemoa)
    & (F.col("status") == "SUCCESS")
)
cross = (
    cross.withColumn("week", F.to_date(F.date_trunc("week", "timestamp")))
    .groupBy(
        "country_code", "entity_type", "sender_country", "receiver_country", "week"
    )
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount_eur").alias("total_amount_eur"),
        F.avg("amount_eur").alias("average_amount_eur"),
    )
)
window = Window.partitionBy("sender_country", "receiver_country").orderBy("week")
cross = cross.withColumn("previous_week", F.lag("week").over(window)).withColumn(
    "previous_week_amount_eur", F.lag("total_amount_eur").over(window)
)
cross = cross.withColumn(
    "weekly_growth",
    F.when(
        (F.datediff("week", "previous_week") == 7)
        & (F.col("previous_week_amount_eur") > 0),
        (F.col("total_amount_eur") - F.col("previous_week_amount_eur"))
        / F.col("previous_week_amount_eur"),
    ),
)
write(cross, "cross_border_transfers", "week")
spark.stop()
