"""Explicit demo-only subscription associations and SLA, absent from raw inputs."""

from pyspark.sql import Window, functions as F
from waba.spark_common import session, replace_countries

spark = session("waba-bi-context")
accounts = spark.table("iceberg.silver.accounts").filter(
    "entity_type IN ('BANK','INSURANCE')"
)
products = spark.table("iceberg.silver.products").filter(
    "entity_type IN ('BANK','INSURANCE')"
)
window = Window.partitionBy("country_code", "entity_type").orderBy("product_id")
products = products.withColumn("product_rank", F.row_number().over(window) - 1)
counts = products.groupBy("country_code", "entity_type").agg(
    F.count("*").alias("product_count")
)
# Fixed hash assignment gives one valid catalog product per account, no fan-out.
subscriptions = accounts.join(counts, ["country_code", "entity_type"]).withColumn(
    "product_rank", F.pmod(F.xxhash64("account_id"), F.col("product_count"))
)
subscriptions = subscriptions.join(
    products, ["country_code", "entity_type", "product_rank"]
).select(
    "country_code",
    "entity_type",
    "account_id",
    "product_id",
    "product_name",
    "opened_date",
)
subscriptions = subscriptions.withColumn(
    "model_version", F.lit("SUBSCRIPTIONS_SIMULATED_V1")
)
replace_countries(
    subscriptions, "iceberg.silver.account_product_subscriptions", "opened_date"
)
popularity = subscriptions.groupBy(
    "country_code", "entity_type", "product_id", "product_name"
).agg(F.count("*").alias("subscription_count"))
popularity = popularity.withColumn(
    "model_version", F.lit("SUBSCRIPTIONS_SIMULATED_V1")
).withColumn("snapshot_date", F.current_date())
replace_countries(popularity, "iceberg.gold.product_subscriptions", "snapshot_date")
sla = spark.table("iceberg.gold.claims_processing_time").withColumn(
    "sla_business_days",
    F.when(F.col("insurance_line") == "IARD", F.lit(15)).otherwise(F.lit(30)),
)
sla = sla.withColumn("sla_model", F.lit("SLA_SIMULATED_V1")).withColumn(
    "within_demo_sla",
    F.col("avg_business_processing_days") <= F.col("sla_business_days"),
)
replace_countries(sla, "iceberg.gold.claims_sla", "month")
spark.stop()
