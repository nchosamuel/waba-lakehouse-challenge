"""Explicit synthetic risk contract inputs; no thresholds derived from alert outputs."""

from decimal import Decimal
from waba.business_scenario import accounts, LINES
from waba.generator import COUNTRIES
from waba.spark_common import session, merge

spark = session("waba-stream-risk-context")
rows = []
for country in COUNTRIES:
    currency = "GHS" if country == "GH" else "XOF"
    rate = Decimal("12.5") if currency == "GHS" else Decimal("655.957")
    for index, policy in enumerate(accounts(country, "INSURANCE")):
        rows.append(
            (
                country,
                "INSURANCE",
                policy["account_id"],
                policy["customer_id"],
                LINES[index % 5],
                currency,
                rate * Decimal(1200),
                "2026-01-01",
                "2026-12-31",
                "SYNTHETIC_ANNUAL_CONTRACT_V1",
            )
        )
frame = spark.createDataFrame(
    rows,
    "country_code string,entity_type string,account_id string,customer_id string,product_line string,currency string,annual_premium decimal(20,3),valid_from string,valid_to string,risk_model string",
)
from pyspark.sql import functions as F

frame = frame.withColumn("valid_from", F.to_date("valid_from")).withColumn(
    "valid_to", F.to_date("valid_to")
)
merge(
    frame,
    "iceberg.silver.insurance_risk_contracts",
    ["country_code", "account_id"],
    "valid_from",
)
spark.stop()
