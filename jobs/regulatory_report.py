"""J+1 demonstrator aggregates. Not an official regulator submission format."""

import json
import os
from datetime import date, timedelta
import boto3
from pyspark.sql import functions as F, Window
from waba.spark_common import session, selected, merge
from waba.pipeline_config import scope
from waba.report_coverage import country_coverage

spark = session("waba-regulatory-report")
report_date = date.fromisoformat(
    os.environ.get("REPORT_DATE") or (date.today() - timedelta(days=1)).isoformat()
)
monthly = report_date.replace(day=1).isoformat()
# Last observed loan snapshot at or before D; no future positions in a J+1 report.
npl = selected(spark.table("iceberg.gold.npl_ratio_by_country")).filter(
    F.col("snapshot_date") <= F.lit(report_date)
)
window = Window.partitionBy("country_code").orderBy(F.col("snapshot_date").desc())
npl = (
    npl.withColumn("_rank", F.dense_rank().over(window))
    .filter("_rank = 1")
    .drop("_rank")
)
npl = npl.groupBy("country_code", "entity_type", "snapshot_date").agg(
    F.sum("total_outstanding_eur").alias("total_outstanding_eur"),
    F.sum("default_outstanding_eur").alias("default_outstanding_eur"),
)
npl = (
    npl.withColumn(
        "npl_ratio", F.col("default_outstanding_eur") / F.col("total_outstanding_eur")
    )
    .withColumn("report_date", F.lit(report_date))
    .withColumn(
        "regulator",
        F.when(
            F.col("country_code").isin("CI", "SN", "ML", "BF", "TG", "BJ"), "BCEAO"
        ).otherwise("OUTSIDE_BCEAO_SCOPE"),
    )
)
merge(
    npl,
    "iceberg.gold.regulatory_bceao_daily",
    ["country_code", "entity_type", "report_date"],
    "report_date",
)
# Reaggregate month-to-date from Silver rather than using the full monthly Gold table.
period_insurance = selected(
    spark.table("iceberg.silver.insurance_operations"), use_dates=False
).filter(
    (F.col("txn_date") >= F.lit(monthly).cast("date"))
    & (F.col("txn_date") <= F.lit(report_date))
)
insurance = period_insurance.filter("accounting_model = 'scenario-v1'")
insurance = insurance.withColumn(
    "earned_to_date_eur",
    F.when(
        F.col("operation_type") == "PREMIUM_PAYMENT",
        F.col("earned_premium_eur")
        * (
            F.datediff(
                F.least(F.col("coverage_end"), F.lit(report_date)),
                F.col("coverage_start"),
            )
            + 1
        )
        / (F.datediff("coverage_end", "coverage_start") + 1),
    ).otherwise(0),
)
loss = insurance.groupBy(
    "country_code", "entity_type", "product_id", "product_line"
).agg(
    F.sum("earned_to_date_eur").alias("earned_premiums_eur"),
    F.sum(
        F.when(
            (F.col("operation_type") == "CLAIM_PAYMENT")
            & (F.col("claim_status") == "PAID"),
            F.col("amount_eur"),
        ).otherwise(0)
    ).alias("paid_claims_eur"),
)
loss = (
    loss.withColumn(
        "loss_ratio",
        F.when(
            F.col("earned_premiums_eur") > 0,
            F.col("paid_claims_eur") / F.col("earned_premiums_eur"),
        ),
    )
    .withColumn("report_date", F.lit(report_date))
    .withColumn(
        "regulator",
        F.when(
            F.col("country_code").isin("CI", "SN", "ML", "BF", "TG", "BJ"), "CIMA"
        ).otherwise("OUTSIDE_CIMA_SCOPE"),
    )
)
merge(
    loss,
    "iceberg.gold.regulatory_cima_daily",
    ["country_code", "entity_type", "product_id", "product_line", "report_date"],
    "report_date",
)
s3 = boto3.client("s3", endpoint_url="http://minio:9000")
for name, frame in (("bceao", npl), ("cima", loss)):
    for country in scope()[0]:
        payload = (
            "\n".join(frame.filter(F.col("country_code") == country).toJSON().collect())
            + "\n"
        )
        s3.put_object(
            Bucket="archive",
            Key=f"regulatory/{report_date}/{country}/{name}.jsonl",
            Body=payload.encode(),
        )
# Publish a data-quality manifest even when a technical run succeeds with empty/stale data.
snapshots = {r.country_code: r.snapshot_date for r in npl.select("country_code", "snapshot_date").collect()}
counts = period_insurance.groupBy("country_code").agg(
    F.sum(F.when(F.col("accounting_model") == "scenario-v1", 1).otherwise(0)).alias("modelled"),
    F.sum(F.when(F.coalesce(F.col("accounting_model"), F.lit("")) != "scenario-v1", 1).otherwise(0)).alias("unmodelled"),
)
counts = {r.country_code: (r.modelled, r.unmodelled) for r in counts.collect()}
coverage = [country_coverage(c, report_date, snapshots.get(c), *counts.get(c, (0, 0))) for c in scope()[0]]
manifest = {"report_date": str(report_date), "max_snapshot_age_days": 1,
            "freshness_policy": "Demo policy: loan snapshot at most one calendar day old",
            "countries": coverage,
            "regulatory_coverage_complete": all(r["coverage_complete"] for r in coverage if r["in_regulatory_scope"]),
            "official_regulatory_submission": False}
s3.put_object(Bucket="archive", Key=f"regulatory/{report_date}/coverage.json",
              Body=json.dumps(manifest).encode(), ContentType="application/json")
print(json.dumps({"event": "regulatory_report_coverage", **manifest}), flush=True)
print(
    json.dumps(
        {
            "event": "regulatory_report_completed",
            "report_date": str(report_date),
            "countries": scope()[0],
        }
    )
)
spark.stop()
