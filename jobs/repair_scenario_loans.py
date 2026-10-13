"""One-time, audited correction of the initial synthetic fixture's loan dates.

Never touches generated user batches, keys, amounts, customers or reference rows.
A no-op after the first repair; retained to make the local development audit clear.
New installations generate the corrected fixture directly and do not need this job.
"""

import json
import boto3
from pyspark.sql import functions as F
from waba.business_scenario import scenario
from waba.spark_common import session
from waba.contracts import SCHEMAS

spark = session("waba-correct-synthetic-loan-dates")
rows = scenario()["flows"]["loan_repayments"]
contract = ", ".join(
    part.replace(":", " ") for part in SCHEMAS["loan_repayments"].split()
)
expected = spark.read.schema(contract).json(
    spark.sparkContext.parallelize([json.dumps(row) for row in rows])
)
expected.createOrReplaceTempView("expected_fixture")
fields = ("timestamp", "due_date", "payment_date", "days_overdue")
condition = " OR ".join(f"NOT (t.{field} <=> s.{field})" for field in fields)
changed = spark.sql(
    f"""SELECT t.* FROM iceberg.raw.loan_repayments t JOIN expected_fixture s
 ON t.repayment_id=s.repayment_id AND t.country_code=s.country_code WHERE {condition}"""
)
count = changed.count()
if count:
    # Audit the original synthetic rows before applying a bounded correction.
    if count > 800:
        raise ValueError("Unexpected repair scope")
    data = "\n".join(changed.toJSON().collect()) + "\n"
    s3 = boto3.client("s3", endpoint_url="http://minio:9000")
    s3.put_object(
        Bucket="archive",
        Key="reports/repairs/scenario_v1_loan_dates_original.jsonl",
        Body=data.encode(),
    )
    assignments = ", ".join(f"t.{field}=s.{field}" for field in fields)
    spark.sql(
        f"""MERGE INTO iceberg.raw.loan_repayments t USING expected_fixture s
 ON t.repayment_id=s.repayment_id AND t.country_code=s.country_code
 WHEN MATCHED AND ({condition}) THEN UPDATE SET {assignments}"""
    )
print(
    json.dumps(
        {
            "event": "synthetic_fixture_dates_corrected",
            "rows": count,
            "fields": list(fields),
        }
    )
)
spark.stop()
