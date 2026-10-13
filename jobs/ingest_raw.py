"""Ingest all raw datasets with explicit schemas, key validation and idempotent merge."""

import csv
import io
import json
import tempfile
from pathlib import Path
from pyspark import StorageLevel
import boto3
from pyspark.sql import SparkSession, functions as F, types as T
from waba.contracts import REFERENCES, FLOW_PREFIX, SCHEMAS, KEYS
from waba.generator import COUNTRIES
from botocore.exceptions import ClientError
from waba.pipeline_config import scope
from waba.quality import validate, add_reason

selected_countries, date_from, date_to = scope()

builder = SparkSession.builder.appName("waba-raw-ingestion")
settings = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "rest",
    "spark.sql.catalog.iceberg.uri": "http://catalog:8181",
    "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.iceberg.s3.path-style-access": "true",
    "spark.sql.shuffle.partitions": "2",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.csv.parser.columnPruning.enabled": "false",
}
for key, value in settings.items():
    builder = builder.config(key, value)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
s3 = boto3.client("s3", endpoint_url="http://minio:9000")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.raw")
TYPE_MAP = {
    "string": T.StringType(),
    "timestamp": T.TimestampType(),
    "date": T.DateType(),
    "double": T.DoubleType(),
    "int": T.IntegerType(),
    "boolean": T.BooleanType(),
}


def ingest(key, name, country):
    source_head = s3.head_object(Bucket="raw-landing", Key=key)
    if name in REFERENCES and spark.catalog.tableExists(f"iceberg.raw.{name}"):
        try:
            archived = s3.head_object(Bucket="archive", Key=key)
            source_hash = source_head.get("Metadata", {}).get("sha256")
            unchanged = (
                source_hash
                and source_hash == archived.get("Metadata", {}).get("sha256")
            ) or (not source_hash and source_head["ETag"] == archived["ETag"])
            if unchanged:
                return
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                raise
    fields = [part.split(":") for part in SCHEMAS[name].split()]
    temp = tempfile.TemporaryDirectory()
    path = Path(temp.name) / "source.csv"
    s3.download_file("raw-landing", key, str(path))
    normalized_path = Path(temp.name) / "normalized.csv"
    names = [n for n, _ in fields]
    extras = ["_source_row_number", "_raw_record", "_structure_error"]
    with path.open(newline="") as source, normalized_path.open(
        "w", newline=""
    ) as output:
        reader = csv.reader(source, strict=True)
        if next(reader) != names:
            raise ValueError(f"CSV header does not match {name}; source preserved")
        writer = csv.writer(output)
        writer.writerow(names + extras)
        for ordinal, row in enumerate(reader, start=2):
            structural = "" if len(row) == len(names) else "Wrong number of CSV columns"
            writer.writerow(
                (row + [""] * len(names))[: len(names)]
                + [ordinal, json.dumps(row, ensure_ascii=False), structural]
            )
    schema = T.StructType(
        [T.StructField(n, TYPE_MAP[t], True) for n, t in fields]
        + [
            T.StructField("_source_row_number", T.LongType(), True),
            T.StructField("_raw_record", T.StringType(), True),
            T.StructField("_structure_error", T.StringType(), True),
            T.StructField("_corrupt_record", T.StringType(), True),
        ]
    )
    parsed = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("escape", '"')
        .option("multiLine", True)
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(str(normalized_path))
        .persist(StorageLevel.DISK_ONLY)
    )
    parsed.count()  # Materialize before querying Spark's corrupt-record column.
    frame = validate(parsed, name, country, spark)
    identity = [KEYS[name], "country_code"]
    duplicates = frame.groupBy(*identity).count().filter("count > 1").select(*identity)
    duplicates = duplicates.withColumn("_duplicate_key", F.lit(True))
    frame = frame.join(duplicates, identity, "left")
    frame = add_reason(
        frame, F.col("_duplicate_key").isNull(), "Duplicate ID within source file"
    )
    frame = frame.drop("_duplicate_key").persist(StorageLevel.DISK_ONLY)
    rejected = frame.filter(F.length("reject_reason") > 0)
    rejected_count = rejected.count()
    if name in REFERENCES and rejected_count:
        raise ValueError(f"Invalid reference rows in {key}; source preserved")
    if rejected_count:
        reject_path = Path(temp.name) / "rejected.jsonl"
        rejected = rejected.withColumn("source_file", F.lit(key))
        with reject_path.open("w") as stream:
            for row in rejected.toJSON().toLocalIterator():
                stream.write(row + "\n")
        s3.upload_file(str(reject_path), "archive", f"quarantine/{key}.jsonl")
    accepted_count = frame.filter(F.length("reject_reason") == 0).count()
    frame_clean = frame.filter(F.length("reject_reason") == 0).drop(
        "reject_reason",
        "_corrupt_record",
        "_source_row_number",
        "_raw_record",
        "_structure_error",
    )
    partition_date = "txn_date" if name in FLOW_PREFIX else "ingestion_date"
    frame_clean = frame_clean.withColumn(
        partition_date,
        F.to_date("timestamp") if name in FLOW_PREFIX else F.current_date(),
    )
    table = f"iceberg.raw.{name}"
    if not spark.catalog.tableExists(table):
        frame_clean.limit(0).writeTo(table).using("iceberg").partitionedBy(
            "country_code", partition_date
        ).create()
    frame_clean.createOrReplaceTempView("incoming")
    spark.sql(
        f"""MERGE INTO {table} t USING incoming s
                  ON t.{KEYS[name]} = s.{KEYS[name]} AND t.country_code = s.country_code
                  WHEN NOT MATCHED THEN INSERT *"""
    )
    report = {
        "source_file": key,
        "dataset": name,
        "accepted_rows": accepted_count,
        "rejected_rows": rejected_count,
        "source_etag": source_head["ETag"],
    }
    s3.put_object(
        Bucket="archive",
        Key=f"reports/ingestion/{key}.json",
        Body=json.dumps(report).encode(),
    )
    s3.copy_object(
        Bucket="archive", Key=key, CopySource={"Bucket": "raw-landing", "Key": key}
    )
    if name in FLOW_PREFIX:
        s3.delete_object(Bucket="raw-landing", Key=key)
    print(
        json.dumps({"event": "raw_ingestion_completed", "dataset": name, "file": key}),
        flush=True,
    )
    print(json.dumps(report), flush=True)
    frame.unpersist()
    parsed.unpersist()
    temp.cleanup()


keys = [
    obj["Key"]
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="raw-landing")
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".csv")
]
# References must be loaded before any events, customers before accounts.
for name in REFERENCES:
    for key in sorted(
        k for k in keys if "/references/" in k and k.endswith(f"/{name}.csv")
    ):
        country = key.split("/")[0]
        if country not in COUNTRIES:
            raise ValueError("Unknown reference country")
        ingest(key, name, country)
for key in sorted(keys):
    parts = key.split("/")
    if len(parts) >= 4 and parts[1] in FLOW_PREFIX:
        if parts[0] not in COUNTRIES:
            raise ValueError("Unknown event country")
        if (
            parts[0] not in selected_countries
            or (date_from and parts[2] < date_from)
            or (date_to and parts[2] > date_to)
        ):
            continue
        ingest(key, parts[1], parts[0])
spark.stop()
