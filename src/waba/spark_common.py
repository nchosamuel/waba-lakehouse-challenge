"""Shared Iceberg configuration and atomic, scoped writes."""

import json
from pyspark.sql import SparkSession, functions as F
from waba.pipeline_config import scope


def session(name):
    builder = SparkSession.builder.appName(name)
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
        "spark.sql.adaptive.enabled": "true",
    }
    for key, value in settings.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def selected(frame, date_column=None, use_dates=True):
    countries, start, end = scope()
    frame = frame.filter(F.col("country_code").isin(countries))
    if date_column and use_dates:
        if start:
            frame = frame.filter(F.to_date(date_column) >= F.lit(start).cast("date"))
        if end:
            frame = frame.filter(F.to_date(date_column) <= F.lit(end).cast("date"))
    return frame


def create(frame, table, date_column=None):
    schema = table.split(".")[1]
    spark = frame.sparkSession
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{schema}")
    if not spark.catalog.tableExists(table):
        writer = (
            frame.limit(0)
            .writeTo(table)
            .using("iceberg")
            .partitionedBy("country_code", *([date_column] if date_column else []))
        )
        writer.tableProperty("format-version", "2").create()


def merge(frame, table, keys, date_column=None):
    create(frame, table, date_column)
    frame.createOrReplaceTempView("incoming_stage")
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    frame.sparkSession.sql(
        f"MERGE INTO {table} t USING incoming_stage s ON {condition} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    print(json.dumps({"event": "table_merged", "table": table}), flush=True)


def replace_countries(frame, table, date_column):
    """Recompute all history of selected countries, preserving every other country."""
    create(frame, table, date_column)
    frame.writeTo(table).overwrite(F.col("country_code").isin(scope()[0]))
    print(
        json.dumps(
            {
                "event": "country_aggregates_replaced",
                "table": table,
                "countries": scope()[0],
            }
        ),
        flush=True,
    )
