"""Shared local-mode Spark session builder with Delta Lake configured.

Everything in this project runs against `local[*]` -- Spark simulating
distributed execution across the cores of one machine. See README.md for
what would need to change to run this against a real cluster.
"""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"


def get_spark(app_name: str, *, iceberg_warehouse: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
            + (
                ",org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                if iceberg_warehouse
                else ""
            ),
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Default Parquet codec (snappy) ships as a native .dylib that
        # snappy-java extracts to a fresh /tmp path on every JVM launch and
        # loads via dlopen. Under real parallel write load (many concurrent
        # write tasks across local[*]'s cores -- not visible with tiny
        # fixtures, only at real dataset scale) this intermittently trips
        # macOS Gatekeeper's library-validation policy on that freshly
        # extracted, non-Apple-signed file ("library load disallowed by
        # system policy"), crashing the whole JVM. gzip uses pure-JVM
        # java.util.zip, no native library involved, so it isn't exposed to
        # this at all -- worth it over snappy's speed edge for a local demo.
        .config("spark.sql.parquet.compression.codec", "gzip")
        # Local-mode default driver memory (1g) isn't enough for the real
        # ~885MB CIC-IDS2017 CSVs: inferSchema=true is a full extra read
        # pass. The benchmark's full-cache baseline also retains all fact
        # columns. Default to 6g (this dev machine has 24GB); CI overrides
        # this to 2g for its small fixture.
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
        # Default shuffle partition count (200) was tuned for real clusters
        # -- on a single machine with 10 cores it just creates 200 tiny
        # concurrent write tasks per stage, which both wastes overhead and
        # was part of what triggered the earlier Snappy native-lib crash
        # under heavy parallelism. Match it to local[*]'s actual core count.
        .config(
            "spark.sql.shuffle.partitions",
            os.environ.get("SPARK_SHUFFLE_PARTITIONS", str(os.cpu_count() or 8)),
        )
        .config(
            "spark.sql.adaptive.enabled", os.environ.get("SPARK_AQE_ENABLED", "true")
        )
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if iceberg_warehouse:
        builder = (
            builder.config(
                "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.iceberg.warehouse",
                str(Path(iceberg_warehouse).resolve()),
            )
        )
    return configure_spark_with_delta_pip(
        builder, extra_packages=[ICEBERG_PACKAGE] if iceberg_warehouse else []
    ).getOrCreate()
