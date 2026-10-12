"""Add a nullable column and append three rows in a separate demo namespace."""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from pyspark.sql import functions as F
from retain_delta import write_json
from spark_session import get_spark


def evolve(spark, warehouse: str) -> dict:
    source = "iceberg.security.network_events"
    target = "iceberg.demo.network_events"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")
    # Only reset the isolated demo table; production output is read-only here.
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    spark.table(source).writeTo(target).using("iceberg").tableProperty(
        "write.parquet.compression-codec", "gzip"
    ).create()
    before = spark.table(target).count()
    if before < 3:
        raise ValueError("Demo needs at least three seed rows")
    snapshot = spark.sql(f"SELECT snapshot_id FROM {target}.snapshots").first()[0]
    spark.sql(f"ALTER TABLE {target} ADD COLUMN threat_intel_score DOUBLE")
    batch = (
        spark.table(source)
        .orderBy("event_date", "label")
        .limit(3)
        .withColumn("threat_intel_score", F.lit(0.9))
    )
    batch.writeTo(target).append()
    current = spark.table(target)
    if current.count() != before + 3:
        raise AssertionError("Evolution append lost or duplicated rows")
    if current.filter("threat_intel_score IS NULL").count() != before:
        raise AssertionError("Existing rows must have NULL for the new column")
    report = {
        "table": target,
        "before_snapshot_id": snapshot,
        "before_rows": before,
        "after_rows": before + 3,
    }
    write_json(Path(warehouse, "_demo.json"), report)
    print(f"[iceberg schema] PASS: {report}")
    return report


def main():
    warehouse = os.environ.get("DATA_ICEBERG_DIR", "data/iceberg")
    spark = get_spark("iceberg-schema-evolution", iceberg_warehouse=warehouse)
    try:
        evolve(spark, warehouse)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
