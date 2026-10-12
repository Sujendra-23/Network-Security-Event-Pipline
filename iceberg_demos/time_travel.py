"""Read the pre-evolution snapshot with its historical schema and row count."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from spark_session import get_spark


def verify(spark, warehouse: str):
    report = json.loads(Path(warehouse, "_demo.json").read_text())
    old = (
        spark.read.format("iceberg")
        .option("snapshot-id", report["before_snapshot_id"])
        .load(report["table"])
    )
    current = spark.table(report["table"])
    if old.count() != report["before_rows"] or current.count() != report["after_rows"]:
        raise AssertionError("Historical or current row count differs")
    if (
        "threat_intel_score" in old.columns
        or "threat_intel_score" not in current.columns
    ):
        raise AssertionError("Snapshot read did not reconstruct the historical schema")
    spark.sql(f"SELECT * FROM {report['table']}.history").show(truncate=False)
    print("[iceberg time travel] PASS: original schema and row count restored")


def main():
    warehouse = os.environ.get("DATA_ICEBERG_DIR", "data/iceberg")
    spark = get_spark("iceberg-time-travel", iceberg_warehouse=warehouse)
    try:
        verify(spark, warehouse)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
