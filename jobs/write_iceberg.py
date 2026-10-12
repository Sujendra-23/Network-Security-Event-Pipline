"""Export validated Delta snapshots to an independent local Iceberg catalog.

Each table is replaced atomically. A retry replaces, rather than appends,
so a partial prior export does not duplicate rows. Multi-table publication
is not atomic; consumers should use the manifest written after all exports.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import functions as F
from retain_delta import write_json
from spark_session import get_spark

TABLES = {
    "network_events": ["event_date", "attack_category"],
    "label_counts": ["event_date"],
    "src_ip_window_counts": ["window_date"],
    "anomaly_threshold_counts": ["event_date"],
}


def export_tables(spark, delta_dir: str, warehouse: str) -> dict:
    for name in ("network_events", "label_counts"):
        if not Path(delta_dir, name, "_delta_log").is_dir():
            raise FileNotFoundError(f"Missing required Delta table: {name}")
    marker = Path(warehouse, "_manifest.json")
    marker.unlink(missing_ok=True)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.security")
    report = {"tables": {}}
    for name, partitions in TABLES.items():
        target = f"iceberg.security.{name}"
        path = str(Path(delta_dir, name))
        if not Path(path, "_delta_log").is_dir():
            spark.sql(f"DROP TABLE IF EXISTS {target}")
            report["tables"][name] = {"present": False}
            continue
        from delta.tables import DeltaTable

        version = DeltaTable.forPath(spark, path).history(1).first()["version"]
        source = spark.read.format("delta").option("versionAsOf", version).load(path)
        (
            source.writeTo(target)
            .using("iceberg")
            .partitionedBy(*[F.col(c) for c in partitions])
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "gzip")
            .createOrReplace()
        )
        source_count = source.count()
        actual_count = spark.table(target).count()
        if source_count != actual_count:
            raise ValueError(
                f"{target}: expected {source_count} rows, got {actual_count}"
            )
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {target}.history ORDER BY made_current_at DESC LIMIT 1"
        ).first()[0]
        report["tables"][name] = {
            "present": True,
            "delta_version": version,
            "iceberg_snapshot_id": snapshot,
            "row_count": actual_count,
        }
    write_json(marker, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--delta-dir", default=os.environ.get("DATA_DELTA_DIR", "data/delta")
    )
    parser.add_argument(
        "--warehouse", default=os.environ.get("DATA_ICEBERG_DIR", "data/iceberg")
    )
    args = parser.parse_args()
    spark = get_spark("network-security-iceberg", iceberg_warehouse=args.warehouse)
    try:
        print(
            json.dumps(export_tables(spark, args.delta_dir, args.warehouse), indent=2)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
