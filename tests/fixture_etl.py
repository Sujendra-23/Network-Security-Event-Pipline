"""Actual Spark integration, run explicitly by CI (not unittest discovery).

Checks independently specified fixture results, both formats, retry safety,
optional-table cleanup, all-null rates, schema evolution and time travel.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs"))
sys.path.insert(0, str(ROOT))
from pyspark.sql import functions as F
from spark_session import get_spark
from transform import build_anomaly_threshold_counts
from write_iceberg import TABLES, export_tables

from iceberg_demos.schema_evolution import evolve
from iceberg_demos.time_travel import verify


def main():
    with tempfile.TemporaryDirectory(prefix="network-security-fixture-") as work:
        delta = str(Path(work, "delta"))
        warehouse = str(Path(work, "iceberg"))
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "jobs/transform.py"),
                "--raw-dir",
                str(ROOT / "tests/fixtures/raw"),
                "--delta-dir",
                delta,
            ],
            check=True,
        )
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "validation/validate_output.py"),
                "--raw-dir",
                str(ROOT / "tests/fixtures/raw"),
                "--delta-dir",
                delta,
            ],
            check=True,
        )
        spark = get_spark("fixture-etl", iceberg_warehouse=warehouse)
        try:
            events = spark.read.format("delta").load(f"{delta}/network_events")
            assert events.count() == 8, "Invalid/header rows should be dropped"
            assert events.filter(F.col("Flow_Packets/s").isNull()).count() == 2, (
                "Nonfinite rates should be null"
            )
            labels = spark.read.format("delta").load(f"{delta}/label_counts")
            assert {r.label: r.flow_count for r in labels.collect()} == {
                "BENIGN": 5,
                "DDoS": 2,
                "PortScan": 1,
            }
            anomaly = (
                spark.read.format("delta")
                .load(f"{delta}/anomaly_threshold_counts")
                .collect()
            )
            assert len(anomaly) == 1 and anomaly[0].label == "DDoS"
            assert (
                anomaly[0].anomalous_flow_count == 2
                and anomaly[0].threshold_packets_per_sec == 4.0
            )
            windows = (
                spark.read.format("delta")
                .load(f"{delta}/src_ip_window_counts")
                .collect()
            )
            assert sorted(
                (r.source_ip, r.flow_count, r.total_bytes, r.attack_flow_count)
                for r in windows
            ) == [
                ("192.0.2.1", 2, 80, 0),
                ("192.0.2.2", 3, 270, 0),
                ("192.0.2.3", 2, 280, 2),
                ("192.0.2.4", 1, 170, 1),
            ]
            null_rates = events.withColumn("Flow_Packets/s", F.lit(None).cast("double"))
            assert (
                build_anomaly_threshold_counts(
                    null_rates, {"flow_packets_per_sec": "Flow_Packets/s"}
                )
                is None
            )
            first = export_tables(spark, delta, warehouse)
            second = export_tables(spark, delta, warehouse)
            for name in TABLES:
                source = spark.read.format("delta").load(f"{delta}/{name}")
                target = spark.table(f"iceberg.security.{name}").select(*source.columns)
                assert (
                    source.exceptAll(target).count() == 0
                    and target.exceptAll(source).count() == 0
                ), name
                assert (
                    first["tables"][name]["row_count"]
                    == second["tables"][name]["row_count"]
                ), "Retry duplicated rows"
            evolve(spark, warehouse)
            verify(spark, warehouse)
            assert spark.table("iceberg.security.network_events").count() == 8, (
                "Demo mutated pipeline output"
            )
            # A new input without optional columns must remove stale outputs.
            for name in ("src_ip_window_counts", "anomaly_threshold_counts"):
                shutil.rmtree(Path(delta, name))
            report = export_tables(spark, delta, warehouse)
            for name in ("src_ip_window_counts", "anomaly_threshold_counts"):
                assert not report["tables"][name]["present"]
                assert not spark.catalog.tableExists(f"iceberg.security.{name}")
        finally:
            spark.stop()
        env = dict(
            os.environ,
            DATA_DELTA_DIR=delta,
            DATA_DELTA_DEMO_DIR=str(Path(work, "delta_demo")),
        )
        for demo in ("schema_evolution.py", "time_travel.py"):
            subprocess.run(
                [sys.executable, str(ROOT / "delta_demos" / demo)], env=env, check=True
            )
    print(
        "PASS: fixture Delta + Iceberg outputs, repeat export, cleanup, both format demos"
    )


if __name__ == "__main__":
    main()
