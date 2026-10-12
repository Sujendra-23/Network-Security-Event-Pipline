"""Compare completed benchmark tables, independent of row/file ordering."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
from pyspark.sql import functions as F
from spark_session import get_spark

root = Path(sys.argv[1])
runs = json.loads((root / "results.json").read_text())["runs"]
spark = get_spark("benchmark-output-equivalence")
try:
    expected = None
    for run in runs:
        delta = root / run["run"] / "delta"
        outputs = {}
        for name in (
            "network_events",
            "label_counts",
            "src_ip_window_counts",
            "anomaly_threshold_counts",
        ):
            if not (delta / name).exists():
                outputs[name] = None
                continue
            df = spark.read.format("delta").load(str(delta / name))
            if name == "network_events":
                # Fixed-width checksum, row count and schema detect changes at
                # full scale without collecting millions of records to Python.
                fingerprint = (
                    df.select(F.xxhash64(*df.columns).alias("hash"))
                    .agg(
                        F.count("*").alias("count"),
                        F.sum(F.col("hash").cast("decimal(38,0)")).alias("sum"),
                        F.min("hash").alias("min"),
                        F.max("hash").alias("max"),
                    )
                    .first()
                    .asDict()
                )
                outputs[name] = {
                    "schema": df.schema.simpleString(),
                    "fingerprint": fingerprint,
                }
            else:
                outputs[name] = {
                    "schema": df.schema.simpleString(),
                    "rows": sorted(str(r) for r in df.collect()),
                }
        if expected is None:
            expected = outputs
        elif outputs != expected:
            raise AssertionError(f"Output mismatch: {run['run']}")
    (root / "verification.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "runs": len(runs),
                "fact_check": "schema, count, commutative xxhash64 sum/min/max",
                "aggregate_check": "exact all-row equality",
            },
            indent=2,
        )
    )
    print(f"PASS: equivalent outputs for {len(runs)} runs")
finally:
    spark.stop()
