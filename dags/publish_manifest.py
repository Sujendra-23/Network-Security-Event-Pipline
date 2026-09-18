"""Publishes a manifest recording what the pipeline run produced.

Used as the DAG's final `write_delta_table` step. The Spark transform task
already writes the Delta tables themselves -- this step is the "publish"
gate: it runs only after validate_output has passed, and records each
table's current Delta version and row count to a JSON manifest. That
manifest is what a downstream consumer (or a human) would check to know
"what did the last validated run actually produce," rather than re-writing
data that's already there.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

# In local dev, dags/ and jobs/ are siblings under the project root, so
# "../jobs" relative to this file resolves correctly. In the Docker
# container they're mounted at different top-level paths (/opt/airflow/dags
# vs. /opt/airflow/project/jobs), so PROJECT_ROOT is set explicitly there
# (see docker-compose.yaml) and takes precedence over the relative fallback.
_project_root = os.environ.get(
    "PROJECT_ROOT", os.path.join(os.path.dirname(__file__), "..")
)
sys.path.insert(0, os.path.join(_project_root, "jobs"))

TABLE_NAMES = [
    "network_events",
    "label_counts",
    "src_ip_window_counts",
    "anomaly_threshold_counts",
]


def publish_manifest(delta_dir: str, output_path: str) -> dict:
    from delta.tables import DeltaTable

    from spark_session import get_spark

    spark = get_spark("network-security-publish-manifest")
    manifest = {
        "published_at": datetime.now(timezone.utc).isoformat(),
        "delta_dir": delta_dir,
        "tables": {},
    }

    for name in TABLE_NAMES:
        path = os.path.join(delta_dir, name)
        if not os.path.exists(path):
            # e.g. src_ip_window_counts when the raw data has no source-IP column
            manifest["tables"][name] = {"present": False}
            continue
        dt = DeltaTable.forPath(spark, path)
        version = dt.history(1).collect()[0]["version"]
        row_count = spark.read.format("delta").load(path).count()
        manifest["tables"][name] = {
            "present": True,
            "delta_version": version,
            "row_count": row_count,
        }

    spark.stop()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(manifest, f, indent=2)

    return manifest


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--delta-dir", default=os.environ.get("DATA_DELTA_DIR", "data/delta"))
    parser.add_argument(
        "--output",
        default=os.path.join(os.environ.get("DATA_DELTA_DIR", "data/delta"), "_manifest.json"),
    )
    args = parser.parse_args()

    manifest = publish_manifest(args.delta_dir, args.output)
    print(f"[publish_manifest] wrote {args.output}")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
