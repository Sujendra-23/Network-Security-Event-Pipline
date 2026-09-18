"""Demonstrates Delta Lake time travel on the schema-evolution demo table.

Run delta_demos/schema_evolution.py FIRST -- it creates the version history
this script reads (version 0: original data; a later version: +3 rows and
a new `threat_intel_score` column). This script then reads the table
`versionAsOf` 0 and compares it against the current version, proving we can
reconstruct exactly what the table looked like before the schema change --
not just what it looks like now.

Run with:
    source scripts/env.sh
    python3 delta_demos/schema_evolution.py   # first, if not already run
    python3 delta_demos/time_travel.py
"""

from __future__ import annotations

import os
import sys

from delta.tables import DeltaTable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))
from spark_session import get_spark  # noqa: E402

TABLE_PATH = os.path.join(
    os.environ.get("DATA_DELTA_DEMO_DIR", "data/delta_demo"), "network_events"
)


def main() -> None:
    spark = get_spark("delta-time-travel-demo")

    if not os.path.exists(TABLE_PATH):
        raise SystemExit(
            f"{TABLE_PATH} doesn't exist yet -- run delta_demos/schema_evolution.py first"
        )

    dt = DeltaTable.forPath(spark, TABLE_PATH)
    history = dt.history().select("version", "timestamp", "operation").orderBy("version")
    print("[time_travel] version history:")
    history.show(truncate=False)

    latest_version = history.selectExpr("max(version)").collect()[0][0]
    if latest_version == 0:
        raise SystemExit(
            "table only has version 0 -- run delta_demos/schema_evolution.py first "
            "to create a second version to travel between"
        )

    v0 = spark.read.format("delta").option("versionAsOf", 0).load(TABLE_PATH)
    latest = spark.read.format("delta").option("versionAsOf", latest_version).load(TABLE_PATH)

    print(f"\n[time_travel] version 0: {len(v0.columns)} columns, {v0.count()} rows")
    print(f"[time_travel] columns: {v0.columns}")
    print(
        f"\n[time_travel] version {latest_version}: {len(latest.columns)} columns, "
        f"{latest.count()} rows"
    )
    print(f"[time_travel] columns: {latest.columns}")

    added_columns = set(latest.columns) - set(v0.columns)
    row_delta = latest.count() - v0.count()
    print(
        f"\n[time_travel] diff: +{row_delta} rows, new columns: {sorted(added_columns) or 'none'}"
    )

    if "threat_intel_score" in v0.columns:
        raise SystemExit(
            "time travel check FAILED: threat_intel_score should not exist at version 0"
        )
    if "threat_intel_score" not in latest.columns:
        raise SystemExit(
            f"time travel check FAILED: threat_intel_score should exist at version {latest_version}"
        )
    print(
        "\n[time_travel] PASS: version 0 predates the schema change, "
        f"version {latest_version} reflects it -- confirmed by reading both explicitly."
    )

    spark.stop()


if __name__ == "__main__":
    main()
