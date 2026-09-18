"""Demonstrates Delta Lake schema evolution on a copy of network_events.

Simulates a realistic scenario: a later batch of flow records arrives with
one extra column (`threat_intel_score`, as if a threat-intel enrichment
step was added downstream after the table was first created). We show:

  1. The guardrail: appending mismatched schema WITHOUT mergeSchema fails.
  2. The safe path: appending WITH `mergeSchema=true` succeeds, the new
     column is added, and pre-existing rows get null for it.

This deliberately runs against `data/delta_demo/network_events`, a
throwaway copy seeded from the real `network_events` table -- NOT the
table itself. The real table is read by validation/validate_output.py and
the Airflow DAG, both of which assume it only ever contains pipeline
output; mutating it here (extra rows, extra column) would break that
assumption. Delete `data/delta_demo/` any time to reset the demo.

Run with:
    source scripts/env.sh
    python3 delta_demos/schema_evolution.py
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))
from spark_session import get_spark  # noqa: E402

SOURCE_TABLE_PATH = os.path.join(
    os.environ.get("DATA_DELTA_DIR", "data/delta"), "network_events"
)
TABLE_PATH = os.path.join(
    os.environ.get("DATA_DELTA_DEMO_DIR", "data/delta_demo"), "network_events"
)


def seed_demo_table(spark) -> None:
    if os.path.exists(TABLE_PATH):
        return
    print(f"[schema_evolution] seeding demo table from {SOURCE_TABLE_PATH} -> {TABLE_PATH}")
    source = spark.read.format("delta").load(SOURCE_TABLE_PATH)
    source.write.format("delta").mode("overwrite").save(TABLE_PATH)


def main() -> None:
    spark = get_spark("delta-schema-evolution-demo")
    seed_demo_table(spark)

    print(f"[schema_evolution] table: {TABLE_PATH}")
    before = spark.read.format("delta").load(TABLE_PATH)
    print("[schema_evolution] schema BEFORE:")
    before.printSchema()
    before_count = before.count()
    print(f"[schema_evolution] row count BEFORE: {before_count}")

    # A small synthetic batch shaped like network_events plus one new column.
    new_batch = before.limit(3).withColumn(
        "threat_intel_score", F.lit(0.87).cast("double")
    )

    print("\n[schema_evolution] attempting append WITHOUT mergeSchema (expected to fail)...")
    try:
        new_batch.write.format("delta").mode("append").save(TABLE_PATH)
        print("[schema_evolution] UNEXPECTED: append without mergeSchema succeeded")
    except AnalysisException as e:
        print(f"[schema_evolution] blocked as expected: {type(e).__name__}: {str(e)[:200]}")

    print("\n[schema_evolution] appending WITH mergeSchema=true...")
    (
        new_batch.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(TABLE_PATH)
    )

    after = spark.read.format("delta").load(TABLE_PATH)
    print("[schema_evolution] schema AFTER:")
    after.printSchema()
    after_count = after.count()
    print(f"[schema_evolution] row count AFTER: {after_count} (was {before_count}, +3 appended)")

    null_score_rows = after.filter(F.col("threat_intel_score").isNull()).count()
    scored_rows = after.filter(F.col("threat_intel_score").isNotNull()).count()
    print(
        f"[schema_evolution] pre-existing rows now have null threat_intel_score: "
        f"{null_score_rows}; newly appended rows have a value: {scored_rows}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
