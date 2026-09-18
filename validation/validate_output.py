"""Independent pandas-based sanity check for the Spark pipeline's output.

Deliberately does NOT import jobs/schema.py or jobs/transform.py's cleaning
logic: reusing the pipeline's own code to "validate" its own output would
just confirm Spark did what Spark's code says it did, not provide
independent verification. Instead this:

  1. Re-implements the label-count aggregate from scratch in pandas,
     against ONE raw CSV file (a natural "small sample" of the full
     multi-file dataset).
  2. Reads Spark's already-materialized `network_events` Delta table,
     filtered to rows whose `_source_file` is that same raw file, groups
     by label itself, and compares counts exactly -- a real cross-engine
     check, not a fuzzy approximation. (Earlier this filtered by
     `event_date` instead of `_source_file`: CIC-IDS2017's Thursday has
     TWO raw files -- Morning-WebAttacks and Afternoon-Infiltration --
     that both fall back to the same capture date, 2017-07-06, when a
     file has no parseable Timestamp column. Date-filtering silently
     pulled in both files' rows on the Spark side while pandas only read
     one, producing a spurious mismatch. Filtering by the exact source
     file avoids that regardless of how many files share a date.)
  3. Does a second, cheap structural check across the WHOLE raw dataset
     (line-counting only, no full load into memory): total cleaned rows
     in `network_events` must be <= total raw rows across all files,
     since cleaning only ever drops rows.

Run with:
    source scripts/env.sh
    python3 validation/validate_output.py [--raw-dir data/raw] [--delta-dir data/delta]
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))
from spark_session import get_spark  # noqa: E402  (infra helper only, not cleaning logic)

def pick_sample_file(raw_dir: str) -> str:
    files = sorted(glob.glob(os.path.join(raw_dir, "*.csv")))
    if not files:
        raise SystemExit(f"No CSV files found under {raw_dir}")
    # smallest file first: keeps the pandas side genuinely "small"
    return min(files, key=os.path.getsize)


def pandas_label_counts(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    label_col = next((c for c in df.columns if c.lower() == "label"), None)
    if label_col is None:
        raise SystemExit(f"{path}: no Label column found (columns: {list(df.columns)})")

    label = df[label_col].astype(str).str.strip()
    is_stray_header_row = df[label_col].isna() | (label == "Label")
    label = label[~is_stray_header_row]
    attack_category = label.str.upper().where(label.str.upper() == "BENIGN", "ATTACK")

    counts = (
        pd.DataFrame({"label": label, "attack_category": attack_category})
        .groupby(["label", "attack_category"])
        .size()
        .reset_index(name="pandas_flow_count")
    )
    return counts


def count_raw_lines(raw_dir: str) -> int:
    total = 0
    for path in glob.glob(os.path.join(raw_dir, "*.csv")):
        with open(path, "rb") as f:
            total += sum(1 for _ in f) - 1  # minus header
    return total


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default=os.environ.get("DATA_RAW_DIR", "data/raw"))
    parser.add_argument("--delta-dir", default=os.environ.get("DATA_DELTA_DIR", "data/delta"))
    args = parser.parse_args()

    sample_path = pick_sample_file(args.raw_dir)
    sample_basename = os.path.basename(sample_path)
    print(f"[validate] pandas sample file: {sample_path}")

    pandas_counts = pandas_label_counts(sample_path)

    spark = get_spark("network-security-validate")
    events = spark.read.format("delta").load(f"{args.delta_dir}/network_events")
    same_file_events = events.filter(events["_source_file"].contains(sample_basename))

    # NOTE: deliberately .collect() instead of .toPandas() -- pyspark 3.5.3's
    # toPandas() imports `distutils`, which was removed in Python 3.12+ and
    # crashes on this project's Python 3.13 venv.
    spark_rows = same_file_events.groupBy("label").count().collect()
    spark_counts = pd.DataFrame(
        [(r["label"], r["count"]) for r in spark_rows],
        columns=["label", "spark_flow_count"],
    )
    spark.stop()

    pandas_by_label = pandas_counts.groupby("label")["pandas_flow_count"].sum()
    merged = pandas_by_label.to_frame().join(
        spark_counts.set_index("label")["spark_flow_count"], how="outer"
    )
    merged = merged.fillna(0).astype({"pandas_flow_count": int, "spark_flow_count": int})
    merged["match"] = merged["pandas_flow_count"] == merged["spark_flow_count"]

    print(f"\n[validate] label counts: pandas vs. Spark, both filtered to {sample_basename}")
    print(merged.to_string())

    label_check_passed = bool(merged["match"].all())

    print("\n[validate] structural check: cleaned rows <= raw rows (across all files)")
    raw_row_count = count_raw_lines(args.raw_dir)
    spark2 = get_spark("network-security-validate-rowcount")
    cleaned_row_count = spark2.read.format("delta").load(f"{args.delta_dir}/network_events").count()
    spark2.stop()
    print(f"[validate] raw rows (all files, header excluded): {raw_row_count}")
    print(f"[validate] cleaned rows (network_events):          {cleaned_row_count}")
    structural_check_passed = cleaned_row_count <= raw_row_count

    passed = label_check_passed and structural_check_passed
    print(f"\n[validate] RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
