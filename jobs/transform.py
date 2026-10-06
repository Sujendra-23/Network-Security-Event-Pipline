"""Main PySpark transformation job for the Network Security Event Pipeline.

Reads raw CIC-IDS2017 CSV files, cleans them, and writes:

  1. `network_events`         -- cleaned flow-level records (the main table)
  2. `src_ip_window_counts`   -- flow counts per source IP per 5-min window
  3. `label_counts`           -- flow counts per attack label per day
  4. `anomaly_threshold_counts` -- flows whose packet rate exceeds a
                                   benign-traffic-derived threshold

All four are written as Delta Lake tables under `data/delta/`.

Run with (from the project root):
    source scripts/env.sh
    python3 jobs/transform.py

Note: this must be run as plain `python`, not `spark-submit`. Delta Lake is
injected via `configure_spark_with_delta_pip` (see spark_session.py), which
works by setting `PYSPARK_SUBMIT_ARGS` before PySpark launches its own JVM
gateway. `spark-submit` launches the JVM itself first, before that env var
takes effect, so the Delta catalog classes never get resolved and every
Delta read/write fails with `ClassNotFoundException`. If you need to
orchestrate this from Airflow, shell out to `python transform.py`
(e.g. via BashOperator), not SparkSubmitOperator.
"""

from __future__ import annotations

import argparse
import os
import shutil

from pyspark.sql import DataFrame, functions as F

from schema import (
    REQUIRED_COLUMNS,
    resolve_columns,
    sanitize_for_delta,
    strip_column_whitespace,
)
from spark_session import get_spark
from retain_delta import PROPERTIES

# CIC-IDS2017 was captured Monday July 3 - Friday July 7, 2017. Several
# Kaggle mirrors strip the Timestamp column for privacy; we fall back to
# this filename -> capture-date mapping so records can still be partitioned
# and windowed sensibly even without a per-row timestamp.
FILENAME_DATE_HINTS = {
    "monday": "2017-07-03",
    "tuesday": "2017-07-04",
    "wednesday": "2017-07-05",
    "thursday": "2017-07-06",
    "friday": "2017-07-07",
}

TIMESTAMP_FORMATS = [
    "d/M/yyyy H:mm",
    "d/M/yyyy H:mm:ss",
    "M/d/yyyy h:mm:ss a",
    "M/d/yyyy H:mm",
]


def read_raw(spark, raw_dir: str) -> DataFrame:
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("nanValue", "NaN")
        .option("positiveInf", "Infinity")
        .option("negativeInf", "-Infinity")
        .option("mode", "PERMISSIVE")
        .csv(os.path.join(raw_dir, "*.csv"))
        .withColumn("_source_file", F.input_file_name())
    )
    return strip_column_whitespace(df)


def clean(df: DataFrame, cols: dict[str, str]) -> DataFrame:
    label_col = cols["label"]

    # Normalize whitespace on the label value itself, not just the column
    # name -- CICFlowMeter's own header quirk (leading spaces on names like
    # " Label") also shows up in stray embedded header rows, where the cell
    # value is literally " Label" rather than "Label".
    df = df.withColumn(label_col, F.trim(F.col(label_col)))

    # Concatenated multi-file exports sometimes repeat a literal header
    # row in the middle of the data -- drop those, plus rows with no label.
    df = df.filter(F.col(label_col).isNotNull() & (F.col(label_col) != "Label"))

    # Every numeric feature column: coerce non-finite values (CICFlowMeter
    # writes Infinity for divide-by-zero rates, e.g. zero-duration flows)
    # to null rather than silently keeping a poisoned aggregate downstream.
    # NOTE: F.isnan() only catches NaN, NOT +/-Infinity -- a real bug we
    # shipped and caught only at real-data scale: on the actual dataset,
    # 1,777 benign rows have Flow Packets/s == Infinity. That's under 1% of
    # benign traffic, but it was enough to make approxQuantile's coarse
    # (relativeError=0.01) 99th-percentile estimate land on Infinity itself
    # in build_anomaly_threshold_counts(), which silently zeroed out the
    # entire anomaly-detection table (every finite value <= Infinity).
    for field in df.schema.fields:
        if field.dataType.typeName() in ("double", "float"):
            c = F.col(field.name)
            df = df.withColumn(
                field.name,
                F.when(F.isnan(c) | (F.abs(c) == float("inf")), None).otherwise(c),
            )

    fallback_date = F.lit(None).cast("date")
    for keyword, date_str in FILENAME_DATE_HINTS.items():
        fallback_date = F.when(
            F.lower(F.col("_source_file")).contains(keyword), F.lit(date_str).cast("date")
        ).otherwise(fallback_date)

    if "timestamp" in cols:
        ts_col = F.col(cols["timestamp"])
        parsed_ts = F.coalesce(
            *[F.to_timestamp(ts_col, fmt) for fmt in TIMESTAMP_FORMATS]
        )
        df = df.withColumn("event_time", parsed_ts)
        df = df.withColumn(
            "event_date", F.coalesce(F.to_date(F.col("event_time")), fallback_date)
        )
    else:
        df = df.withColumn("event_time", F.lit(None).cast("timestamp"))
        df = df.withColumn("event_date", fallback_date)

    df = df.withColumn(
        "attack_category",
        F.when(F.upper(F.col(label_col)) == "BENIGN", "BENIGN").otherwise("ATTACK"),
    )
    df = df.withColumnRenamed(label_col, "label")

    return df.filter(F.col("event_date").isNotNull())


def write_delta(df: DataFrame, path: str, partition_cols: list[str]) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .options(**PROPERTIES)
        .partitionBy(*partition_cols)
        .save(path)
    )


def build_src_ip_window_counts(df: DataFrame, cols: dict[str, str]) -> DataFrame | None:
    if "source_ip" not in cols:
        return None
    src_ip = cols["source_ip"]
    bytes_expr = F.lit(0)
    if "total_len_fwd_packets" in cols and "total_len_bwd_packets" in cols:
        bytes_expr = F.col(cols["total_len_fwd_packets"]).cast("long") + F.col(
            cols["total_len_bwd_packets"]
        ).cast("long")

    windowed = (
        df.filter(F.col("event_time").isNotNull())
        .withColumn("_bytes", bytes_expr)
        .groupBy(F.col(src_ip).alias("source_ip"), "label", F.window("event_time", "5 minutes"))
        .agg(
            F.count("*").alias("flow_count"),
            F.sum("_bytes").alias("total_bytes"),
            F.sum(F.when(F.col("attack_category") == "ATTACK", 1).otherwise(0)).alias(
                "attack_flow_count"
            ),
        )
        .select(
            "source_ip",
            "label",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "flow_count",
            "total_bytes",
            "attack_flow_count",
        )
        .withColumn("window_date", F.to_date("window_start"))
    )
    return windowed


def build_label_counts(df: DataFrame) -> DataFrame:
    return df.groupBy("event_date", "label", "attack_category").agg(
        F.count("*").alias("flow_count")
    )


def build_anomaly_threshold_counts(
    df: DataFrame, cols: dict[str, str]
) -> DataFrame | None:
    if "flow_packets_per_sec" not in cols:
        return None
    rate_col = cols["flow_packets_per_sec"]

    benign_rates = df.filter(F.col("attack_category") == "BENIGN").select(rate_col)
    # relativeError=0.01 (1%) sounds tight but isn't, here: CICFlowMeter's
    # Flow Packets/s has a handful of extreme outliers (garbage/overflow
    # values in the millions), and with that much tail-heaviness a 1%-error
    # approximation can overshoot all the way to the max. Confirmed on the
    # real dataset: 0.01 returned 4,000,000.0 (the near-max, only 2 rows
    # away from it) while the exact (relativeError=0.0) p99 is 1,000,000.0
    # -- 4x lower. This is a one-time full-column stat, not a hot path, so
    # paying for the exact computation is worth it over a silently-wrong
    # threshold that flags zero anomalies.
    threshold = benign_rates.approxQuantile(rate_col, [0.99], 0.0)[0]
    if threshold is None:
        return None

    flagged = df.withColumn(
        "is_anomalous", F.col(rate_col) > F.lit(threshold)
    ).filter(F.col("is_anomalous"))

    group_cols = ["event_date", "label"]
    if "source_ip" in cols:
        group_cols = ["event_date", cols["source_ip"], "label"]

    result = flagged.groupBy(*group_cols).agg(
        F.count("*").alias("anomalous_flow_count"),
        F.lit(threshold).alias("threshold_packets_per_sec"),
    )
    if "source_ip" in cols:
        result = result.withColumnRenamed(cols["source_ip"], "source_ip")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default=os.environ.get("DATA_RAW_DIR", "data/raw"))
    parser.add_argument("--delta-dir", default=os.environ.get("DATA_DELTA_DIR", "data/delta"))
    args = parser.parse_args()

    spark = get_spark("network-security-transform")

    raw = read_raw(spark, args.raw_dir)
    cols = resolve_columns(raw)
    raw, cols = sanitize_for_delta(raw, cols)

    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise ValueError(
            f"Raw data is missing required columns (resolved via schema.py aliases): {missing}. "
            f"Columns found: {sorted(raw.columns)}"
        )

    cleaned = clean(raw, cols).cache()
    total_rows = cleaned.count()
    print(f"[transform] cleaned rows: {total_rows}")

    write_delta(cleaned, f"{args.delta_dir}/network_events", ["event_date", "attack_category"])
    print("[transform] wrote network_events")

    label_counts = build_label_counts(cleaned)
    write_delta(label_counts, f"{args.delta_dir}/label_counts", ["event_date"])
    print("[transform] wrote label_counts")

    window_counts = build_src_ip_window_counts(cleaned, cols)
    if window_counts is not None:
        write_delta(window_counts, f"{args.delta_dir}/src_ip_window_counts", ["window_date"])
        print("[transform] wrote src_ip_window_counts")
    else:
        if os.path.exists(f"{args.delta_dir}/src_ip_window_counts"):
            shutil.rmtree(f"{args.delta_dir}/src_ip_window_counts")
        print("[transform] skipped src_ip_window_counts: no source IP column in this data")

    anomaly_counts = build_anomaly_threshold_counts(cleaned, cols)
    if anomaly_counts is not None:
        write_delta(
            anomaly_counts, f"{args.delta_dir}/anomaly_threshold_counts", ["event_date"]
        )
        print("[transform] wrote anomaly_threshold_counts")
    else:
        if os.path.exists(f"{args.delta_dir}/anomaly_threshold_counts"):
            shutil.rmtree(f"{args.delta_dir}/anomaly_threshold_counts")
        print(
            "[transform] skipped anomaly_threshold_counts: no Flow Packets/s column in this data"
        )

    cleaned.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
