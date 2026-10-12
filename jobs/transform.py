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
import json
import os
import shutil
import time
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from retain_delta import PROPERTIES
from schema import (
    REQUIRED_COLUMNS,
    resolve_columns,
    sanitize_for_delta,
    strip_column_whitespace,
)
from spark_session import get_spark

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
            F.lower(F.col("_source_file")).contains(keyword),
            F.lit(date_str).cast("date"),
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
        .groupBy(
            F.col(src_ip).alias("source_ip"),
            "label",
            F.window("event_time", "5 minutes"),
        )
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
    quantiles = benign_rates.approxQuantile(rate_col, [0.99], 0.0)
    if not quantiles:
        return None
    threshold = quantiles[0]
    if threshold is None:
        return None

    flagged = df.withColumn("is_anomalous", F.col(rate_col) > F.lit(threshold)).filter(
        F.col("is_anomalous")
    )

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


def run_transform(
    raw_dir: str,
    delta_dir: str,
    *,
    cache_mode: str = "none",
    plans_dir: str | None = None,
) -> dict:
    """Full fact write plus exact aggregates; measure actions, not lazy plans.

    full reproduces the prior cache strategy; narrow caches the aggregate
    projection from the written Delta fact; none reads that fact without
    caching and lets Parquet prune each action's columns.
    """
    if cache_mode not in ("full", "narrow", "none"):
        raise ValueError(f"Unknown cache mode: {cache_mode}")
    start = time.perf_counter()
    spark = get_spark("network-security-transform")
    cached = None
    timings = {}
    report = {
        "cache_mode": cache_mode,
        "settings": {
            k: spark.conf.get(k)
            for k in (
                "spark.sql.adaptive.enabled",
                "spark.sql.shuffle.partitions",
                "spark.sql.parquet.compression.codec",
                "spark.sql.session.timeZone",
            )
        },
    }

    def write(df, name, partitions):
        began = time.perf_counter()
        write_delta(df, f"{delta_dir}/{name}", partitions)
        timings[name] = time.perf_counter() - began
        if plans_dir:
            Path(plans_dir).mkdir(parents=True, exist_ok=True)
            Path(plans_dir, f"{name}.txt").write_text(
                df._jdf.queryExecution().toString()
            )
        print(f"[transform] wrote {name}")

    def remove_optional(name):
        if os.path.exists(f"{delta_dir}/{name}"):
            shutil.rmtree(f"{delta_dir}/{name}")
        print(f"[transform] skipped {name}: missing optional input")

    try:
        began = time.perf_counter()
        raw = read_raw(spark, raw_dir)
        cols = resolve_columns(raw)
        raw, cols = sanitize_for_delta(raw, cols)
        missing = [c for c in REQUIRED_COLUMNS if c not in cols]
        if missing:
            raise ValueError(f"Raw data is missing required columns: {missing}")
        cleaned = clean(raw, cols)
        timings["read_and_plan"] = time.perf_counter() - began
        if cache_mode == "full":
            cached = cleaned.cache()
            cleaned = cached
            began = time.perf_counter()
            report["cleaned_rows"] = cleaned.count()
            timings["materialize"] = time.perf_counter() - began
        write(cleaned, "network_events", ["event_date", "attack_category"])
        if cache_mode != "full":
            # The wide fact table is written once. Only these columns are
            # needed by the repeated count, label, quantile and window actions.
            needed = {"event_date", "event_time", "label", "attack_category"}
            needed.update(
                cols[c]
                for c in (
                    "source_ip",
                    "flow_packets_per_sec",
                    "total_len_fwd_packets",
                    "total_len_bwd_packets",
                )
                if c in cols
            )
            # Reuse the materialized fact table instead of parsing CSV again.
            # Parquet column pruning reads only the aggregate projection.
            cleaned = (
                spark.read.format("delta")
                .load(f"{delta_dir}/network_events")
                .select(*sorted(needed))
            )
            if cache_mode == "narrow":
                cached = cleaned.cache()
                cleaned = cached
        if cache_mode != "full":
            began = time.perf_counter()
            report["cleaned_rows"] = cleaned.count()
            timings["materialize"] = time.perf_counter() - began
        print(f"[transform] cleaned rows: {report['cleaned_rows']}")
        write(build_label_counts(cleaned), "label_counts", ["event_date"])
        window_counts = build_src_ip_window_counts(cleaned, cols)
        if window_counts is not None:
            write(window_counts, "src_ip_window_counts", ["window_date"])
        else:
            remove_optional("src_ip_window_counts")
        began = time.perf_counter()
        anomaly_counts = build_anomaly_threshold_counts(cleaned, cols)
        timings["exact_quantile"] = time.perf_counter() - began
        if anomaly_counts is not None:
            write(anomaly_counts, "anomaly_threshold_counts", ["event_date"])
        else:
            remove_optional("anomaly_threshold_counts")
        report["cached_columns"] = len(cached.columns) if cached is not None else 0
        report["input_partitions"] = raw.rdd.getNumPartitions()
        report["timings_seconds"] = timings
    finally:
        if cached is not None:
            cached.unpersist()
        spark.stop()
    report["wall_seconds"] = time.perf_counter() - start
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default=os.environ.get("DATA_RAW_DIR", "data/raw"))
    parser.add_argument(
        "--delta-dir", default=os.environ.get("DATA_DELTA_DIR", "data/delta")
    )
    parser.add_argument(
        "--cache-mode", choices=["full", "narrow", "none"], default="none"
    )
    parser.add_argument("--plans-dir", help="Write explain plans for benchmark review")
    parser.add_argument("--metrics", help="Write measured timings as JSON")
    args = parser.parse_args()
    report = run_transform(
        args.raw_dir,
        args.delta_dir,
        cache_mode=args.cache_mode,
        plans_dir=args.plans_dir,
    )
    if args.metrics:
        Path(args.metrics).parent.mkdir(parents=True, exist_ok=True)
        Path(args.metrics).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
