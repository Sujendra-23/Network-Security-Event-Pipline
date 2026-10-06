"""Batch-load current Delta snapshots into sandbox-compatible BigQuery tables."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import tempfile

from deltalake import DeltaTable
from google.cloud import bigquery
import pyarrow.parquet as pq

TABLES = ("network_events", "label_counts", "src_ip_window_counts", "anomaly_threshold_counts")
REQUIRED = {"network_events", "label_counts"}


def export_snapshot(path: Path, destination: Path) -> tuple[int, int]:
    # Read the transaction log, never glob Delta's physical Parquet files:
    # obsolete files remain on disk after an overwrite until VACUUM.
    table = DeltaTable(str(path))
    dataset = table.to_pyarrow_dataset()
    if "label" not in dataset.schema.names:
        raise ValueError(f"{path.name} has no label column; rerun spark_transform")
    rows = 0
    with pq.ParquetWriter(destination, dataset.schema, compression="gzip",
                          coerce_timestamps="us", allow_truncated_timestamps=False) as writer:
        for batch in dataset.to_batches(batch_size=65536):
            writer.write_batch(batch)
            rows += batch.num_rows
    return table.version(), rows


def load_bigquery(delta_dir: str, project: str, dataset_id: str, location: str,
                  client=None) -> dict:
    if not project or not dataset_id:
        raise ValueError("Set BIGQUERY_PROJECT and BIGQUERY_DATASET before running load_bigquery")
    root = Path(delta_dir)
    for name in REQUIRED:
        if not (root / name / "_delta_log").is_dir():
            raise FileNotFoundError(f"Required Delta table missing: {root / name}")
    client = client or bigquery.Client(project=project, location=location)
    dataset = bigquery.Dataset(f"{project}.{dataset_id}")
    dataset.location = location
    existing = client.create_dataset(dataset, exists_ok=True)
    if existing.location.lower() != location.lower():
        raise ValueError(f"Dataset location is {existing.location}, expected {location}")
    report = {}
    with tempfile.TemporaryDirectory(prefix="bigquery-export-") as temp:
        for name in TABLES:
            if not (root / name).exists():
                # Remove a previous run's optional output to avoid stale results.
                client.delete_table(f"{project}.{dataset_id}.{name}", not_found_ok=True)
                report[name] = {"present": False}
                continue
            parquet = Path(temp) / f"{name}.parquet"
            version, rows = export_snapshot(root / name, parquet)
            config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                # Ingestion date retains historical CIC-IDS2017 rows in sandbox.
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    expiration_ms=60 * 24 * 60 * 60 * 1000,
                ),
                clustering_fields=["label"],
            )
            target = f"{project}.{dataset_id}.{name}"
            with parquet.open("rb") as stream:
                job = client.load_table_from_file(stream, target, job_config=config, location=location)
                job.result()
            if job.output_rows != rows:
                raise RuntimeError(f"{target}: expected {rows} rows, loaded {job.output_rows}")
            report[name] = {"present": True, "delta_version": version,
                            "row_count": rows, "job_id": job.job_id, "table": target}
            print(json.dumps(report[name]), flush=True)
            parquet.unlink()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-dir", default=os.getenv("DATA_DELTA_DIR", "data/delta"))
    args = parser.parse_args()
    report = load_bigquery(args.delta_dir, os.getenv("BIGQUERY_PROJECT", ""),
                          os.getenv("BIGQUERY_DATASET", "network_security"),
                          os.getenv("BIGQUERY_LOCATION", "US"))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
