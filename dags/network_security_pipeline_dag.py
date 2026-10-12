"""Network Security Event Pipeline DAG.

ingest_raw -> spark_transform -> validate_output -> write_iceberg -> load_bigquery -> retain_delta -> write_delta_table

Runs entirely inside this DAG's own Docker container (see Dockerfile.airflow
and docker-compose.yaml at the project root) against Spark local[*] mode --
simulated distributed processing on one machine, not a real cluster. See
README.md for what a real deployment (EMR/Databricks/Spark-on-k8s) would
need instead.

- ingest_raw: downloads the CIC-IDS2017 CSVs from Kaggle if data/raw is
  empty (scripts/download_dataset.sh is idempotent -- a no-op if the raw
  files are already present).
- spark_transform: runs jobs/transform.py, which cleans the raw flow
  records and writes 4 Delta tables to data/delta/.
- validate_output: runs validation/validate_output.py, an independent
  pandas-based cross-check against Spark's output. Fails the task (and the
  DAG) if the check doesn't pass -- this is the gate that decides whether
  the run is trustworthy enough to publish.
- write_iceberg: exports validated Delta snapshots to the Iceberg catalog.
- load_bigquery: uploads validated Delta snapshots to daily ingestion-time
  partitioned BigQuery tables clustered by label.
- retain_delta: applies Delta retention properties and audits VACUUM results.
- write_delta_table: runs dags/publish_manifest.py, which only executes
  after validation passes, and records each Delta table's version + row
  count to data/delta/_manifest.json as the "this run is published" marker.
"""

from __future__ import annotations

import os
import shlex
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).parent))
from alerts import sla_miss_alert, task_failure_alert

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/airflow/project")
RAW_DIR = f"{PROJECT_ROOT}/data/raw"
DELTA_DIR = f"{PROJECT_ROOT}/data/delta"
ICEBERG_DIR = f"{PROJECT_ROOT}/data/iceberg"
# Shell-quote paths so local clones containing spaces work as well.
ROOT = shlex.quote(PROJECT_ROOT)
JOBS = shlex.quote(f"{PROJECT_ROOT}/jobs")

default_args = {
    "owner": "network-security-pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(minutes=90),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="network_security_event_pipeline",
    description="CIC-IDS2017 ingest -> Spark transform -> validate -> export Iceberg -> load BigQuery -> publish Delta manifest",
    default_args=default_args,
    # Airflow does not evaluate SLAs for manual runs. Set a cron expression
    # to opt into scheduled refreshes and scheduler-driven SLA monitoring.
    schedule=os.environ.get("PIPELINE_SCHEDULE") or None,
    sla_miss_callback=sla_miss_alert,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # shared local Delta paths must not overlap between runs
    tags=["spark", "delta-lake", "iceberg", "network-security"],
) as dag:
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        # download_dataset.sh checks for jobs/transform.py relative to its
        # CWD to confirm it's being run from the project root -- cd there
        # first, or that check fails before it even looks for credentials.
        #
        # Trailing space is deliberate: BashOperator templates bash_command
        # with Jinja, and Jinja's file loader tries to load any command
        # ending in ".sh"/".bash" as a template FILE rather than inline
        # content (TemplateNotFound). The trailing space avoids the match.
        bash_command=f"cd {ROOT} && bash scripts/download_dataset.sh ",
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            f"cd {JOBS} && python3 transform.py "
            f"--raw-dir {shlex.quote(RAW_DIR)} --delta-dir {shlex.quote(DELTA_DIR)}"
        ),
    )

    validate_output = BashOperator(
        task_id="validate_output",
        bash_command=(
            f"cd {ROOT} && python3 validation/validate_output.py "
            f"--raw-dir {shlex.quote(RAW_DIR)} --delta-dir {shlex.quote(DELTA_DIR)}"
        ),
    )

    write_iceberg = BashOperator(
        task_id="write_iceberg",
        bash_command=(
            f"cd {ROOT} && python3 jobs/write_iceberg.py "
            f"--delta-dir {shlex.quote(DELTA_DIR)} --warehouse {shlex.quote(ICEBERG_DIR)}"
        ),
    )

    load_bigquery = BashOperator(
        task_id="load_bigquery",
        bash_command=(
            f"cd {ROOT} && python3 jobs/load_bigquery.py "
            f"--delta-dir {shlex.quote(DELTA_DIR)}"
        ),
    )

    retain_delta = BashOperator(
        task_id="retain_delta",
        bash_command=(
            f"cd {ROOT} && python3 jobs/retain_delta.py "
            f"--delta-dir {shlex.quote(DELTA_DIR)}"
        ),
    )

    def _write_delta_table(run_id: str) -> None:
        import sys

        sys.path.insert(0, os.path.dirname(__file__))
        from publish_manifest import publish_manifest

        manifest = publish_manifest(
            DELTA_DIR, f"{DELTA_DIR}/_manifest.json", run_id=run_id
        )
        print(manifest)

    write_delta_table = PythonOperator(
        task_id="write_delta_table",
        python_callable=_write_delta_table,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    (
        ingest_raw
        >> spark_transform
        >> validate_output
        >> write_iceberg
        >> load_bigquery
        >> retain_delta
        >> write_delta_table
    )
