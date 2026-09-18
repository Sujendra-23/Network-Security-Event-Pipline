"""Network Security Event Pipeline DAG.

ingest_raw -> spark_transform -> validate_output -> write_delta_table

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
- write_delta_table: runs dags/publish_manifest.py, which only executes
  after validation passes, and records each Delta table's version + row
  count to data/delta/_manifest.json as the "this run is published" marker.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/project"
RAW_DIR = f"{PROJECT_ROOT}/data/raw"
DELTA_DIR = f"{PROJECT_ROOT}/data/delta"

default_args = {
    "owner": "network-security-pipeline",
    "retries": 0,
}

with DAG(
    dag_id="network_security_event_pipeline",
    description="CIC-IDS2017 ingest -> Spark transform -> validate -> publish to Delta Lake",
    default_args=default_args,
    schedule=None,  # manually triggered: this processes a fixed historical dataset, not a stream
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "delta-lake", "network-security"],
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
        bash_command=f"cd {PROJECT_ROOT} && bash scripts/download_dataset.sh ",
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            f"cd {PROJECT_ROOT}/jobs && python3 transform.py "
            f"--raw-dir {RAW_DIR} --delta-dir {DELTA_DIR}"
        ),
    )

    validate_output = BashOperator(
        task_id="validate_output",
        bash_command=(
            f"cd {PROJECT_ROOT} && python3 validation/validate_output.py "
            f"--raw-dir {RAW_DIR} --delta-dir {DELTA_DIR}"
        ),
    )

    def _write_delta_table() -> None:
        import sys

        sys.path.insert(0, os.path.dirname(__file__))
        from publish_manifest import publish_manifest

        manifest = publish_manifest(DELTA_DIR, f"{DELTA_DIR}/_manifest.json")
        print(manifest)

    write_delta_table = PythonOperator(
        task_id="write_delta_table",
        python_callable=_write_delta_table,
    )

    ingest_raw >> spark_transform >> validate_output >> write_delta_table
