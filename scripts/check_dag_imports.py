"""Fail CI on parse/import errors (the CLI JSON command alone exits zero)."""

from pathlib import Path

from airflow.models import DagBag

bag = DagBag(
    dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False
)
if bag.import_errors:
    raise SystemExit(f"DAG import errors: {bag.import_errors}")
dag = bag.dags.get("network_security_event_pipeline")
if dag is None or len(dag.tasks) != 7:
    raise SystemExit("Expected the seven-task pipeline DAG")
print(f"PASS: {dag.dag_id}, {len(dag.tasks)} tasks, no import errors")
