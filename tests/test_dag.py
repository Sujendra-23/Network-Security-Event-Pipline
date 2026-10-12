"""Run in the pinned Airflow image with an isolated AIRFLOW_HOME."""

import importlib.util
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

HAS_AIRFLOW = importlib.util.find_spec("airflow") is not None


@unittest.skipUnless(HAS_AIRFLOW, "Airflow integration runs in Docker")
class DagTests(unittest.TestCase):
    def test_import_retry_policy_and_validation_gate(self):
        from airflow.models import DagBag

        bag = DagBag(
            dag_folder=str(Path(__file__).resolve().parents[1] / "dags"),
            include_examples=False,
        )
        self.assertEqual(bag.import_errors, {})
        dag = bag.get_dag("network_security_event_pipeline")
        self.assertIsNotNone(dag)
        for task in dag.tasks:
            self.assertEqual(task.retries, 3)
            self.assertTrue(task.retry_exponential_backoff)
            self.assertEqual(task.max_retry_delay, timedelta(minutes=10))
            self.assertIsNotNone(task.on_failure_callback)
            self.assertEqual(task.sla, timedelta(minutes=90))
        self.assertIsNotNone(dag.sla_miss_callback)
        self.assertEqual(
            dag.get_task("write_iceberg").upstream_task_ids, {"validate_output"}
        )
        self.assertEqual(
            dag.get_task("load_bigquery").upstream_task_ids, {"write_iceberg"}
        )

    def test_real_failing_task_invokes_alert_callback(self):
        import alerts
        from airflow import DAG
        from airflow.operators.bash import BashOperator
        from airflow.utils import timezone
        from airflow.utils.state import DagRunState

        with DAG(
            "ci_forced_failure",
            schedule=None,
            start_date=timezone.datetime(2024, 1, 1),
            catchup=False,
        ) as dag:
            BashOperator(
                task_id="fail",
                bash_command="exit 42",
                retries=0,
                on_failure_callback=alerts.task_failure_alert,
            )
        with patch.object(alerts, "send_alert") as send:
            run = dag.test()
        self.assertEqual(run.state, DagRunState.FAILED)
        send.assert_called_once()
        self.assertIn("task=fail", send.call_args.args[0])
        self.assertIn("attempt=1", send.call_args.args[0])
