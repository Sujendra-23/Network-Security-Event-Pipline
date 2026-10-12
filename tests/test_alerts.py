import importlib.util
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import alerts


class AlertTests(unittest.TestCase):
    def test_failure_has_run_and_log_link(self):
        ti = SimpleNamespace(
            dag_id="pipeline",
            task_id="transform",
            run_id="forced-failure",
            log_url="https://airflow.example/log",
        )
        with patch.object(alerts, "send_alert") as send:
            alerts.task_failure_alert({"task_instance": ti})
        message = send.call_args.args[0]
        for value in (
            "pipeline",
            "transform",
            "forced-failure",
            ti.log_url,
        ):
            self.assertIn(value, message)

    def test_sla_callback_includes_blockers(self):
        with patch.object(alerts, "send_alert") as send:
            alerts.sla_miss_alert(
                SimpleNamespace(dag_id="pipeline"), "transform", "ingest", [], []
            )
        self.assertIn("SLA missed", send.call_args.args[0])
        self.assertIn("ingest", send.call_args.args[0])

    def test_slack_posts_json_with_timeout(self):
        response = Mock(status=200)
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        with (
            patch.dict(
                os.environ,
                {
                    "PIPELINE_SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/test",
                    "PIPELINE_ALERT_EMAILS": "",
                },
            ),
            patch.object(alerts, "urlopen", return_value=response) as post,
        ):
            alerts.send_alert("forced failure")
        request = post.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.data, b'{"text": "forced failure"}')
        self.assertEqual(post.call_args.kwargs["timeout"], 10)

    def test_delivery_failure_does_not_mask_task_or_leak_url(self):
        secret = "https://hooks.slack.com/services/secret"
        with (
            patch.dict(
                os.environ,
                {"PIPELINE_SLACK_WEBHOOK_URL": secret, "PIPELINE_ALERT_EMAILS": ""},
            ),
            patch.object(alerts, "urlopen", side_effect=OSError(secret)),
            self.assertLogs(alerts.log, "ERROR") as logs,
        ):
            alerts.send_alert("failed")
        self.assertNotIn(secret, "\n".join(logs.output))

    def test_unconfigured_alert_is_visible_in_logs(self):
        with (
            patch.dict(
                os.environ,
                {"PIPELINE_SLACK_WEBHOOK_URL": "", "PIPELINE_ALERT_EMAILS": ""},
            ),
            self.assertLogs(alerts.log, "WARNING"),
        ):
            alerts.send_alert("failed")

    @unittest.skipUnless(
        importlib.util.find_spec("airflow"), "Email integration runs in Docker"
    )
    def test_email_still_delivered_when_slack_fails(self):
        with (
            patch.dict(
                os.environ,
                {
                    "PIPELINE_SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/test",
                    "PIPELINE_ALERT_EMAILS": "one@example.com, two@example.com",
                },
            ),
            patch.object(alerts, "urlopen", side_effect=OSError("unavailable")),
            patch("airflow.utils.email.send_email") as email,
            self.assertLogs(alerts.log, "ERROR"),
        ):
            alerts.send_alert("task <failed>")
        self.assertEqual(
            email.call_args.args[0], ["one@example.com", "two@example.com"]
        )
        self.assertIn("&lt;failed&gt;", email.call_args.args[2])
