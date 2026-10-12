"""Airflow callbacks. Destinations are resolved at runtime, never at DAG import."""

from __future__ import annotations

import json
import logging
import os
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)


def send_alert(message: str) -> None:
    """Send to configured Slack and/or Airflow email; delivery errors are logged.

    Callback failures must not obscure the original task failure. Do not log
    HTTP exceptions: they may contain the secret webhook URL.
    """
    webhook = os.environ.get("PIPELINE_SLACK_WEBHOOK_URL", "")
    recipients = [
        s.strip()
        for s in os.environ.get("PIPELINE_ALERT_EMAILS", "").split(",")
        if s.strip()
    ]
    if not webhook and not recipients:
        log.warning("Pipeline alert has no destination configured: %s", message)
    if webhook:
        try:
            if not webhook.startswith("https://hooks.slack.com/services/"):
                raise ValueError("Expected a Slack HTTPS incoming webhook")
            request = Request(
                webhook,
                data=json.dumps({"text": message}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=10) as response:
                if response.status != 200:
                    raise RuntimeError("Slack returned a non-success status")
        except Exception:  # noqa: BLE001 -- preserve the original Airflow failure
            log.error(
                "Slack pipeline alert delivery failed; check webhook configuration"
            )
    if recipients:
        try:
            from html import escape

            from airflow.utils.email import send_email

            send_email(
                recipients,
                "Network security pipeline alert",
                f"<pre>{escape(message)}</pre>",
            )
        except Exception:  # noqa: BLE001 -- preserve the original Airflow failure
            log.error(
                "Email pipeline alert delivery failed; check Airflow SMTP configuration"
            )


def task_failure_alert(context: dict) -> None:
    ti = context["task_instance"]
    send_alert(
        f"Task failed: dag={ti.dag_id} task={ti.task_id} "
        f"run={ti.run_id}\nLogs: {ti.log_url}"
    )


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    send_alert(
        f"SLA missed: dag={dag.dag_id}\nTasks: {task_list}\nBlocking: {blocking_task_list}"
    )
