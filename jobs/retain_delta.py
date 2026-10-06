"""Apply Delta retention and audit VACUUM for this local pipeline."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import uuid
from urllib.parse import unquote, urlparse

from deltalake import DeltaTable

TABLES = ("network_events", "label_counts", "src_ip_window_counts", "anomaly_threshold_counts")
PROPERTIES = {
    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
}


def audit_path(delta_dir: str, run_id: str) -> Path:
    key = hashlib.sha256(run_id.encode()).hexdigest()
    return Path(delta_dir) / "_retention" / f"{key}.json"


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(value, stream, indent=2)
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def local_candidate(table_path: Path, candidate: str) -> Path:
    parsed = urlparse(candidate)
    if parsed.scheme not in ("", "file"):
        raise ValueError("Retention audit supports local Delta tables only")
    path = Path(unquote(parsed.path)) if parsed.scheme else Path(candidate)
    path = (table_path / path).resolve() if not path.is_absolute() else path.resolve()
    if not path.is_relative_to(table_path):
        raise ValueError(f"VACUUM candidate outside table: {candidate}")
    return path


def retain_delta(delta_dir: str, run_id: str) -> dict:
    root = Path(delta_dir).resolve()
    for name in TABLES[:2]:
        if not (root / name / "_delta_log").is_dir():
            raise FileNotFoundError(f"Required Delta table missing: {root / name}")
    output = audit_path(delta_dir, run_id)
    report = json.loads(output.read_text()) if output.exists() else {
        "run_id": run_id, "delta_dir": str(root), "tables": {},
        "started_at": datetime.now(timezone.utc).isoformat(),
        "properties": PROPERTIES,
    }
    report["status"] = "running"
    write_json(output, report)
    try:
        for name in TABLES:
            path = root / name
            if not path.exists():
                report["tables"][name] = {"present": False}
                write_json(output, report)
                continue
            table = DeltaTable(str(path))
            if any(table.metadata().configuration.get(k) != v for k, v in PROPERTIES.items()):
                table.alter.set_table_properties(PROPERTIES)
            entry = report["tables"].setdefault(name, {"present": True, "attempts": []})
            attempt = {"started_at": datetime.now(timezone.utc).isoformat(),
                       "status": "running", "delta_version_before": table.version(),
                       "candidates": table.vacuum(dry_run=True)}
            entry["present"] = True
            entry.setdefault("attempts", []).append(attempt)
            # Persist candidates before deletion; interrupted attempts remain visible.
            write_json(output, report)
            existing = {candidate: local_candidate(path, candidate)
                        for candidate in attempt["candidates"]}
            existing = {candidate: file for candidate, file in existing.items() if file.is_file()}
            attempt["existing_candidates"] = list(existing)
            write_json(output, report)
            vacuum_result = table.vacuum(dry_run=False)
            # delta-rs can return already-absent tombstones on retries. Count only
            # files observed present before VACUUM and absent after it.
            removed = [candidate for candidate, file in existing.items() if not file.exists()]
            attempt["vacuum_result"] = vacuum_result
            table.update_incremental()
            attempt.update(status="complete", removed_files=removed,
                           removed_file_count=len(removed), delta_version_after=table.version())
            entry["delta_version"] = table.version()
            write_json(output, report)
        report["status"] = "complete"
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        report.pop("error", None)
    except Exception as exc:
        report.update(status="failed", error=str(exc))
        write_json(output, report)
        raise
    write_json(output, report)
    return report


def read_audit(delta_dir: str, run_id: str) -> dict:
    report = json.loads(audit_path(delta_dir, run_id).read_text())
    if (report.get("run_id") != run_id or report.get("status") != "complete"
            or report.get("delta_dir") != str(Path(delta_dir).resolve())):
        raise ValueError("Retention audit is incomplete or belongs to another run/directory")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta-dir", default=os.getenv("DATA_DELTA_DIR", "data/delta"))
    parser.add_argument("--run-id", default=os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or f"manual-{uuid.uuid4()}")
    args = parser.parse_args()
    print(json.dumps(retain_delta(args.delta_dir, args.run_id), indent=2))
