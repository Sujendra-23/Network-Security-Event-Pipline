"""Prove the SCD2 snapshot keeps history: change the label catalog and snapshot again.

    python dbt/scd2_demo.py          (dbt must be on PATH)

Works on a throwaway copy of the project and its own DuckDB file, so the real seed, warehouse
and Delta tables are untouched. Needs no Delta data: only `dbt seed` and `dbt snapshot` run.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

PROJECT = Path(__file__).resolve().parent
COPIED = ("dbt_project.yml", "profiles.yml", "macros", "seeds", "snapshots")


def dbt(workdir: Path, env: dict[str, str], *args: str) -> None:
    subprocess.run(
        ["dbt", *args, "--profiles-dir", "."], cwd=workdir, env=env, check=True,
        stdout=subprocess.DEVNULL,
    )


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="dbt-scd2-") as tmp:
        work = Path(tmp)
        for name in COPIED:
            source = PROJECT / name
            (shutil.copytree if source.is_dir() else shutil.copy)(source, work / name)
        db = work / "demo.duckdb"
        env = {**os.environ, "DBT_DUCKDB_PATH": str(db)}

        dbt(work, env, "seed")
        dbt(work, env, "snapshot")

        seed = work / "seeds" / "label_catalog.csv"
        text = seed.read_text()
        changed = text.replace("PortScan,Reconnaissance,low", "PortScan,Reconnaissance,high")
        changed = changed.replace("Bot,Botnet,high", "Bot,Botnet C2,high")
        assert changed != text, "seed no longer contains the rows this demo edits"
        seed.write_text(changed)
        dbt(work, env, "seed")
        dbt(work, env, "snapshot")

        con = duckdb.connect(str(db), read_only=True)
        rows = con.execute(
            "select label, attack_family, severity, dbt_valid_to is null as is_current "
            "from snapshots.snap_label_catalog order by label, dbt_valid_from"
        ).fetchall()
        con.close()

    history: dict[str, list[tuple]] = {}
    for label, family, severity, current in rows:
        history.setdefault(label, []).append((family, severity, current))
    for label, versions in history.items():
        print(label, versions)
    problems = []
    if len(rows) != 17:
        problems.append(f"expected 17 snapshot rows (15 labels + 2 new versions), got {len(rows)}")
    if history["PortScan"] != [("Reconnaissance", "low", False), ("Reconnaissance", "high", True)]:
        problems.append("PortScan should have a closed 'low' row and a current 'high' row")
    if history["Bot"] != [("Botnet", "high", False), ("Botnet C2", "high", True)]:
        problems.append("Bot should have a closed 'Botnet' row and a current 'Botnet C2' row")
    unchanged = [k for k, v in history.items() if k not in ("PortScan", "Bot") and len(v) != 1]
    if unchanged:
        problems.append(f"unchanged labels gained versions: {unchanged}")
    for problem in problems:
        print("FAIL:", problem, file=sys.stderr)
    print("SCD2 demo:", "FAILED" if problems else "OK (history preserved, unchanged rows not duplicated)")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
