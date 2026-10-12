"""Sequential fresh-JVM benchmarks on identical input; never reuse output paths.

Run: python benchmarks/run.py --raw-dir data/raw --output /tmp/etl-benchmark --repeats 3
Includes startup, inference, cleaning, full fact writes and exact aggregates.
Retains logs, plans and metrics, and verifies equality against the first run.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--output", required=True, help="Must be a new directory")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--shuffle-partitions", type=int, default=os.cpu_count() or 8)
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    raw = Path(args.raw_dir).resolve()
    inputs = {p.name: p.stat().st_size for p in sorted(raw.glob("*.csv"))}
    if not inputs or args.repeats < 1:
        raise SystemExit("Need CSV input and at least one repeat")
    env = dict(os.environ)
    records = []
    # Rotate order to reduce systematic page-cache/thermal bias. No concurrent
    # Spark processes, no discarded warmup, no writes to production outputs.
    partitions = str(args.shuffle_partitions)
    profiles = [
        ("full", "true", partitions),
        ("narrow", "true", partitions),
        ("none", "true", partitions),
        ("narrow", "false", partitions),
        ("narrow", "true", str(args.shuffle_partitions * 2)),
    ]
    for repeat in range(args.repeats):
        order = profiles[repeat % len(profiles) :] + profiles[: repeat % len(profiles)]
        for mode, aqe, partitions in order:
            name = f"{mode}-aqe-{aqe}-p{partitions}-{repeat + 1}"
            run = output / name
            run.mkdir()
            current_env = dict(
                env, SPARK_AQE_ENABLED=aqe, SPARK_SHUFFLE_PARTITIONS=partitions
            )
            command = [
                sys.executable,
                str(ROOT / "jobs/transform.py"),
                "--raw-dir",
                str(raw),
                "--delta-dir",
                str(run / "delta"),
                "--cache-mode",
                mode,
                "--plans-dir",
                str(run / "plans"),
                "--metrics",
                str(run / "metrics.json"),
            ]
            with (run / "run.log").open("w") as log:
                subprocess.run(
                    command,
                    cwd=ROOT,
                    env=current_env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=True,
                )
            report = json.loads((run / "metrics.json").read_text())
            report["run"] = name
            records.append(report)
            (output / "results.json").write_text(
                json.dumps(
                    {
                        "python": platform.python_version(),
                        "platform": platform.platform(),
                        "cores": os.cpu_count(),
                        "input_bytes": inputs,
                        "runs": records,
                    },
                    indent=2,
                )
                + "\n"
            )
            print(f"{name}: {report['wall_seconds']:.3f}s", flush=True)
    # Aggregate comparisons include every group value and anomaly threshold.
    # Fact equality is checked with a commutative row fingerprint by Spark.
    subprocess.run(
        [sys.executable, str(ROOT / "benchmarks/verify.py"), str(output)],
        env=env,
        check=True,
    )


if __name__ == "__main__":
    main()
