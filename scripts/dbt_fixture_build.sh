#!/usr/bin/env bash
# Run the real Spark job on the tracked CI fixture, then `dbt build` on its Delta output.
# Needs the Spark environment (source scripts/env.sh) and dbt (pip install -r dbt/requirements.txt).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
python "$ROOT/jobs/transform.py" --raw-dir "$ROOT/tests/fixtures/raw" --delta-dir "$WORK/delta" >/dev/null
cd "$ROOT/dbt"
DATA_DELTA_DIR="$WORK/delta" DBT_DUCKDB_PATH="$WORK/fixture.duckdb" dbt build --profiles-dir .
