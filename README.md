# Network Security Event Pipeline

Ingests the CIC-IDS2017 network intrusion dataset, cleans and aggregates it
with Spark, writes the result to Delta Lake tables, and orchestrates the
whole thing with Airflow.

## Architecture

```
Kaggle (CIC-IDS2017 mirror)
        |
        v
  data/raw/*.csv  ----------------------------+
        |                                     |
        v                                     v
  jobs/transform.py (Spark, local[*])   validation/validate_output.py
        |                                (independent pandas cross-check)
        v
  data/delta/
    network_events            (cleaned flow records)
    label_counts               (flows per label per day)
    src_ip_window_counts       (flows per source IP per 5-min window)*
    anomaly_threshold_counts   (flows exceeding a benign-derived rate threshold)

  * only written if the raw data has a source-IP column (see "Dataset" below)

Orchestrated by Airflow: ingest_raw -> spark_transform -> validate_output -> write_delta_table
```

## Dataset

[CIC-IDS2017](https://www.unb.ca/cic/datasets/ids-2017.html) (Canadian
Institute for Cybersecurity) -- labeled network traffic captured Mon
Jul 3 - Fri Jul 7, 2017: normal traffic plus DDoS, port scan, brute-force,
infiltration, and web attacks.

The official UNB source requires filling out a request form, so
`scripts/download_dataset.sh` pulls the standard, widely-cited Kaggle
mirror instead: [`chethuhn/network-intrusion-dataset`](https://www.kaggle.com/datasets/chethuhn/network-intrusion-dataset)
(the "MachineLearningCVE" CICFlowMeter export) -- 8 daily CSVs, ~230MB
zipped / ~885MB unzipped, 2,830,743 flow records total (confirmed by
actually downloading and processing it -- see "Real-data findings" below).
**This mirror does not include Source IP /
Destination IP / Flow ID columns** (privacy scrubbing before re-upload),
though `Timestamp` is present. `jobs/schema.py` resolves column names via
aliases and degrades gracefully when optional columns are absent:
`src_ip_window_counts` and the per-IP breakdown in
`anomaly_threshold_counts` are simply skipped for this mirror, which is
why they're marked conditional above.

### Getting the data

```bash
# 1. Get a Kaggle API token: https://www.kaggle.com/settings -> API -> Create New Token
#    Either credential format works (scripts/download_dataset.sh checks both):
mkdir -p ~/.kaggle && mv ~/Downloads/kaggle.json ~/.kaggle/ && chmod 600 ~/.kaggle/kaggle.json
#    ...or the newer plain-text token format:
mkdir -p ~/.kaggle && echo YOUR_TOKEN > ~/.kaggle/access_token && chmod 600 ~/.kaggle/access_token

# 2. Download (idempotent -- skips if data/raw already has CSVs)
source scripts/env.sh
scripts/download_dataset.sh
```

The `kaggle` package actually installed in this venv is 2.2.4 (not the
1.6.17 originally pinned -- requirements.txt now matches what's really
installed). It supports both credential formats above; only the legacy
`kaggle.json` (username+key) was documented here at first, which meant a
valid `~/.kaggle/access_token` was initially rejected with a "missing
credentials" error until `scripts/download_dataset.sh`'s check was widened.

## Local environment

Spark 3.5 / Hadoop's auth code crashes on Java 24 (the default JDK on a
modern Mac) with `UnsupportedOperationException: getSubject is not
supported`. `scripts/env.sh` pins `JAVA_HOME` to a Homebrew-installed Java
17 for this project only -- it does not touch the system default Java.

```bash
brew install openjdk@17          # once
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
source scripts/env.sh            # JAVA_HOME + venv, every new shell
```

Delta Lake's `configure_spark_with_delta_pip` resolves the Delta jars by
setting `PYSPARK_SUBMIT_ARGS` before PySpark launches its own JVM gateway.
`spark-submit` launches that JVM itself first, so the Delta catalog class
never resolves under `spark-submit` (`ClassNotFoundException`). Every job
in this project is therefore run as a plain script, not via `spark-submit`:

```bash
python3 jobs/transform.py                 # writes the 4 Delta tables
python3 validation/validate_output.py     # independent pandas sanity check
python3 delta_demos/schema_evolution.py   # schema evolution demo
python3 delta_demos/time_travel.py        # time travel demo (run after the above)
```

Also note: `pyspark==3.5.3`'s `.toPandas()` imports `distutils`, removed in
Python 3.12+. `validation/validate_output.py` uses `.collect()` instead --
worth remembering if you add pandas-conversion code elsewhere on this venv.

## Validation

`validation/validate_output.py` deliberately does **not** import the
pipeline's own cleaning code -- reusing it to "validate" its own output
would just confirm Spark did what Spark's code says it did, not provide
independent verification. Instead it:

1. Re-implements the label-count aggregate from scratch in pandas, against
   the smallest raw CSV file (a natural "small sample" of the full
   multi-file dataset).
2. Cross-checks those counts exactly against Spark's `label_counts` Delta
   table, filtered to that file's capture date.
3. Runs a cheap structural check across the *whole* raw dataset (line
   counting only, no full load into memory): cleaned rows in
   `network_events` must be <= total raw rows, since cleaning only ever
   drops rows.

This is the same "verify before trusting" discipline as the GPU Suite
project, applied here. It already earned its keep during development: it
caught a real bug where a stray embedded CSV header row (a known
CIC-IDS2017 concatenation artifact) survived cleaning because the filter
compared against an untrimmed literal.

## Delta Lake: schema evolution & time travel

`delta_demos/` runs against `data/delta_demo/network_events`, a throwaway
copy seeded from the real `network_events` table -- not the table itself,
since validation and the DAG assume that table only ever contains
pipeline output.

- **`schema_evolution.py`**: appends a batch with an extra
  `threat_intel_score` column. Shows the guardrail (append without
  `mergeSchema` fails with a schema-mismatch error) and the safe path
  (`mergeSchema=true` succeeds; old rows get `null` for the new column).
- **`time_travel.py`**: reads the table `versionAsOf` 0 vs. the latest
  version and proves the pre-evolution schema/row-count is exactly
  reconstructable, not just inferred from the current state.

Delete `data/delta_demo/` any time to reset both demos.

## Orchestration (Airflow)

A dedicated single-container Airflow (`apache/airflow:2.9.3-python3.11` +
Java 17 + PySpark + Delta, via `Dockerfile.airflow`) runs the DAG in
`dags/network_security_pipeline_dag.py`:

```
ingest_raw -> spark_transform -> validate_output -> write_delta_table
```

- **ingest_raw**: `scripts/download_dataset.sh` (idempotent).
- **spark_transform**: `python3 jobs/transform.py`.
- **validate_output**: `python3 validation/validate_output.py` -- fails
  the task (and halts the DAG) if the cross-check doesn't pass. This is
  the gate that decides whether a run is trustworthy enough to publish.
- **write_delta_table**: `dags/publish_manifest.py`, which only runs after
  validation passes, and records each Delta table's version + row count to
  `data/delta/_manifest.json` -- the "this run is published" marker. (The
  Spark step already writes the Delta tables themselves; re-writing them
  here would be redundant, so this step is the publish gate instead.)

```bash
docker compose build
docker compose up
# UI at http://localhost:8081 (8081, not 8080 -- this machine already runs
# an unrelated Airflow stack from another project on 8080). Credentials:
# docker exec network-security-airflow cat /opt/airflow/standalone_admin_password.txt
```

This machine also has a separate Airflow environment from another project
(GPU Suite, at `../gpu-benchmarking-suite/orchestration`, Airflow 3.3.1, no
Spark/Java installed). Reusing it would couple two unrelated projects and
require modifying infrastructure outside this repo, so this project ships
its own self-contained Airflow instead.

**Verified**: the Docker image builds cleanly, the DAG loads with no
import errors, and a full trigger runs `ingest_raw -> spark_transform ->
validate_output -> write_delta_table` inside the container end-to-end
against a synthetic fixture shaped like the real dataset (see "What was
actually tested" below). Building the custom Airflow image surfaced two
real Airflow gotchas along the way: `BashOperator.bash_command` is
Jinja-templated, and a command ending in `.sh`/`.bash` gets treated as a
template *file path* (`TemplateNotFound`) unless you add a trailing space;
and `docker compose up` on a machine with an existing Airflow stack needs
its own host port (hence 8081).

## Real-data findings

The pipeline was built and first verified against a small synthetic
fixture, then run against the actual ~885MB / 2.83M-row CIC-IDS2017
download. Three real bugs only showed up at real scale -- each passed
cleanly on the fixture and would have shipped broken without the
real-data run:

1. **Snappy/Gatekeeper crash.** Spark's default Parquet codec (snappy)
   ships as a native `.dylib` that gets extracted to a fresh `/tmp` path
   and `dlopen`'d on every JVM launch. Under the real write parallelism
   (many concurrent write tasks across `local[*]`'s cores -- not present
   with a handful of fixture rows), macOS Gatekeeper's library-validation
   policy intermittently blocked that freshly-extracted, non-Apple-signed
   file (`UnsatisfiedLinkError: library load disallowed by system
   policy`), crashing the whole JVM. Fixed in `jobs/spark_session.py` by
   switching to `spark.sql.parquet.compression.codec=gzip` (pure-JVM
   `java.util.zip`, no native library involved).
2. **Out-of-memory.** Local mode's default 1g driver memory isn't enough
   for real ~885MB CSVs read with `inferSchema=true` (a full extra read
   pass) plus `.cache()` on the whole cleaned dataset across 4 downstream
   aggregations (`SparkOutOfMemoryError: Unable to acquire ... memory`).
   Fixed by setting `spark.driver.memory=6g` and reducing
   `spark.sql.shuffle.partitions` from Spark's cluster-tuned default of
   200 down to the local core count -- also reduces the write parallelism
   that contributed to (1).
3. **Anomaly threshold silently zeroed out.** `anomaly_threshold_counts`
   came back with 0 rows on the real data, which is wrong by construction
   (a 99th-percentile threshold should always flag ~1% of benign flows).
   Two compounding causes: `clean()`'s "coerce non-finite values to null"
   step only checked `F.isnan()`, which does **not** catch `+/-Infinity`
   (a different IEEE 754 special value) -- ~1,777 real benign rows have
   `Flow Packets/s == Infinity` (zero-duration flows, a known CIC-IDS2017
   artifact). And separately, `approxQuantile`'s default
   `relativeError=0.01` was too coarse for this column's heavy tail (a
   couple of extreme outlier values pulled the approximate p99 all the
   way to the max, leaving nothing to exceed it). Fixed by nulling out
   `+/-Infinity` alongside NaN, and using `relativeError=0.0` (exact) for
   this one-time full-column stat -- negligible extra cost (~37s
   unchanged) for a materially different, and correct, result.

Final real-data output: `anomaly_threshold_counts` has 12 rows, threshold
1,000,000 packets/sec, and the distribution makes real security sense --
DoS Hulk (a high-packet-rate attack) dominates the flagged anomalies by
far, exactly the expected signal, followed by the ~1% benign tail and
smaller counts across PortScan/FTP-Patator/DDoS/etc.

Two more things worth knowing, neither a bug:
- The real CSVs have a genuine duplicate `Fwd Header Length` column in
  the header (a known CICFlowMeter quirk in this dataset). Spark logs a
  `WARN CSVHeaderChecker: CSV header does not conform to the schema` for
  every file except whichever one schema inference picks as canonical --
  harmless (Spark falls back to positional matching, confirmed correct by
  the final row counts) but produces a wall of scary-looking warnings on
  every run.
- `validation/validate_output.py` originally cross-checked Spark's output
  by filtering to the sample file's *capture date*. CIC-IDS2017's
  Thursday has **two** raw files (Morning-WebAttacks and
  Afternoon-Infiltration) that both fall back to the same date,
  2017-07-06, when a file has no parseable Timestamp. Date-filtering
  silently pulled in both files' rows on the Spark side while pandas only
  read one, producing a spurious mismatch that had nothing to do with the
  pipeline being wrong. Fixed by filtering on the exact `_source_file`
  instead of the derived date.

`approxQuantile`'s default tolerance and `F.isnan()`'s blind spot on
`Infinity` are both the same shape of mistake: an approximation or edge
case that looks fine on a small, well-behaved fixture and is silently
wrong at real scale. Worth treating as a standing question for any future
statistical/tolerance-based check in this pipeline, not just these two.

## What was actually tested vs. what wasn't

**Tested against the real dataset (2,830,743 flow records, all 8 files):**
- `jobs/transform.py` end to end: cleaning, all 4 aggregations (in this
  mirror, `src_ip_window_counts` correctly absent -- no source-IP column),
  writing to Delta.
- `validation/validate_output.py`: PASS, independent pandas cross-check
  exact-matches Spark on every label.
- Both Delta Lake demos (schema evolution guardrail + safe merge; time
  travel across versions) against the full 2.83M-row table.
- `dags/publish_manifest.py`, producing a real manifest with correct
  per-table Delta versions and row counts.

**Tested with real execution, but only against a synthetic fixture (not
re-run against the real 2.83M rows -- see below):**
- The full Airflow DAG, containerized, triggered end-to-end
  (`ingest_raw -> spark_transform -> validate_output -> write_delta_table`,
  including a real failed run against real Kaggle-credential checks and a
  real successful run once credentials were present).

This machine's Docker Desktop has only 7.65GB allocated (shared with the
unrelated GPU Suite Airflow stack already running on this machine), which
doesn't comfortably fit the 6g driver memory the real-data run needs
without risking an avoidable OOM crash in a resource-constrained VM.
Since the DAG's tasks are the same `python3 jobs/transform.py` /
`python3 validation/validate_output.py` commands already verified against
the real data on the host, re-running the identical code through Docker
specifically wasn't judged worth the resource risk. If you have more
memory allocated to Docker Desktop, `docker compose up` and triggering the
DAG should work the same way against real data as it already does against
the fixture.

**What a real production deployment would need that wasn't tested here:**
- A real cluster (EMR, Databricks, or Spark-on-Kubernetes) instead of
  `local[*]` -- this simulates distributed processing on one machine, it
  does not test actual multi-node behavior (shuffle over the network,
  executor failure/recovery, data skew across real partitions).
- Airflow with `CeleryExecutor`/`KubernetesExecutor` + Postgres instead of
  the single-container `standalone` command (SQLite + SequentialExecutor)
  used here -- fine for a demo DAG run manually once, not for concurrent
  or scheduled production runs.
- A secrets manager for the Kaggle token (and any real ingestion
  credentials) instead of a bind-mounted `~/.kaggle/kaggle.json`.
- Data larger than fits comfortably in local disk/memory on one machine.
  This dataset (~885MB, 2.83M rows) is genuinely large enough to justify
  Spark over pandas -- and large enough that it needed real memory/codec
  tuning to run reliably (see "Real-data findings" above) -- but Spark
  still held the whole cleaned dataset in a single machine's memory via
  `.cache()`. It does not stress-test genuine out-of-core / multi-node
  behavior (shuffle over the network, executor failure/recovery, data
  skew across real partitions) the way a real multi-GB/TB production
  workload on an actual cluster would.
