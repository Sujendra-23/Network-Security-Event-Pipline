#!/usr/bin/env bash
# Downloads the CIC-IDS2017 dataset into data/raw/.
#
# The official UNB source (https://www.unb.ca/cic/datasets/ids-2017.html)
# requires filling out a request form to get a download link, so this
# project uses the well-known Kaggle mirror instead:
#   https://www.kaggle.com/datasets/chethuhn/network-intrusion-dataset
# (the "MachineLearningCVE" CICFlowMeter export: 8 daily CSVs, ~225MB
# zipped / ~650MB unzipped, no Source/Destination IP columns -- see
# jobs/schema.py for how the pipeline adapts when IP columns are absent).
#
# Requires Kaggle credentials, either:
#   ~/.kaggle/kaggle.json      (legacy username+key)
#   ~/.kaggle/access_token     (newer API token, plain text)
#   $KAGGLE_API_TOKEN          (env var, token or path to a token file)
# Get one at https://www.kaggle.com/settings -> API -> Create New Token
#
# Usage:
#   source scripts/env.sh   # activates the venv that has the kaggle CLI
#   scripts/download_dataset.sh

set -euo pipefail

PROJECT_ROOT="$(pwd)"
RAW_DIR="${DATA_RAW_DIR:-$PROJECT_ROOT/data/raw}"
KAGGLE_DATASET="chethuhn/network-intrusion-dataset"

if [ ! -f "jobs/transform.py" ]; then
    echo "Run this from the project root (cd there first)." >&2
    exit 1
fi

if ls "$RAW_DIR"/*.csv >/dev/null 2>&1; then
    echo "data/raw already has CSV files -- skipping download (idempotent)."
    echo "Delete data/raw/*.csv first if you want to re-download."
    exit 0
fi

if [ -f "$HOME/.kaggle/kaggle.json" ]; then
    chmod 600 "$HOME/.kaggle/kaggle.json"
elif [ -f "$HOME/.kaggle/access_token" ]; then
    chmod 600 "$HOME/.kaggle/access_token"
elif [ -n "${KAGGLE_API_TOKEN:-}" ]; then
    : # credential supplied via env var, nothing to check on disk
else
    echo "Missing Kaggle credentials (checked $HOME/.kaggle/kaggle.json, $HOME/.kaggle/access_token, \$KAGGLE_API_TOKEN)." >&2
    echo "Get one at https://www.kaggle.com/settings -> API -> Create New Token" >&2
    exit 1
fi

mkdir -p "$RAW_DIR"
kaggle datasets download --dataset "$KAGGLE_DATASET" --path "$RAW_DIR" --unzip

echo "Downloaded to $RAW_DIR:"
ls -la "$RAW_DIR"/*.csv
