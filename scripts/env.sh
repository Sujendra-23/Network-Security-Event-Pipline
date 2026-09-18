# Source this before running any Spark job or script in this project:
#   source scripts/env.sh
#
# Spark 3.5 / Hadoop's auth code breaks on Java 24 (the machine's system
# default JDK) with `UnsupportedOperationException: getSubject is not
# supported`. We pin JAVA_HOME to a Homebrew-installed Java 17 for this
# project only -- the system default Java is left untouched.

# Must be sourced from the project root (not resolved via BASH_SOURCE,
# which behaves inconsistently across bash/zsh):
#   cd "/path/to/Network Security Event Pipline" && source scripts/env.sh
if [ ! -f "jobs/transform.py" ]; then
    echo "env.sh must be sourced from the project root (cd there first)" >&2
    return 1 2>/dev/null || exit 1
fi
PROJECT_ROOT="$(pwd)"

export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"

source "$PROJECT_ROOT/.venv/bin/activate"

export PROJECT_ROOT
export DATA_RAW_DIR="$PROJECT_ROOT/data/raw"
export DATA_DELTA_DIR="$PROJECT_ROOT/data/delta"
