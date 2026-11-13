#!/usr/bin/env bash
# Recompute gold for chosen days. Safe to run again: each day is rebuilt from silver and replaces its old rows in one
# atomic step, and every other day is left alone. Do not run this while an Airflow run is in progress.
# Usage: scripts/backfill.sh YYYY-MM-DD [YYYY-MM-DD ...]
set -euo pipefail

cd "$(dirname "$0")/.."

[ $# -gt 0 ] || { echo "usage: scripts/backfill.sh YYYY-MM-DD [YYYY-MM-DD ...]" >&2; exit 2; }

args=()
for day in "$@"; do
  python3 -c "import datetime, sys; datetime.date.fromisoformat(sys.argv[1])" "${day}" 2> /dev/null \
    || { echo "not a date (use YYYY-MM-DD): ${day}" >&2; exit 2; }
  args+=(--day "${day}")
done

out="$(scripts/spark-yarn.sh gold_daily.py "${args[@]}" 2>&1)" || { tail -30 <<< "${out}" >&2; echo "FAIL: backfill failed" >&2; exit 1; }
grep -E '^gold:' <<< "${out}"
