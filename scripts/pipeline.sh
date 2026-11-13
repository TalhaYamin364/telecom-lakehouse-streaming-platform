#!/usr/bin/env bash
# Run the batch pipeline by hand, in order: bronze (what is in Kafka now), then silver, then gold.
# The Airflow DAG lakehouse_batch runs the same three jobs on a schedule. Do not run this while an Airflow run is in
# progress: the lake has one writer at a time.
set -euo pipefail

cd "$(dirname "$0")/.."

step() {  # step <label> <pattern of the result lines to show, or empty to show none> <command...>
  local label="$1" pattern="$2" out
  shift 2
  echo "== ${label}"
  out="$("$@" 2>&1)" || { tail -30 <<< "${out}" >&2; echo "FAIL: ${label} failed" >&2; exit 1; }
  [ -z "${pattern}" ] || grep -E "${pattern}" <<< "${out}" || true
}

step "bronze: Kafka to Iceberg" '' scripts/bronze-stream.sh --once
step "silver: clean, deduplicate, mask" '^silver:|nothing to do|empty' scripts/spark-yarn.sh silver_cdr.py
step "gold: daily KPIs" '^gold:|nothing to do' scripts/spark-yarn.sh gold_daily.py
echo "OK: pipeline finished"
