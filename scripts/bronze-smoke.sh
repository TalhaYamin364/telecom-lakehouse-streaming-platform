#!/usr/bin/env bash
# Smoke test for the bronze layer: every Kafka message is in lake.bronze.cdr_raw exactly once,
# including after new events arrive and after a rerun with nothing new.
# Needs both stacks up (see README). Takes about two minutes.
set -euo pipefail

cd "$(dirname "$0")/.."

kafka=(docker compose -f docker/compose.kafka.yml)
spark=(docker compose -f docker/compose.hadoop.yml --profile spark run --rm -T spark-client)
fail() { echo "FAIL: $1" >&2; exit 1; }

kafka_total() {
  "${kafka[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic 'cdr\.raw' \
    | awk -F: '{sum += $3} END {print sum + 0}'
}

# Prints "rows distinct_offsets" for the bronze table.
bronze_counts() {
  "${spark[@]}" bash -c '/opt/spark/bin/spark-submit --master "local[1]" --jars "$(ls /opt/extra-jars/*.jar | paste -sd, -)" \
    --driver-memory 512m /opt/spark_jobs/bronze_check.py' 2>&1 \
    | sed -n 's/^ROWS=\([0-9]*\) DISTINCT_OFFSETS=\([0-9]*\).*/\1 \2/p'
}

check() {
  local label="$1" total rows distinct
  total="$(kafka_total)"
  read -r rows distinct <<< "$(bronze_counts)"
  echo "    ${label}: kafka=${total} bronze_rows=${rows:-?} distinct_offsets=${distinct:-?}"
  [ "${rows:-}" = "${total}" ] || fail "${label}: bronze has ${rows:-no} rows but Kafka holds ${total} messages"
  [ "${distinct:-}" = "${rows}" ] || fail "${label}: ${rows} rows but only ${distinct:-no} distinct offsets, so something was written twice"
}

generate() {
  "${kafka[@]}" --profile generator run --rm -T generator --count "$1" --seed "${RANDOM}${RANDOM}" 2>&1 | grep -E '^(sent|attempted|FAIL)'
}

echo "1/4 generate 1000 events and ingest them (on YARN)"
generate 1000
scripts/bronze-stream.sh --once > /dev/null 2>&1 || fail "bronze job failed (run scripts/bronze-stream.sh --once to see why)"
check "first load"

echo "2/4 generate 500 more events and ingest again: only the new ones are added"
generate 500
scripts/bronze-stream.sh --once > /dev/null 2>&1 || fail "bronze job failed on the second run"
check "second load"

echo "3/4 rerun with nothing new: nothing changes"
scripts/bronze-stream.sh --once > /dev/null 2>&1 || fail "bronze job failed on the rerun"
check "rerun"

echo "4/4 the table is partitioned by day in HDFS"
docker compose -f docker/compose.hadoop.yml exec -T namenode hdfs dfs -ls /warehouse/bronze/cdr_raw/data | grep -q 'kafka_timestamp_day=' \
  || fail "no kafka_timestamp_day= partition folders under /warehouse/bronze/cdr_raw/data"

echo "OK: bronze matches Kafka exactly, with no loss and no duplicates"
