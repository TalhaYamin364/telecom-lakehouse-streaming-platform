#!/usr/bin/env bash
# Smoke test for the silver layer, from an empty lake (it runs scripts/lake-reset.sh first, so it deletes local data).
# Checks: one row per event, corrections win, bad records are quarantined, no raw phone number reaches silver,
# a rerun and a replay change nothing, and events from an earlier day land in that day's partition.
# Needs both stacks up (see README). Takes about four minutes.
set -euo pipefail

cd "$(dirname "$0")/.."

kafka=(docker compose -f docker/compose.kafka.yml)
spark=(docker compose -f docker/compose.hadoop.yml --profile spark run --rm -T spark-client)
fail() { echo "FAIL: $1" >&2; exit 1; }
indent() { sed 's/^/    /'; }

generate() {  # generate <count> <seed> [start]; prints how many distinct events were created
  local out args=(--count "$1" --seed "$2")
  [ -z "${3:-}" ] || args+=(--start "$3")
  out="$("${kafka[@]}" --profile generator run --rm -T generator "${args[@]}" 2>&1)"
  grep -E '^sent' <<< "${out}" >&2 || fail "generator failed: ${out}"
  sed -n 's/^sent to cdr.raw: \([0-9]*\) new.*/\1/p' <<< "${out}"
}

inject_bad_records() {  # four records that must be quarantined, each breaking a different rule
  printf '%s\n' \
    'this is not json' \
    '{"event_id":"bad-msisdn","event_type":"VOICE","msisdn":"+8801234","counterparty_msisdn":"+88010000000001","cell_id":"DHK-0001","event_time":"2026-01-01T00:00:00.000Z","emitted_at":"2026-01-01T00:00:01.000Z","duration_sec":10,"charge_paisa":20}' \
    '{"event_id":"bad-duration","event_type":"VOICE","msisdn":"+8801000000001","counterparty_msisdn":"+8801000000002","cell_id":"DHK-0001","event_time":"2026-01-01T00:00:00.000Z","emitted_at":"2026-01-01T00:00:01.000Z","duration_sec":-5,"charge_paisa":20}' \
    '{"event_id":"bad-type","event_type":"FAX","msisdn":"+8801000000001","cell_id":"DHK-0001","event_time":"2026-01-01T00:00:00.000Z","emitted_at":"2026-01-01T00:00:01.000Z","charge_paisa":20}' \
    | "${kafka[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic cdr.raw > /dev/null
}

# shellcheck disable=SC2016  # single-quoted on purpose: $(ls ...) must expand inside the container
silver_check() {
  "${spark[@]}" bash -c '/opt/spark/bin/spark-submit --master "local[1]" --jars "$(ls /opt/extra-jars/*.jar | paste -sd, -)" \
    --driver-memory 768m /opt/spark_jobs/silver_check.py' 2>&1 | grep -E '^(SILVER_ROWS|SUBSCRIBERS|REJECTED)'
}

value() {  # value KEY "text with KEY=value pairs"
  grep -oE "(^| )$1=[^ ]*" <<< "$2" | head -1 | sed 's/.*=//'
}

ingest() {  # bronze from Kafka, then silver, both on YARN
  scripts/bronze-stream.sh --once > /dev/null 2>&1 || fail "bronze job failed"
  scripts/spark-yarn.sh silver_cdr.py 2>&1 | grep -E '^silver:|nothing to do' || fail "silver job failed (run scripts/spark-yarn.sh silver_cdr.py to see why)"
}

expect() {  # expect <label> <name> <actual> <wanted>
  [ "$3" = "$4" ] || fail "$1: $2 is '$3', expected '$4'"
}

echo "1/6 start from an empty lake"
scripts/lake-reset.sh > /dev/null

echo "2/6 generate 1000 events and 4 bad records, then build bronze and silver"
total="$(generate 1000 7001)"
inject_bad_records
ingest
report="$(silver_check)"; indent <<< "${report}"
expect "first build" SILVER_ROWS "$(value SILVER_ROWS "${report}")" "${total}"
expect "first build" DISTINCT_IDS "$(value DISTINCT_IDS "${report}")" "${total}"
expect "first build" LEAKED_NUMBERS "$(value LEAKED_NUMBERS "${report}")" 0
expect "first build" LATEST_MISMATCH "$(value LATEST_MISMATCH "${report}")" 0
expect "first build" REJECTED "$(value REJECTED "${report}")" 4
[ "$(value CORRECTED_IDS "${report}")" -gt 0 ] || fail "no corrected events were generated, so latest-wins was not tested"

echo "3/6 rerun silver with nothing new: nothing changes"
ingest
expect "rerun" report "$(silver_check)" "${report}"

echo "4/6 replay the same events (same seed): bronze grows, silver does not"
generate 1000 7001 > /dev/null
ingest
expect "replay" SILVER_ROWS "$(value SILVER_ROWS "$(silver_check)")" "${total}"

echo "5/6 events from two days earlier arrive late: they land in that day's partition, silver has no duplicates"
more="$(generate 500 7002 2025-12-29T00:00:00Z)"
ingest
report="$(silver_check)"; indent <<< "${report}"
expect "late events" SILVER_ROWS "$(value SILVER_ROWS "${report}")" "$((total + more))"
expect "late events" DISTINCT_IDS "$(value DISTINCT_IDS "${report}")" "$((total + more))"
expect "late events" LATEST_MISMATCH "$(value LATEST_MISMATCH "${report}")" 0
grep -q '2025-12-29' <<< "$(value DAYS "${report}")" || fail "no 2025-12-29 partition after the late events: $(value DAYS "${report}")"

echo "6/6 the table is partitioned by event day in HDFS"
listing="$(docker compose -f docker/compose.hadoop.yml exec -T namenode hdfs dfs -ls /warehouse/silver/cdr/data)"  # not piped into grep -q: it exits early and the pipe breaks
grep -q 'event_time_day=2025-12-29' <<< "${listing}" \
  || fail "no event_time_day=2025-12-29 folder under /warehouse/silver/cdr/data"

echo "OK: silver has one row per event, the latest copy wins, bad records are quarantined, no phone numbers leak, reruns and replays change nothing"
