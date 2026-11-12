#!/usr/bin/env bash
# Smoke test for the silver layer, from an empty lake (it runs scripts/lake-reset.sh first, so it deletes local data).
# Checks: one row per event, corrections win, bad records are quarantined, no raw phone number reaches silver,
# a rerun and a replay change nothing, and events from an earlier day land in that day's partition.
# Needs both stacks up (see README). Takes about four minutes.
set -euo pipefail

# shellcheck source=/dev/null
source "$(dirname "$0")/smoke-lib.sh"
spark=(docker compose -f docker/compose.hadoop.yml --profile spark run --rm -T spark-client)

inject_bad_records() {  # six records that must be quarantined, each breaking a different rule
  produce_raw \
    'this is not json' \
    '{"event_id":"+8801000000009","event_type":"DATA","msisdn":"+8801000000001","cell_id":"DHK-0001","event_time":"2026-01-01T00:00:00.000Z","emitted_at":"2026-01-01T00:00:01.000Z","bytes_up":1,"bytes_down":1,"charge_paisa":1}' \
    '{"event_id":"bad-msisdn","event_type":"VOICE","msisdn":"+8801234","counterparty_msisdn":"+88010000000001","cell_id":"DHK-0001","event_time":"2026-01-01T00:00:00.000Z","emitted_at":"2026-01-01T00:00:01.000Z","duration_sec":10,"charge_paisa":20}' \
    '{"event_id":"bad-duration","event_type":"VOICE","msisdn":"+8801000000001","counterparty_msisdn":"+8801000000002","cell_id":"DHK-0001","event_time":"2026-01-01T00:00:00.000Z","emitted_at":"2026-01-01T00:00:01.000Z","duration_sec":-5,"charge_paisa":20}' \
    '{"event_id":"bad-type","event_type":"FAX","msisdn":"+8801000000001","cell_id":"DHK-0001","event_time":"2026-01-01T00:00:00.000Z","emitted_at":"2026-01-01T00:00:01.000Z","charge_paisa":20}' \
    '{"event_id":"bad-cell","event_type":"DATA","msisdn":"+8801000000001","cell_id":"  ","event_time":"2026-01-01T00:00:00.000Z","emitted_at":"2026-01-01T00:00:01.000Z","bytes_up":1,"bytes_down":1,"charge_paisa":1}'
}

# shellcheck disable=SC2016  # single-quoted on purpose: $(ls ...) must expand inside the container
silver_check() {
  "${spark[@]}" bash -c '/opt/spark/bin/spark-submit --master "local[1]" --jars "$(ls /opt/extra-jars/*.jar | paste -sd, -)" \
    --driver-memory 768m /opt/spark_jobs/silver_check.py' 2>&1 | grep -E '^(SILVER_ROWS|FIXTIME_ROWS|SUBSCRIBERS|REJECTED)'
}

echo "1/6 start from an empty lake"
scripts/lake-reset.sh > /dev/null

echo "2/6 generate 1000 events, 6 bad records and one event to be corrected later, then build bronze and silver"
total="$(generate 1000 7001)"
inject_bad_records
fix_time_original
ingest
report="$(silver_check)"; indent <<< "${report}"
expect "first build" SILVER_ROWS "$(value SILVER_ROWS "${report}")" "$((total + 1))"
expect "first build" DISTINCT_IDS "$(value DISTINCT_IDS "${report}")" "$((total + 1))"
expect "first build" LEAKED_NUMBERS "$(value LEAKED_NUMBERS "${report}")" 0
expect "first build" LATEST_MISMATCH "$(value LATEST_MISMATCH "${report}")" 0
expect "first build" REJECTED "$(value REJECTED "${report}")" 6
expect "first build" FIXTIME_CHARGE "$(value FIXTIME_CHARGE "${report}")" 10
[ "$(value CORRECTED_IDS "${report}")" -gt 0 ] || fail "no corrected events were generated, so latest-wins was not tested"

echo "3/6 rerun silver with nothing new: nothing changes"
ingest
expect "rerun" report "$(silver_check)" "${report}"

echo "4/6 replay the same events (same seed): bronze grows, silver does not"
generate 1000 7001 > /dev/null
ingest
expect "replay" SILVER_ROWS "$(value SILVER_ROWS "$(silver_check)")" "$((total + 1))"

echo "5/6 events from two days earlier arrive late, and the 'fix-time' event is corrected with a different event_time"
more="$(generate 500 7002 2025-12-29T00:00:00Z)"
fix_time_correction
ingest
report="$(silver_check)"; indent <<< "${report}"
expect "late events" SILVER_ROWS "$(value SILVER_ROWS "${report}")" "$((total + more + 1))"
expect "late events" DISTINCT_IDS "$(value DISTINCT_IDS "${report}")" "$((total + more + 1))"
expect "late events" LATEST_MISMATCH "$(value LATEST_MISMATCH "${report}")" 0
expect "late events" LEAKED_NUMBERS "$(value LEAKED_NUMBERS "${report}")" 0
expect "corrected event_time" FIXTIME_ROWS "$(value FIXTIME_ROWS "${report}")" 1
expect "corrected event_time" FIXTIME_CHARGE "$(value FIXTIME_CHARGE "${report}")" 99
expect "corrected event_time" FIXTIME_DAY "$(value FIXTIME_DAY "${report}")" 2025-12-29
grep -q '2025-12-29' <<< "$(value DAYS "${report}")" || fail "no 2025-12-29 partition after the late events: $(value DAYS "${report}")"

echo "6/6 the table is partitioned by event day in HDFS"
listing="$(docker compose -f docker/compose.hadoop.yml exec -T namenode hdfs dfs -ls /warehouse/silver/cdr/data)"  # not piped into grep -q: it exits early and the pipe breaks
grep -q 'event_time_day=2025-12-29' <<< "${listing}" \
  || fail "no event_time_day=2025-12-29 folder under /warehouse/silver/cdr/data"

echo "OK: silver has one row per event, the latest copy wins, bad records are quarantined, no phone numbers leak, reruns and replays change nothing"
