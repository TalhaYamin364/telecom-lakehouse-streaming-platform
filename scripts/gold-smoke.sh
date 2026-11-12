#!/usr/bin/env bash
# Smoke test for the gold layer, from an empty lake (it runs scripts/lake-reset.sh first, so it deletes local data).
# Checks: gold reconciles with silver, there is one row per key, a rerun and a replay change nothing, late and corrected
# events recompute only the days they touch, and --day recomputes one day and leaves every other day alone.
# Needs both stacks up (see README). Takes about five minutes.
set -euo pipefail

# shellcheck source=/dev/null
source "$(dirname "$0")/smoke-lib.sh"
spark=(docker compose -f docker/compose.hadoop.yml --profile spark run --rm -T spark-client)

# shellcheck disable=SC2016  # single-quoted on purpose: $(ls ...) must expand inside the container
gold_check() {
  "${spark[@]}" bash -c '/opt/spark/bin/spark-submit --master "local[1]" --jars "$(ls /opt/extra-jars/*.jar | paste -sd, -)" \
    --driver-memory 768m /opt/spark_jobs/gold_check.py' 2>&1 | grep -E '^(SILVER|USAGE|REVENUE|CELL|DUPLICATE_KEYS|DAYS)'
}

gold() {  # run the gold job on YARN with the given arguments and print its one-line result
  scripts/spark-yarn.sh gold_daily.py "$@" 2>&1 | grep -E '^gold:|nothing to do' || fail "gold job failed (run scripts/spark-yarn.sh gold_daily.py to see why)"
}

computed_at() {  # computed_at <DAYS value> <day>: when that day was last computed
  tr ',' '\n' <<< "$1" | sed -n "s/^$2://p"
}

reconcile() {  # reconcile <label> <report>: every gold total equals the same total over silver
  local label="$1" report="$2" name
  for name in ROWS CHARGE VOICE SMS BYTES CALLS TEXTS SESSIONS; do
    expect "${label}" "USAGE_${name}" "$(value "USAGE_${name}" "${report}")" "$(value "SILVER_${name}" "${report}")"
  done
  expect "${label}" REVENUE_ROWS "$(value REVENUE_ROWS "${report}")" "$(value SILVER_ROWS "${report}")"
  expect "${label}" REVENUE_CHARGE "$(value REVENUE_CHARGE "${report}")" "$(value SILVER_CHARGE "${report}")"
  for name in ROWS CHARGE VOICE SMS BYTES; do
    expect "${label}" "CELL_${name}" "$(value "CELL_${name}" "${report}")" "$(value "SILVER_${name}" "${report}")"
  done
  expect "${label}" DUPLICATE_KEYS "$(value DUPLICATE_KEYS "${report}")" 0
  expect "${label}" TYPE_MISMATCH "$(value TYPE_MISMATCH "${report}")" 0
  expect "${label}" SUBSCRIBER_MISMATCH "$(value SUBSCRIBER_MISMATCH "${report}")" 0
}

echo "1/7 start from an empty lake"
scripts/lake-reset.sh > /dev/null

echo "2/7 generate 1000 events and one event to be corrected later, build bronze and silver, then gold"
total="$(generate 1000 8201)"
fix_time_original
ingest
gold
report="$(gold_check)"; grep -E '^(SILVER_ROWS|DAYS)' <<< "${report}" | cut -c1-200 | indent
expect "first build" SILVER_ROWS "$(value SILVER_ROWS "${report}")" "$((total + 1))"
reconcile "first build" "${report}"
days1="$(value DAYS "${report}")"

echo "3/7 rerun gold with nothing new: nothing is recomputed"
gold | grep -q 'nothing to do' || fail "gold recomputed days although silver did not change"
expect "rerun" DAYS "$(value DAYS "$(gold_check)")" "${days1}"

echo "4/7 replay the same events: silver does not change, so gold does not either"
generate 1000 8201 > /dev/null
ingest
gold | grep -q 'nothing to do' || fail "gold recomputed days after a replay that changed nothing"
expect "replay" DAYS "$(value DAYS "$(gold_check)")" "${days1}"

echo "5/7 late events from two days earlier arrive and one event is corrected onto another day: only the touched days are recomputed"
generate 500 8202 2025-12-29T00:00:00Z > /dev/null
fix_time_correction
ingest
gold
report="$(gold_check)"; value DAYS "${report}" | tr ',' '\n' | indent
reconcile "late and corrected events" "${report}"
days2="$(value DAYS "${report}")"
expect "untouched day" "computed_at of 2025-12-31" "$(computed_at "${days2}" 2025-12-31)" "$(computed_at "${days1}" 2025-12-31)"
[ "$(computed_at "${days2}" 2026-01-01)" != "$(computed_at "${days1}" 2026-01-01)" ] \
  || fail "2026-01-01 was not recomputed although the corrected event moved off it"
[ -n "$(computed_at "${days2}" 2025-12-29)" ] || fail "no gold rows for 2025-12-29 after the late events"

echo "6/7 backfill one day with --day: that day is recomputed, every other day is left alone, nothing is duplicated"
gold --day 2025-12-29
report="$(gold_check)"
reconcile "backfill" "${report}"
days3="$(value DAYS "${report}")"
[ "$(computed_at "${days3}" 2025-12-29)" != "$(computed_at "${days2}" 2025-12-29)" ] || fail "--day 2025-12-29 did not recompute that day"
for day in 2025-12-28 2025-12-31 2026-01-01; do
  expect "backfill leaves other days alone" "computed_at of ${day}" "$(computed_at "${days3}" "${day}")" "$(computed_at "${days2}" "${day}")"
done

echo "7/7 the tables are partitioned by day in HDFS"
listing="$(docker compose -f docker/compose.hadoop.yml exec -T namenode hdfs dfs -ls /warehouse/gold/daily_usage_per_subscriber/data)"  # not piped into grep -q: it exits early and the pipe breaks
grep -q 'usage_date=2025-12-29' <<< "${listing}" || fail "no usage_date=2025-12-29 folder under /warehouse/gold/daily_usage_per_subscriber/data"

echo "OK: gold reconciles with silver, reruns and replays change nothing, late and corrected events recompute only their days, and --day backfills one day safely"