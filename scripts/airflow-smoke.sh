#!/usr/bin/env bash
# Smoke test for the Airflow DAGs, from an empty lake (it runs scripts/lake-reset.sh first, so it deletes local data).
# Checks: the DAGs load and the one-slot pool exists; a scheduled run builds bronze, silver and gold in order; reruns
# change nothing; two runs at once never overlap; the backfill DAG recomputes only the day it is given and refuses a
# bad parameter.
# Needs the stack with Airflow up: scripts/stack.sh start --airflow. Takes about ten minutes.
set -euo pipefail

# shellcheck source=/dev/null
source "$(dirname "$0")/smoke-lib.sh"

airflow_compose=(docker compose -f docker/compose.hadoop.yml --profile spark --profile airflow)
af() { "${airflow_compose[@]}" exec -T airflow airflow "$@"; }

runs() {  # runs <dag>: one "run_id state" line per run, newest first
  af dags list-runs "$1" -o json 2> /dev/null | python3 -c '
import json, sys
for run in json.load(sys.stdin):
    print(run["run_id"], run["state"])'
}

run_ids() { runs "$1" | cut -d' ' -f1; }  # run_ids <dag>

count_state() { grep -c " $2\$" <<< "$(runs "$1")" || true; }  # count_state <dag> <state>

wait_for_new_run() {  # wait_for_new_run <dag> <run_id prefix> <run ids that already existed> <seconds>: a new such run succeeds
  local dag="$1" prefix="$2" before="$3" deadline=$((SECONDS + $4)) id state
  while (( SECONDS < deadline )); do
    while read -r id state; do
      grep -qxF "${id}" <<< "${before}" && continue
      case "${id}:${state}" in
        "${prefix}"*:success) echo "${id} ${state}"; return 0 ;;
        "${prefix}"*:failed) fail "${dag} run failed: ${id}" ;;
      esac
    done <<< "$(runs "${dag}")"
    sleep 5
  done
  fail "no new ${prefix} run of ${dag} succeeded within $4 seconds"
}

# Pausing a DAG also stops the scheduler from advancing runs already in flight, so the test never pauses to wait:
# it waits for the running count to reach zero and only then pauses.
wait_not_running() {  # wait_not_running <dag> <seconds>
  local deadline=$((SECONDS + $2))
  while (( SECONDS < deadline )); do
    [ "$(count_state "$1" running)" -eq 0 ] && return 0
    sleep 3
  done
  fail "$1 still has a run in progress after $2 seconds"
}

echo "1/7 the DAGs load, and the one-slot pool exists"
scripts/stack.sh health > /dev/null || fail "the stack is not healthy; run scripts/stack.sh health"
for dag in lakehouse_batch lakehouse_backfill; do
  af dags list 2> /dev/null | grep -q "${dag}" || fail "DAG ${dag} is not loaded"
done
af dags list-import-errors 2>&1 | grep -q 'No data found' || fail "a DAG file has an import error"
af pools get lake_writer 2> /dev/null | grep -qE 'lake_writer +\| 1 ' || fail "the lake_writer pool does not have exactly one slot"

echo "2/7 a scheduled run builds bronze, silver and gold in order (the schedule is set to every 2 minutes for this test)"
scripts/lake-reset.sh > /dev/null
total="$(generate 600 9301)"
before="$(run_ids lakehouse_batch)"
# Recreate Airflow with a short schedule so a run starts by itself; the DAG is unpaused only now.
LAKEHOUSE_SCHEDULE='*/2 * * * *' "${airflow_compose[@]}" up -d --wait airflow > /dev/null 2>&1
af dags unpause lakehouse_batch > /dev/null 2>&1
first="$(wait_for_new_run lakehouse_batch scheduled__ "${before}" 480)"; echo "${first}" | indent
run_id="${first%% *}"
order="$(af tasks states-for-dag-run lakehouse_batch "${run_id}" 2> /dev/null | grep -E '\| (bronze|silver|gold) +\|' | sed 's/ //g' | cut -d'|' -f3,5,6 | sort -t'|' -k2)"
expect "task order" tasks "$(cut -d'|' -f1 <<< "${order}" | tr '\n' ' ')" "bronze silver gold "
wait_not_running lakehouse_batch 300
report="$(gold_check)"; grep -E '^(SILVER_ROWS|DAYS)' <<< "${report}" | cut -c1-200 | indent
expect "scheduled run" SILVER_ROWS "$(value SILVER_ROWS "${report}")" "${total}"
reconcile "scheduled run" "${report}"
days1="$(value DAYS "${report}")"

echo "3/7 two runs triggered at once, while the schedule keeps starting runs: they never overlap, all succeed, nothing changes"
before="$(run_ids lakehouse_batch)"
af dags trigger lakehouse_batch > /dev/null 2>&1
af dags trigger lakehouse_batch > /dev/null 2>&1
deadline=$((SECONDS + 720)); most_running=0; manual_done=0
while (( SECONDS < deadline )); do
  running="$(count_state lakehouse_batch running)"
  [ "${running}" -le "${most_running}" ] || most_running="${running}"
  manual_done="$(runs lakehouse_batch | grep '^manual__' | grep ' success$' | cut -d' ' -f1 | grep -cvxF "${before}" || true)"
  [ "${manual_done}" -lt 2 ] || break
  sleep 3
done
echo "    most runs running at once: ${most_running}; new manual runs finished: ${manual_done}"
[ "${most_running}" -ge 1 ] || fail "no run was ever seen running, so the overlap check proved nothing"
[ "${most_running}" -le 1 ] || fail "${most_running} runs of lakehouse_batch ran at the same time"
[ "${manual_done}" -ge 2 ] || fail "the two manual runs did not both succeed"
wait_not_running lakehouse_batch 300
report="$(gold_check)"
expect "reruns" DAYS "$(value DAYS "${report}")" "${days1}"
reconcile "reruns" "${report}"

echo "4/7 the backfill DAG recomputes only the day it is given"
backfill_day="$(cut -d: -f1 <<< "$(tr ',' '\n' <<< "${days1}" | head -1)")"
other_day="$(cut -d: -f1 <<< "$(tr ',' '\n' <<< "${days1}" | tail -1)")"
[ "${backfill_day}" != "${other_day}" ] || fail "the test data covers a single day, so a backfill cannot be told apart"
before="$(run_ids lakehouse_backfill)"
af dags unpause lakehouse_backfill > /dev/null 2>&1  # a no-op on a fresh install, where it starts unpaused
af dags trigger lakehouse_backfill --conf "{\"days\": \"${backfill_day}\"}" > /dev/null 2>&1
wait_for_new_run lakehouse_backfill manual__ "${before}" 480 | indent
wait_not_running lakehouse_batch 300
report="$(gold_check)"
days2="$(value DAYS "${report}")"
[ "$(computed_at "${days2}" "${backfill_day}")" != "$(computed_at "${days1}" "${backfill_day}")" ] || fail "the backfill did not recompute ${backfill_day}"
expect "backfill leaves other days alone" "computed_at of ${other_day}" "$(computed_at "${days2}" "${other_day}")" "$(computed_at "${days1}" "${other_day}")"
reconcile "backfill" "${report}"

echo "5/7 the backfill DAG refuses a parameter that is not a list of dates"
if af dags trigger lakehouse_backfill --conf '{"days": "2026-01-01; rm -rf /"}' > /dev/null 2>&1; then
  fail "the backfill DAG accepted a parameter that is not a date"
fi

echo "6/7 leave Airflow on its normal schedule, with the batch DAG paused (only once nothing is running)"
wait_not_running lakehouse_batch 300
af dags pause lakehouse_batch > /dev/null 2>&1
"${airflow_compose[@]}" up -d --wait airflow > /dev/null 2>&1  # recreated without the test schedule; the paused state is kept

echo "7/7 the schedule in the DAG file is the normal one"
grep -q '"\*/15 \* \* \* \*"' airflow/dags/lakehouse_batch.py || fail "the default schedule in lakehouse_batch.py is not every 15 minutes"

echo "OK: Airflow runs bronze, silver and gold on a schedule, in order, one run at a time, reruns change nothing, and the backfill DAG recomputes only its day"
