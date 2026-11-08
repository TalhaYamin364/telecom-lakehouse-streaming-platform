#!/usr/bin/env bash
# Smoke test for the Hadoop stack: 2 live DataNodes, a running NodeManager, and an HDFS write and read.
set -euo pipefail

compose=(docker compose -f docker/compose.hadoop.yml)
nn() { "${compose[@]}" exec -T namenode "$@"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

# Retry a check every 2s, up to N times, so slow registration is not mistaken for a failure.
retry() {
  local attempts="$1"; shift
  for ((i = 1; i <= attempts; i++)); do
    "$@" && return 0
    sleep 2
  done
  return 1
}
safemode_off() { [[ "$(nn hdfs dfsadmin -safemode get 2>/dev/null)" == *"OFF"* ]]; }
two_live_datanodes() {
  local report
  report="$(nn hdfs dfsadmin -report 2>/dev/null)"
  [[ "${report}" == *"Live datanodes (2)"* ]]
}
nodemanager_running() { [[ "$(nn yarn node -list 2>/dev/null)" == *RUNNING* ]]; }
fully_replicated() { [[ "$(nn hdfs fsck "${path}" -files -blocks 2>/dev/null)" == *"Live_repl=2"* ]]; }

path="/smoke/hello-$(date +%s).txt"
cleanup() { nn hdfs dfs -rm -f "${path}" > /dev/null 2>&1 || true; }
trap cleanup EXIT

echo "1/5 HDFS is out of safe mode"
retry 30 safemode_off || fail "HDFS is still in safe mode"

echo "2/5 two live DataNodes"
retry 15 two_live_datanodes || fail "expected 2 live DataNodes"

echo "3/5 YARN NodeManager"
retry 15 nodemanager_running || fail "no running NodeManager"

echo "4/5 write a file to HDFS and read it back"
payload="hello from the smoke test"
nn hdfs dfs -mkdir -p /smoke
printf '%s\n' "${payload}" | nn hdfs dfs -put -f - "${path}"
read_back="$(nn hdfs dfs -cat "${path}")"
[ "${read_back}" = "${payload}" ] || fail "read back '${read_back}', expected '${payload}'"

echo "5/5 replication across both DataNodes"
retry 15 fully_replicated || fail "block is not replicated twice"

echo "OK: HDFS and YARN are healthy"
