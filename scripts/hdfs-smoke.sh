#!/usr/bin/env bash
# Smoke test for the Hadoop stack: 2 live DataNodes, a running NodeManager, and an HDFS write and read.
set -euo pipefail

compose=(docker compose -f docker/compose.hadoop.yml)
nn() { "${compose[@]}" exec -T namenode "$@"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

echo "1/4 live DataNodes"
live="$(nn hdfs dfsadmin -report | awk '/^Live datanodes/ {gsub(/[^0-9]/, "", $3); print $3}')"
[ "${live}" = "2" ] || fail "expected 2 live DataNodes, got '${live}'"

echo "2/4 YARN NodeManager"
nodes="$(nn yarn node -list 2>/dev/null)"
grep -q RUNNING <<< "${nodes}" || fail "no running NodeManager"

echo "3/4 write a file to HDFS and read it back"
path="/smoke/hello-$(date +%s).txt"
payload="hello from the smoke test"
nn hdfs dfs -mkdir -p /smoke
printf '%s\n' "${payload}" | nn hdfs dfs -put -f - "${path}"
read_back="$(nn hdfs dfs -cat "${path}")"
[ "${read_back}" = "${payload}" ] || fail "read back '${read_back}', expected '${payload}'"

echo "4/4 replication across both DataNodes"
fsck="$(nn hdfs fsck "${path}" -files -blocks 2>/dev/null)"
grep -q 'Live_repl=2' <<< "${fsck}" || fail "block is not replicated twice"

nn hdfs dfs -rm -f "${path}" > /dev/null
echo "OK: HDFS and YARN are healthy"
