#!/usr/bin/env bash
# Smoke test for Spark on YARN: run the bundled SparkPi in cluster mode and check YARN reports it SUCCEEDED.
set -euo pipefail

compose=(docker compose -f docker/compose.hadoop.yml)
fail() { echo "FAIL: $1" >&2; exit 1; }

echo "1/2 submit SparkPi to YARN (cluster mode)"
# shortcut: Spark uploads its jars to HDFS on every submit; set spark.yarn.archive once that gets slow.
out="$("${compose[@]}" --profile spark run --rm -T spark-client /opt/spark/bin/spark-submit \
  --master yarn --deploy-mode cluster \
  --driver-memory 512m --executor-memory 512m --num-executors 1 \
  --class org.apache.spark.examples.SparkPi \
  file:///opt/spark/examples/jars/spark-examples.jar 10 2>&1)" || { echo "${out}" | tail -30 >&2; fail "spark-submit failed"; }

[[ "${out}" =~ (application_[0-9]+_[0-9]+) ]] || fail "no YARN application id in the spark-submit output"
app_id="${BASH_REMATCH[1]}"

echo "2/2 YARN reports ${app_id} as SUCCEEDED"
status="$("${compose[@]}" exec -T resourcemanager yarn application -status "${app_id}" 2>/dev/null)"
grep -q 'Final-State : SUCCEEDED' <<< "${status}" || fail "${app_id} did not succeed: ${status}"

echo "OK: Spark job ran on YARN"
