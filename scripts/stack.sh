#!/usr/bin/env bash
# Operate the whole local stack.
# Usage: scripts/stack.sh start [--airflow] | stop [--volumes] | status | health
#   start   build and start Hadoop, then Kafka (and Airflow with --airflow), and create the cdr.raw topic
#   stop    stop everything (--volumes also deletes the data volumes)
#   status  show every container and whether it is healthy
#   health  check the stack end to end without changing any data; exits non-zero if anything is wrong
set -euo pipefail

cd "$(dirname "$0")/.."

# Both profiles, so Compose can see the Spark image the Airflow image is built on.
hadoop=(docker compose -f docker/compose.hadoop.yml --profile spark --profile airflow)
kafka=(docker compose -f docker/compose.kafka.yml)
topics=("${kafka[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092)

start() {
  local with_airflow=false
  [ "${1:-}" != "--airflow" ] || with_airflow=true
  "${hadoop[@]}" up -d --build --wait namenode datanode1 datanode2 resourcemanager nodemanager
  "${hadoop[@]}" build spark-client > /dev/null
  # After Hadoop: Kafka joins its network so the YARN executors can reach it.
  "${kafka[@]}" up -d --wait
  "${topics[@]}" --create --if-not-exists --topic cdr.raw --partitions 3 --replication-factor 1 > /dev/null
  # After a restart HDFS stays in safe mode for a while; it cannot take writes until that ends.
  local deadline=$((SECONDS + 120))
  until hdfs_in "OFF" -safemode get; do
    (( SECONDS < deadline )) || { echo "FAIL: HDFS is still in safe mode after 120s" >&2; exit 1; }
    sleep 3
  done
  if ${with_airflow}; then
    # build, then up: `up --build` fails on the Spark image the Airflow image is built on, while a separate build works.
    "${hadoop[@]}" build airflow > /dev/null
    "${hadoop[@]}" up -d --wait airflow
    echo "Airflow UI:           http://localhost:8080  (local only, no login)"
  fi
  echo "NameNode UI:          http://localhost:9870"
  echo "ResourceManager UI:   http://localhost:8088"
  echo "Started. Check it with: scripts/stack.sh health"
}

stop() {
  local volumes=()
  [ "${1:-}" != "--volumes" ] || volumes=(--volumes)
  # Kafka first: it is attached to the Hadoop network, which Hadoop's shutdown removes.
  # ${volumes[@]+...} keeps an empty array from counting as unset on macOS's bash 3.2.
  "${kafka[@]}" down ${volumes[@]+"${volumes[@]}"}
  "${hadoop[@]}" down ${volumes[@]+"${volumes[@]}"}
}

status() {
  "${hadoop[@]}" ps --all --format 'table {{.Service}}\t{{.Status}}'
  "${kafka[@]}" ps --all --format 'table {{.Service}}\t{{.Status}}'
}

failures=0
check() {  # check <name> <command...>: print ok or FAIL and remember a failure
  local name="$1"
  shift
  if "$@" > /dev/null 2>&1; then echo "ok    ${name}"; else echo "FAIL  ${name}"; failures=$((failures + 1)); fi
}

container_healthy() {  # container_healthy <compose project: hadoop|kafka> <service>
  local compose=("${hadoop[@]}")
  [ "$1" != "kafka" ] || compose=("${kafka[@]}")
  [ "$("${compose[@]}" ps --format '{{.Health}}' "$2")" = "healthy" ]
}

hdfs_in() {  # hdfs_in <pattern> <hdfs dfsadmin arguments...>: the output must contain the pattern
  local pattern="$1"
  shift
  [[ "$("${hadoop[@]}" exec -T namenode hdfs dfsadmin "$@")" == *"${pattern}"* ]]
}

yarn_node_running() { [[ "$("${hadoop[@]}" exec -T resourcemanager yarn node -list)" == *RUNNING* ]]; }
topic_exists() { grep -Fxq 'cdr.raw' <<< "$("${topics[@]}" --list)"; }  # a variable, not a pipe: grep -q would break the pipe
airflow_healthy() { [ "$("${hadoop[@]}" ps --format '{{.Health}}' airflow)" = "healthy" ]; }

health() {
  for service in namenode datanode1 datanode2 resourcemanager nodemanager; do
    check "container ${service} is healthy" container_healthy hadoop "${service}"
  done
  check "container kafka is healthy" container_healthy kafka kafka
  check "HDFS is out of safe mode" hdfs_in "OFF" -safemode get
  check "HDFS has 2 live DataNodes" hdfs_in "Live datanodes (2)" -report
  check "YARN has a running NodeManager" yarn_node_running
  check "Kafka topic cdr.raw exists" topic_exists
  # Airflow is optional: only checked if its container is there.
  if [ -n "$("${hadoop[@]}" ps --quiet airflow)" ]; then check "container airflow is healthy" airflow_healthy; fi
  if [ "${failures}" -ne 0 ]; then echo "${failures} check(s) failed" >&2; exit 1; fi
  echo "All checks passed"
}

case "${1:-}" in
  start) start "${2:-}" ;;
  stop) stop "${2:-}" ;;
  status) status ;;
  health) health ;;
  *) sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
esac
