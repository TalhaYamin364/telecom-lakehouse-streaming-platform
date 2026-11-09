#!/usr/bin/env bash
# Smoke test for Kafka: broker is up, topic cdr.raw exists (created if missing), and a message round-trips.
set -euo pipefail

cd "$(dirname "$0")/.."

compose=(docker compose -f docker/compose.kafka.yml)
bin=/opt/kafka/bin
bootstrap=localhost:19092
k() { "${compose[@]}" exec -T kafka "$@"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

# Retry a check every 2s, up to N times, so a slow broker start is not mistaken for a failure.
retry() {
  local attempts="$1"; shift
  for ((i = 1; i <= attempts; i++)); do
    "$@" && return 0
    sleep 2
  done
  return 1
}
broker_ready() { k "${bin}/kafka-broker-api-versions.sh" --bootstrap-server "${bootstrap}" > /dev/null 2>&1; }

probe="smoke-$(date +%s)"
cleanup() { k "${bin}/kafka-topics.sh" --bootstrap-server "${bootstrap}" --delete --if-exists --topic "${probe}" > /dev/null 2>&1 || true; }
trap cleanup EXIT

echo "1/3 broker is reachable"
retry 15 broker_ready || fail "broker did not answer on ${bootstrap}"

echo "2/3 topic cdr.raw exists with 3 partitions"
# Safe to rerun: --if-not-exists never changes or duplicates an existing topic.
k "${bin}/kafka-topics.sh" --bootstrap-server "${bootstrap}" --create --if-not-exists \
  --topic cdr.raw --partitions 3 --replication-factor 1 > /dev/null
described="$(k "${bin}/kafka-topics.sh" --bootstrap-server "${bootstrap}" --describe --topic cdr.raw)"
[[ "${described}" == *"PartitionCount: 3"* ]] || fail "cdr.raw is missing or has the wrong partition count: ${described}"

echo "3/3 a message round-trips on a throwaway topic"
k "${bin}/kafka-topics.sh" --bootstrap-server "${bootstrap}" --create --topic "${probe}" --partitions 1 --replication-factor 1 > /dev/null
printf '%s\n' "hello from the smoke test" | k "${bin}/kafka-console-producer.sh" --bootstrap-server "${bootstrap}" --topic "${probe}" > /dev/null
got="$(k "${bin}/kafka-console-consumer.sh" --bootstrap-server "${bootstrap}" --topic "${probe}" \
  --from-beginning --max-messages 1 --timeout-ms 20000 2>/dev/null)"
[ "${got}" = "hello from the smoke test" ] || fail "read back '${got}', expected the message that was produced"

echo "OK: Kafka (KRaft) is healthy and cdr.raw is ready"
