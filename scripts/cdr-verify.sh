#!/usr/bin/env bash
# Generate a batch of CDR events, read them back with the Kafka console consumer, and show the
# duplicates and late events in them. Everything runs in Docker.
set -euo pipefail

cd "$(dirname "$0")/.."

kafka=(docker compose -f docker/compose.kafka.yml)
consumer=("${kafka[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic cdr.raw)
generator=("${kafka[@]}" --profile generator run --rm -T)

"${kafka[@]}" --profile generator build --quiet generator > /dev/null

echo "1/3 generate 1000 events into cdr.raw (a new seed each run, so they are new events)"
"${generator[@]}" generator --count 1000 --seed "${RANDOM}${RANDOM}" 2>&1 | grep -E '^(sent|attempted|stopped|FAIL)'

echo
echo "2/3 the first 5 events, as the console consumer prints them"
"${consumer[@]}" --from-beginning --max-messages 5 --timeout-ms 20000 2>/dev/null

echo
echo "3/3 everything in cdr.raw, summarised"
"${consumer[@]}" --from-beginning --timeout-ms 8000 \
  --property print.partition=true --property print.key=true --property key.separator='|' 2>/dev/null \
  | "${generator[@]}" --entrypoint python generator summarize.py
