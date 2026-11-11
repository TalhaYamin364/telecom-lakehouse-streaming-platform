#!/usr/bin/env bash
# Start over with empty data: delete the bronze table and its checkpoint, and recreate an empty cdr.raw.
# They go together: a checkpoint that points past the end of a recreated topic would make the job fail.
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose -f docker/compose.hadoop.yml exec -T namenode hdfs dfs -rm -r -f /warehouse/bronze /checkpoints/bronze_cdr_raw > /dev/null
docker compose -f docker/compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --delete --if-exists --topic 'cdr\.raw' > /dev/null
sleep 4
scripts/kafka-smoke.sh > /dev/null
echo "OK: bronze table and checkpoint deleted, cdr.raw recreated empty"
