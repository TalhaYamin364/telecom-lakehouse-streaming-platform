#!/usr/bin/env bash
# Build and start the Hadoop and Kafka stacks, wait until they are healthy, then run the smoke tests.
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose -f docker/compose.hadoop.yml up -d --build --wait
docker compose -f docker/compose.kafka.yml up -d --wait   # after Hadoop: Kafka joins its network
scripts/hdfs-smoke.sh
scripts/spark-yarn-smoke.sh
scripts/kafka-smoke.sh
