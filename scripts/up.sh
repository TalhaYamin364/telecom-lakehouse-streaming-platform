#!/usr/bin/env bash
# Build and start the Hadoop stack, wait until it is healthy, then run both smoke tests.
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose -f docker/compose.hadoop.yml up -d --build --wait
scripts/hdfs-smoke.sh
scripts/spark-yarn-smoke.sh
