#!/usr/bin/env bash
# Run the bronze job on YARN (executors on the cluster, driver in the spark-client container).
# Usage: scripts/bronze-stream.sh [--once]   (--once drains Kafka and stops; no flag runs until you press Ctrl-C)
set -euo pipefail

cd "$(dirname "$0")/.."

# The driver must be reachable from the YARN containers, so it advertises its own IP on the shared network.
docker compose -f docker/compose.hadoop.yml --profile spark run --rm -T spark-client bash -c '
  exec /opt/spark/bin/spark-submit \
    --master yarn --deploy-mode client \
    --jars "$(ls /opt/extra-jars/*.jar | paste -sd, -)" \
    --driver-memory 512m --executor-memory 512m --num-executors 1 --executor-cores 1 \
    --conf spark.driver.host="$(hostname -i)" --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.sql.shuffle.partitions=4 \
    /opt/spark_jobs/bronze_stream.py "$@"
' bash "$@"
