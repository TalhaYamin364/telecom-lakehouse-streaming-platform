#!/usr/bin/env bash
# Run a job from spark_jobs/ on YARN: executors on the cluster, driver in the spark-client container.
# Usage: scripts/spark-yarn.sh <job.py> [job arguments...]
# MSISDN_SALT, if set in your shell, is passed to the job (the silver layer uses it to hash phone numbers).
set -euo pipefail

cd "$(dirname "$0")/.."
job="${1:?usage: scripts/spark-yarn.sh <job.py> [job arguments...]}"
shift

# The driver must be reachable from the YARN containers, so it advertises its own IP on the shared network.
# The command is single-quoted on purpose: the expansions must happen inside the container, not on the host.
# shellcheck disable=SC2016
docker compose -f docker/compose.hadoop.yml --profile spark run --rm -T -e MSISDN_SALT spark-client bash -c '
  job="$1"; shift
  exec /opt/spark/bin/spark-submit \
    --master yarn --deploy-mode client \
    --jars "$(ls /opt/extra-jars/*.jar | paste -sd, -)" \
    --driver-memory 512m --executor-memory 512m --num-executors 1 --executor-cores 1 \
    --conf spark.driver.host="$(hostname -i)" --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.sql.shuffle.partitions=4 \
    "/opt/spark_jobs/${job}" "$@"
' bash "${job}" "$@"
