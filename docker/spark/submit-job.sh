#!/usr/bin/env bash
# Submit a job from /opt/spark_jobs to YARN: executors on the cluster, driver in this container.
# Used by scripts/spark-yarn.sh and by the Airflow DAGs, so there is one copy of the submit settings.
# Usage: submit-job.sh <job.py> [job arguments...]
set -euo pipefail

job="${1:?usage: submit-job.sh <job.py> [job arguments...]}"
shift

# The driver must be reachable from the YARN containers, so it advertises its own IP on the shared network.
exec /opt/spark/bin/spark-submit \
  --master yarn --deploy-mode client \
  --jars "$(ls /opt/extra-jars/*.jar | paste -sd, -)" \
  --driver-memory 512m --executor-memory 512m --num-executors 1 --executor-cores 1 \
  --conf spark.driver.host="$(hostname -i)" --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.shuffle.partitions=4 \
  "/opt/spark_jobs/${job}" "$@"
