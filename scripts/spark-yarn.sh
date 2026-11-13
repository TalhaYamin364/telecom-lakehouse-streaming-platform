#!/usr/bin/env bash
# Run a job from spark_jobs/ on YARN: executors on the cluster, driver in the spark-client container.
# Usage: scripts/spark-yarn.sh <job.py> [job arguments...]
# MSISDN_SALT, if set in your shell, is passed to the job (the silver layer uses it to hash phone numbers).
set -euo pipefail

cd "$(dirname "$0")/.."
job="${1:?usage: scripts/spark-yarn.sh <job.py> [job arguments...]}"
shift

# The submit settings live in docker/spark/submit-job.sh, baked into the image, so Airflow uses the same ones.
exec docker compose -f docker/compose.hadoop.yml --profile spark run --rm -T -e MSISDN_SALT spark-client /opt/submit-job.sh "${job}" "$@"
