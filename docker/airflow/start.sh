#!/usr/bin/env bash
# Start Airflow for local use: set up its database and the one-slot pool, then run all its parts in this container.
set -euo pipefail

airflow db migrate
# Every DAG task that writes to the lake runs in this pool, so two writers can never overlap, even across DAGs.
airflow pools set lake_writer 1 "Only one job writes to the lake at a time"
exec airflow standalone
