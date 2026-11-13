"""The batch half of the lakehouse: bronze, then silver, then gold, one run at a time.

Every task runs the same idempotent Spark job that scripts/spark-yarn.sh runs by hand, so a retry or a rerun of a task
reads the same input and reaches the same result; nothing is added twice. Runs never overlap (max_active_runs=1) and
every task shares the one-slot lake_writer pool, which keeps the single-writer rule from the README.
"""
import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

SUBMIT = "/opt/submit-job.sh"

with DAG(
    dag_id="lakehouse_batch",
    schedule=os.environ.get("LAKEHOUSE_SCHEDULE", "*/15 * * * *"),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,  # a missed interval needs no catch-up: each run processes everything new, whatever interval it is for
    max_active_runs=1,
    default_args={
        "pool": "lake_writer",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["lakehouse"],
    doc_md=__doc__,
) as dag:
    bronze = BashOperator(task_id="bronze", bash_command=f"{SUBMIT} bronze_stream.py --once")
    silver = BashOperator(task_id="silver", bash_command=f"{SUBMIT} silver_cdr.py")
    gold = BashOperator(task_id="gold", bash_command=f"{SUBMIT} gold_daily.py")

    bronze >> silver >> gold
