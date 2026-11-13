"""Recompute gold for chosen days, on demand: trigger it with the days to rebuild, for example 2026-01-01,2026-01-02.

It runs `gold_daily.py --day ...`, which recomputes exactly those days from silver in one atomic overwrite each and leaves
every other day alone, so it is safe to run again. It shares the lake_writer pool with lakehouse_batch, so it waits
for a running batch instead of overlapping it.
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

with DAG(
    dag_id="lakehouse_backfill",
    schedule=None,
    is_paused_upon_creation=False,  # on demand only, so it is ready to trigger straight away
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "days": Param(
            "2026-01-01", type="string", pattern=r"^\d{4}-\d{2}-\d{2}(,\d{4}-\d{2}-\d{2})*$",
            description="Days to recompute, as YYYY-MM-DD, comma separated",
        ),
    },
    default_args={"pool": "lake_writer", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["lakehouse"],
    doc_md=__doc__,
) as dag:
    BashOperator(
        task_id="gold_days",
        # The days reach the script through the environment, not the command line, and the pattern above has already
        # restricted them to dates and commas.
        env={"DAYS": "{{ params.days }}"},
        append_env=True,
        bash_command=(
            'args=(); IFS=, read -ra days <<< "$DAYS"; '
            'for d in "${days[@]}"; do args+=(--day "$d"); done; '
            'exec /opt/submit-job.sh gold_daily.py "${args[@]}"'
        ),
    )
