"""Daily climate pipeline: download -> bronze -> silver -> gold -> reconcile.

Every job is idempotent and incremental, so a rerun only does what is left. A backfill (e.g.
after changing START_YEAR or a quality rule) is a manual run with `full_refresh` set, which
recomputes silver and gold from bronze.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

# The jobs live in their own virtualenv in the image, apart from Airflow's dependencies.
JOB = "/opt/climate/.venv/bin/python -m climate_lakehouse."
FULL_REFRESH = "{{ '--full-refresh' if params.full_refresh else '' }}"

with DAG(
    dag_id="climate_pipeline",
    description="DWD daily climate data through bronze, silver and gold",
    # DWD replaces the recent files during the night
    schedule="0 6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,  # one Spark driver at a time fits the memory budget
    params={
        "full_refresh": Param(
            False, type="boolean", description="recompute silver and gold completely (backfill)"
        )
    },
    tags=["climate"],
) as dag:
    download = BashOperator(
        task_id="download",
        bash_command=f"{JOB}ingest.download",
        # only network trouble is worth a retry; the Spark jobs fail deterministically
        retries=2,
        retry_delay=timedelta(minutes=10),
    )
    bronze = BashOperator(task_id="bronze", bash_command=f"{JOB}transform.bronze")
    silver = BashOperator(task_id="silver", bash_command=f"{JOB}transform.silver {FULL_REFRESH}")
    gold = BashOperator(task_id="gold", bash_command=f"{JOB}transform.gold {FULL_REFRESH}")
    reconcile = BashOperator(task_id="reconcile", bash_command=f"{JOB}transform.reconcile")

    download >> bronze >> silver >> gold >> reconcile
