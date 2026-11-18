"""Tägliche Klima-Pipeline: download -> bronze -> silver -> gold -> reconcile -> maintain.

Jeder Job ist idempotent und inkrementell, ein erneuter Lauf erledigt also nur, was fehlt. Ein
Backfill (z. B. nach Änderung von START_YEAR oder einer Qualitätsregel) ist ein manueller Lauf
mit `full_refresh`, der Silver und Gold aus Bronze neu berechnet.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

# Die Jobs liegen im Image in einem eigenen virtualenv, getrennt von Airflows Abhängigkeiten.
JOB = "/opt/climate/.venv/bin/python -m climate_lakehouse."
FULL_REFRESH = "{{ '--full-refresh' if params.full_refresh else '' }}"

with DAG(
    dag_id="climate_pipeline",
    description="DWD daily climate data through bronze, silver and gold",
    # Der DWD ersetzt die recent-Dateien nachts
    schedule="0 6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,  # nur ein Spark-Driver gleichzeitig passt ins Speicherbudget
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
        # nur Netzwerkprobleme lohnen einen Retry; die Spark-Jobs scheitern deterministisch
        retries=2,
        retry_delay=timedelta(minutes=10),
    )
    bronze = BashOperator(task_id="bronze", bash_command=f"{JOB}transform.bronze")
    silver = BashOperator(task_id="silver", bash_command=f"{JOB}transform.silver {FULL_REFRESH}")
    gold = BashOperator(task_id="gold", bash_command=f"{JOB}transform.gold {FULL_REFRESH}")
    reconcile = BashOperator(task_id="reconcile", bash_command=f"{JOB}transform.reconcile")
    maintain = BashOperator(task_id="maintain", bash_command=f"{JOB}transform.maintain compact")

    download >> bronze >> silver >> gold >> reconcile >> maintain
