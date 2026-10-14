"""Daily batch DAG: wait for raw data -> run the Spark batch job for the logical date.

Backfill the 31 simulated days:
    airflow dags backfill ledgerflow_daily -s 2026-01-01 -e 2026-01-31
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ledgerflow_daily",
    start_date=datetime(2026, 1, 1),
    end_date=datetime(2026, 1, 31),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ledgerflow"],
) as dag:
    wait_for_raw = FileSensor(
        task_id="wait_for_raw_csv",
        fs_conn_id="fs_default",
        filepath="/opt/ledgerflow/data/raw/paysim.csv",
        poke_interval=30,
        timeout=600,
    )

    run_batch = BashOperator(
        task_id="spark_batch_daily",
        bash_command="python /opt/ledgerflow/jobs/batch_daily.py --date {{ ds }}",
    )

    wait_for_raw >> run_batch
