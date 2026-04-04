from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "alexh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dst_airlines_airports",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dst_airlines", "airports"],
) as dag:

    run_airports = BashOperator(
        task_id="run_airports_etl",
        bash_command="python -m dst_airlines.cli run-etl --source aviationstack --endpoint airports",
    )