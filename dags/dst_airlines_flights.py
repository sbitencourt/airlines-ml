from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "alexh",
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="dst_airlines_flights",
    start_date=datetime(2026, 4, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dst_airlines", "flights"],
) as dag:

    run_flights = BashOperator(
        task_id="run_flights_etl",
        bash_command="python -m dst_airlines.cli run-etl --source aviationstack --endpoint flights",
    )