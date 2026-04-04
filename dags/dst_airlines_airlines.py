from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "alexh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dst_airlines_airlines",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dst_airlines", "airlines"],
) as dag:

    run_airlines = BashOperator(
        task_id="run_airlines_etl",
        bash_command="python -m dst_airlines.cli run-etl --source aviationstack --endpoint airlines",
    )