from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "alexh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="dst_airlines_flights",
    start_date=datetime(2026, 4, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dst_airlines", "flights"],
) as dag:

    run_flights_etl = BashOperator(
        task_id="run_flights_etl",
        bash_command=(
            "python -m dst_airlines.cli run-etl "
            "--source aviationstack "
            "--endpoint flights "
            "--run-id '{{ dag.dag_id }}__{{ ts_nodash }}'"
        ),
    )

    build_flight_snapshot_insights = BashOperator(
        task_id="build_flight_snapshot_insights",
        bash_command=(
            "python -m dst_airlines.etl.insights.flights_snapshot_to_postgres "
            "--source aviationstack "
            "--endpoint flights "
            "--run-id '{{ dag.dag_id }}__{{ ts_nodash }}'"
        ),
    )

    build_flight_aircraft_routes = BashOperator(
        task_id="build_flight_aircraft_routes",
        bash_command=(
            "python -m dst_airlines.etl.insights.flight_aircraft_routes_to_postgres "
            "--source aviationstack "
            "--endpoint flights "
            "--run-id '{{ dag.dag_id }}__{{ ts_nodash }}'"
        ),
    )

    run_flights_etl >> build_flight_snapshot_insights >> build_flight_aircraft_routes