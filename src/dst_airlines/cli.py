import argparse
import time

from dst_airlines.etl.load.common import generate_run_id, log_event
from dst_airlines.etl.load.metrics import (
    emit_dag_active,
    emit_dag_last_run_timestamp,
    emit_dag_last_success_timestamp,
    emit_dag_run_status,
    emit_dag_schedule_interval,
)
from dst_airlines.pipelines.registry import get_pipeline
from dst_airlines.utils.metrics import delete_metric_group_for_dag


def run_etl(
    source: str,
    endpoint: str,
    run_id: str | None = None,
    dag_id: str | None = None,
    schedule_interval_seconds: int | None = None,
):
    source = source.lower()
    endpoint = endpoint.lower()
    run_id = run_id or generate_run_id()
    dag_id = dag_id or f"{source}_{endpoint}"

    delete_metric_group_for_dag(
        dag_id=dag_id,
        source=source,
        endpoint=endpoint,
    )

    pipeline = get_pipeline(source, endpoint)
    now = int(time.time())

    emit_dag_active(
        dag_id=dag_id,
        source=source,
        endpoint=endpoint,
        value=1,
    )
    emit_dag_last_run_timestamp(
        dag_id=dag_id,
        source=source,
        endpoint=endpoint,
        timestamp_seconds=now,
    )
    emit_dag_run_status(
        dag_id=dag_id,
        source=source,
        endpoint=endpoint,
        status="running",
    )

    if schedule_interval_seconds is not None:
        emit_dag_schedule_interval(
            dag_id=dag_id,
            source=source,
            endpoint=endpoint,
            schedule_interval_seconds=schedule_interval_seconds,
        )

    log_event(
        "INFO",
        "cli",
        "etl_started",
        source=source,
        endpoint=endpoint,
        run_id=run_id,
        dag_id=dag_id,
    )

    try:
        pipeline.extract_fn(source=source, endpoint=endpoint, run_id=run_id)
        pipeline.transform_fn(source=source, endpoint=endpoint, run_id=run_id)
        pipeline.load_fn(source=source, endpoint=endpoint, run_id=run_id)

        success_now = int(time.time())

        emit_dag_last_success_timestamp(
            dag_id=dag_id,
            source=source,
            endpoint=endpoint,
            timestamp_seconds=success_now,
        )
        emit_dag_run_status(
            dag_id=dag_id,
            source=source,
            endpoint=endpoint,
            status="success",
        )

        log_event(
            "INFO",
            "cli",
            "etl_finished",
            source=source,
            endpoint=endpoint,
            run_id=run_id,
            dag_id=dag_id,
            status="success",
        )

    except Exception as e:
        emit_dag_run_status(
            dag_id=dag_id,
            source=source,
            endpoint=endpoint,
            status="failed",
        )

        log_event(
            "ERROR",
            "cli",
            "etl_failed",
            source=source,
            endpoint=endpoint,
            run_id=run_id,
            dag_id=dag_id,
            error=str(e),
        )
        raise


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run-etl")
    run.add_argument("--source", required=True)
    run.add_argument("--endpoint", required=True)
    run.add_argument("--run-id", required=False)
    run.add_argument("--dag-id", required=False)
    run.add_argument("--schedule-interval-seconds", type=int, required=False)

    args = parser.parse_args()

    if args.command == "run-etl":
        run_etl(
            args.source,
            args.endpoint,
            args.run_id,
            dag_id=args.dag_id,
            schedule_interval_seconds=args.schedule_interval_seconds,
        )


if __name__ == "__main__":
    main()