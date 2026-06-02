import argparse

from dst_airlines.etl.load.common import generate_run_id, log_event
from dst_airlines.pipelines.registry import get_pipeline


def run_etl(source: str, endpoint: str, run_id: str | None = None) -> None:
    """
    Execute an ETL pipeline for a given data source and endpoint.

    This function orchestrates the full ETL lifecycle:
    - Normalizes input parameters.
    - Retrieves the corresponding pipeline.
    - Executes extract, transform, and load steps.
    - Logs execution events and handles errors.

    Args:
        source: Name of the data source (e.g., "aviationstack").
        endpoint: Name of the endpoint to process (e.g., "flights", "airports").
        run_id: Unique identifier for the ETL run. If not provided, one is generated.

    Raises:
        Exception: Re-raises any exception produced during pipeline execution
            after logging the failure event.
    """
    source = source.lower()
    endpoint = endpoint.lower()
    run_id = run_id or generate_run_id()

    pipeline = get_pipeline(source, endpoint)

    log_event(
        "INFO",
        "cli",
        "etl_started",
        source=source,
        endpoint=endpoint,
        run_id=run_id,
    )

    try:
        pipeline.extract_fn(
            source=source,
            endpoint=endpoint,
            run_id=run_id,
        )

        pipeline.transform_fn(
            source=source,
            endpoint=endpoint,
            run_id=run_id,
        )

        pipeline.load_fn(
            source=source,
            endpoint=endpoint,
            run_id=run_id,
        )

        log_event(
            "INFO",
            "cli",
            "etl_finished",
            source=source,
            endpoint=endpoint,
            run_id=run_id,
            status="success",
        )

    except Exception as e:
        log_event(
            "ERROR",
            "cli",
            "etl_failed",
            source=source,
            endpoint=endpoint,
            run_id=run_id,
            error=str(e),
        )
        raise



def run_predictions() -> None:
    """
    Execute the ML prediction batch.

    This function keeps Airflow and local executions centralized through
    the project CLI instead of calling pipeline modules directly.
    """
    log_event(
        "INFO",
        "cli",
        "predictions_started",
    )

    try:
        from dst_airlines.pipelines.run_predictions import main as run_predictions_main

        run_predictions_main()

        log_event(
            "INFO",
            "cli",
            "predictions_finished",
            status="success",
        )

    except Exception as e:
        log_event(
            "ERROR",
            "cli",
            "predictions_failed",
            error=str(e),
        )
        raise

def main() -> None:
    """
    Entry point for the CLI application.
    """
    parser = argparse.ArgumentParser(
        description="CLI for running dst_airlines project pipelines.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser(
        "run-etl",
        help="Run ETL pipeline for a given source and endpoint.",
    )

    run.add_argument(
        "--source",
        required=True,
        help="Data source name (e.g., aviationstack).",
    )

    run.add_argument(
        "--endpoint",
        required=True,
        help="Endpoint name (e.g., airports, flights, airlines).",
    )

    run.add_argument(
        "--run-id",
        required=False,
        help="Optional run identifier. If not provided, one will be generated.",
    )

    sub.add_parser(
        "run-predictions",
        help="Run ML delay prediction batch.",
    )

    args = parser.parse_args()

    if args.command == "run-etl":
        run_etl(args.source, args.endpoint, args.run_id)

    elif args.command == "run-predictions":
        run_predictions()


if __name__ == "__main__":
    main()