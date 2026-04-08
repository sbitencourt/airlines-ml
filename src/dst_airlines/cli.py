import argparse

from dst_airlines.etl.load.common import generate_run_id, log_event
from dst_airlines.pipelines.registry import get_pipeline


def run_etl(source: str, endpoint: str, run_id: str | None = None):
    """
    Execute the ETL pipeline for a given data source and endpoint.

    This function orchestrates the full ETL lifecycle:
    - Extract: retrieves raw data from the source
    - Transform: processes and cleans the data
    - Load: stores the processed data into the target system

    It also handles logging for observability and error tracking.

    Args:
        source (str): Data source name (e.g., "aviationstack").
        endpoint (str): Endpoint within the source (e.g., "airports", "flights").
        run_id (str | None, optional): Unique identifier for the ETL run.
            If not provided, it will be generated automatically.

    Raises:
        Exception: Re-raises any exception that occurs during the ETL process
        after logging the failure event.

    Examples:
        Run ETL for airports:
            python -m dst_airlines.cli run-etl --source aviationstack --endpoint airports

        Run ETL for flights:
            python -m dst_airlines.cli run-etl --source aviationstack --endpoint flights
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
        pipeline.extract_fn(source=source, endpoint=endpoint, run_id=run_id)
        pipeline.transform_fn(source=source, endpoint=endpoint, run_id=run_id)
        pipeline.load_fn(source=source, endpoint=endpoint, run_id=run_id)

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


def main():
    """
    Entry point for the CLI interface.

    This function parses command-line arguments and dispatches
    the corresponding command.

    Available commands:
        run-etl:
            Executes an ETL pipeline for a given source and endpoint.

    CLI Usage:
        python -m dst_airlines.cli run-etl --source <source> --endpoint <endpoint> [--run-id <id>]

    Examples:
        python -m dst_airlines.cli run-etl --source aviationstack --endpoint airports
        python -m dst_airlines.cli run-etl --source aviationstack --endpoint flights
    """
    parser = argparse.ArgumentParser(
        description="CLI for running ETL pipelines in dst_airlines project.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser(
        "run-etl",
        help="Run ETL pipeline for a given source and endpoint."
    )
    run.add_argument(
        "--source",
        required=True,
        help="Data source name (e.g., aviationstack)."
    )
    run.add_argument(
        "--endpoint",
        required=True,
        help="Endpoint name (e.g., airports, flights, airlines)."
    )
    run.add_argument(
        "--run-id",
        required=False,
        help="Optional run identifier. If not provided, one will be generated."
    )

    args = parser.parse_args()

    if args.command == "run-etl":
        run_etl(args.source, args.endpoint, args.run_id)


if __name__ == "__main__":
    main()