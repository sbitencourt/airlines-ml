import argparse

from dst_airlines.etl.load.common import generate_run_id, log_event
from dst_airlines.pipelines.registry import get_pipeline
from dst_airlines.utils.prometheus import (
    create_metrics_registry,
    push_metrics_registry,
)


def run_etl(source: str, endpoint: str, run_id: str | None = None):
    """
    Execute an ETL pipeline for a given data source and endpoint.

    This function orchestrates the full ETL lifecycle:
    - Normalizes input parameters.
    - Retrieves the corresponding pipeline.
    - Creates a Prometheus metrics registry for the run.
    - Executes extract, transform, and load steps.
    - Logs execution events and handles errors.
    - Pushes metrics once at the end of the execution.

    Args:
        source (str): Name of the data source (e.g., "aviationstack").
        endpoint (str): Name of the endpoint to process (e.g., "flights", "airports").
        run_id (str | None, optional): Unique identifier for the ETL run.
            If not provided, a new one is generated automatically.

    Raises:
        Exception: Propagates any exception raised during pipeline execution
            after logging the failure event.

    Side Effects:
        - Logs events using `log_event`.
        - Executes pipeline functions (`extract_fn`, `transform_fn`, `load_fn`).
        - Pushes Prometheus metrics via `push_metrics_registry`.

    Notes:
        - A single metrics push is performed at the end of the run (even on failure).
        - The metrics registry is passed only to the load step.
    """
    source = source.lower()
    endpoint = endpoint.lower()
    run_id = run_id or generate_run_id()

    pipeline = get_pipeline(source, endpoint)

  
    metrics_registry = create_metrics_registry()

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

    
        pipeline.load_fn(
            source=source,
            endpoint=endpoint,
            run_id=run_id,
            metrics_registry=metrics_registry,
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

    finally:
        push_metrics_registry(metrics_registry)


def main():
    """
    Entry point for the CLI application.

    This function:
    - Configures the command-line interface using argparse.
    - Defines available commands and their arguments.
    - Parses user input.
    - Dispatches execution to the appropriate command handler.

    Supported Commands:
        run-etl:
            Executes an ETL pipeline for a specified source and endpoint.

    CLI Arguments:
        --source (str, required):
            Data source name (e.g., "aviationstack").

        --endpoint (str, required):
            Endpoint name (e.g., "airports", "flights", "airlines").

        --run-id (str, optional):
            Custom run identifier. If omitted, a new one is generated.

    Example:
        python script.py run-etl --source aviationstack --endpoint flights

    Raises:
        SystemExit: Triggered by argparse on invalid arguments or usage errors.
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