import argparse

from etl.load.common import generate_run_id, log_event
from dst_airlines.pipelines.registry import get_pipeline


def run_etl(source: str, endpoint: str, run_id: str | None = None):
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
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run-etl")
    run.add_argument("--source", required=True)
    run.add_argument("--endpoint", required=True)
    run.add_argument("--run-id", required=False)

    args = parser.parse_args()

    if args.command == "run-etl":
        run_etl(args.source, args.endpoint, args.run_id)


if __name__ == "__main__":
    main()