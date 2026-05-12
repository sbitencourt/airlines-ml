import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from dst_airlines.config import INCOMING_DIR, PROCESSED_DIR
from dst_airlines.etl.load.common import build_incoming_pattern, log_event
from dst_airlines.insights.flights_snapshot import build_flight_snapshot_insights
from dst_airlines.io.postgres_writer import insert_flight_snapshot_insights


env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(env_file)


def find_snapshot_files(source: str, endpoint: str, run_id: str) -> tuple[list[Path], str]:
    pattern = build_incoming_pattern(source, endpoint, run_id=run_id)

    search_attempts = [
        (INCOMING_DIR, pattern, False),
        (PROCESSED_DIR, pattern, True),
        (PROCESSED_DIR, f"*{pattern}", True),
        (INCOMING_DIR, f"*{run_id}*.json", False),
        (PROCESSED_DIR, f"*{run_id}*.json", True),
        (PROCESSED_DIR, f"*{run_id}*", True),
    ]

    for base_dir, file_pattern, recursive in search_attempts:
        target_files = (
            sorted(base_dir.rglob(file_pattern))
            if recursive
            else sorted(base_dir.glob(file_pattern))
        )

        if target_files:
            return target_files, str(base_dir)

    raise FileNotFoundError(
        f"No files found for run_id={run_id}. "
        f"Expected pattern: {pattern}. "
        f"Searched in {INCOMING_DIR} and {PROCESSED_DIR}."
    )


def build_snapshot_insights_to_postgres(
    source: str = "aviationstack",
    endpoint: str = "flights",
    run_id: str | None = None,
) -> None:
    if not run_id:
        raise ValueError("run_id is required to build flight snapshot insights")

    source = source.strip().lower()
    endpoint = endpoint.strip().lower()

    stage = "flight_snapshot_insights"

    target_files, search_location = find_snapshot_files(
        source=source,
        endpoint=endpoint,
        run_id=run_id,
    )

    log_event(
        "INFO",
        stage,
        "input_files_discovered",
        run_id=run_id,
        source=source,
        endpoint=endpoint,
        files=len(target_files),
        search_location=search_location,
    )

    total_inserted = 0

    for file_path in target_files:
        log_event(
            "INFO",
            stage,
            "file_processing_started",
            run_id=run_id,
            source=source,
            endpoint=endpoint,
            file_name=file_path.name,
            file_path=str(file_path),
        )

        with open(file_path, "r", encoding="utf-8") as file:
            payload = json.load(file)

        insights = build_flight_snapshot_insights(
            payload=payload,
            run_id=run_id,
            source=source,
            endpoint=endpoint,
        )

        insert_flight_snapshot_insights(insights)
        total_inserted += 1

        log_event(
            "INFO",
            stage,
            "snapshot_insights_inserted",
            run_id=run_id,
            source=source,
            endpoint=endpoint,
            file_name=file_path.name,
            observed_flights_total=insights["observed_flights_total"],
            observed_flights_active=insights["observed_flights_active"],
            observed_flights_scheduled=insights["observed_flights_scheduled"],
            observed_flights_landed=insights["observed_flights_landed"],
            observed_flights_cancelled=insights["observed_flights_cancelled"],
            observed_flights_unknown=insights["observed_flights_unknown"],
            observed_flights_active_pct=insights["observed_flights_active_pct"],
        )

    log_event(
        "INFO",
        stage,
        "pipeline_summary",
        run_id=run_id,
        source=source,
        endpoint=endpoint,
        inserted=total_inserted,
        errors=0,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build flight snapshot insights and write them to PostgreSQL"
    )

    parser.add_argument("--source", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--run-id", required=True)

    args = parser.parse_args()

    build_snapshot_insights_to_postgres(
        source=args.source,
        endpoint=args.endpoint,
        run_id=args.run_id,
    )