import argparse
import json
import os
import shutil
import time


from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from dst_airlines.config import INCOMING_DIR, PROCESSED_DIR

from dst_airlines.etl.load.metrics import (
    emit_file_metrics,
    emit_pipeline_metrics,
    emit_pipeline_status
    )

from dst_airlines.etl.load.common import (
    build_document_meta,
    build_incoming_pattern,
    build_processed_destination,
    generate_run_id,
    log_event
    )

load_dotenv()



def extract_records(payload):
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return payload["data"]
        if isinstance(payload.get("results"), list):
            return payload["results"]
        return [payload]

    return []


def build_flight_key(item):
    flight_date = item.get("flight_date")

    flight = item.get("flight") or {}
    departure = item.get("departure") or {}
    arrival = item.get("arrival") or {}
    airline = item.get("airline") or {}

    return {
        "flight_date": flight_date,
        "flight_number": flight.get("iata") or flight.get("icao") or flight.get("number"),
        "departure_iata": departure.get("iata"),
        "arrival_iata": arrival.get("iata"),
        "airline_iata": airline.get("iata"),
    }


def is_valid_flight_key(flight_key):
    return (
        flight_key.get("flight_date") is not None
        and flight_key.get("flight_number") is not None
    )


def sync_flights_to_mongo(
    source: str = "aviationstack",
    endpoint: str = "flights",
    run_id: str | None = None,
    mongodb_uri=None,
    mongodb_db=None,
    mongodb_collection=None,
):
    run_id = run_id or generate_run_id()
    stage = "load_flights"
    started_at = time.perf_counter()

    mongodb_uri = mongodb_uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    mongodb_db = mongodb_db or os.getenv("MONGODB_DB", "dev")
    mongodb_collection = mongodb_collection or os.getenv("MONGODB_COLLECTION_FLIGHTS", "flights")

    total_files = 0
    total_records = 0
    total_valid_records = 0
    total_invalid_records = 0
    total_inserted = 0
    total_updated = 0
    total_unchanged = 0

    log_event(
        "INFO",
        stage,
        "pipeline_started",
        run_id=run_id,
        source=source,
        endpoint=endpoint,
        incoming_dir=str(INCOMING_DIR),
        processed_dir=str(PROCESSED_DIR),
        mongodb_db=mongodb_db,
        mongodb_collection=mongodb_collection,
    )

    client = MongoClient(mongodb_uri)

    try:
        db = client[mongodb_db]
        collection = db[mongodb_collection]

        collection.create_index(
            [
                ("flight_date", 1),
                ("flight_number", 1),
                ("departure_iata", 1),
                ("arrival_iata", 1),
                ("airline_iata", 1),
            ],
            unique=True,
            name="uniq_flight_record",
        )

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

        # First try: strict run_id matching
        pattern = build_incoming_pattern(source, endpoint, run_id=run_id)
        target_files = sorted(INCOMING_DIR.glob(pattern))

        # Fallback: allow legacy/test files without run_id
        if not target_files:
            fallback_pattern = build_incoming_pattern(source, endpoint, run_id=None)
            target_files = sorted(INCOMING_DIR.glob(fallback_pattern))

            if target_files:
                log_event(
                    "WARNING",
                    stage,
                    "fallback_to_legacy_pattern",
                    run_id=run_id,
                    source=source,
                    endpoint=endpoint,
                    fallback_pattern=fallback_pattern,
                    files=len(target_files),
                )

        if not target_files:
            raise FileNotFoundError(
                f"No files found in data/incoming matching {pattern}"
            )

        log_event(
            "INFO",
            stage,
            "input_files_discovered",
            run_id=run_id,
            source=source,
            endpoint=endpoint,
            files=len(target_files),
            pattern=pattern,
        )

        for file_path in target_files:
            file_started_at = time.perf_counter()
            total_files += 1

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

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    payload = json.load(f)

                data = extract_records(payload)
                total_records += len(data)

                updates = []
                valid_in_file = 0
                invalid_in_file = 0

                for item in data:
                    if not isinstance(item, dict):
                        invalid_in_file += 1
                        continue

                    flight_key = build_flight_key(item)

                    if not is_valid_flight_key(flight_key):
                        invalid_in_file += 1
                        continue

                    valid_in_file += 1
                    item.update(flight_key)
                    item["_meta"] = build_document_meta(run_id, source, endpoint)

                    updates.append(
                        UpdateOne(
                            flight_key,
                            {"$set": item},
                            upsert=True,
                        )
                    )

                total_valid_records += valid_in_file
                total_invalid_records += invalid_in_file

                inserted_in_file = 0
                updated_in_file = 0
                unchanged_in_file = 0

                if updates:
                    result = collection.bulk_write(updates, ordered=False)
                    inserted_in_file = result.upserted_count
                    updated_in_file = result.modified_count
                    unchanged_in_file = max(
                        result.matched_count - result.modified_count,
                        0,
                    )

                    total_inserted += inserted_in_file
                    total_updated += updated_in_file
                    total_unchanged += unchanged_in_file

                destination = build_processed_destination(
                    PROCESSED_DIR,
                    run_id,
                    file_path.name,
                )
                shutil.move(str(file_path), str(destination))

                file_duration_seconds = round(time.perf_counter() - file_started_at, 3)

                log_event(
                    "INFO",
                    stage,
                    "file_processed",
                    run_id=run_id,
                    source=source,
                    endpoint=endpoint,
                    file_name=file_path.name,
                    records=len(data),
                    valid=valid_in_file,
                    invalid=invalid_in_file,
                    inserted=inserted_in_file,
                    updated=updated_in_file,
                    unchanged=unchanged_in_file,
                    moved_to=str(destination),
                    duration_seconds=file_duration_seconds,
                )

                emit_file_metrics(
                    source=source,
                    endpoint=endpoint,
                    stage=stage,
                    run_id=run_id,
                    file_name=file_path.name,
                    records=len(data),
                    valid=valid_in_file,
                    invalid=invalid_in_file,
                    inserted=inserted_in_file,
                    updated=updated_in_file,
                    unchanged=unchanged_in_file,
                    duration_seconds=file_duration_seconds,
                )

            except json.JSONDecodeError as exc:
                log_event(
                    "ERROR",
                    stage,
                    "file_json_decode_error",
                    run_id=run_id,
                    source=source,
                    endpoint=endpoint,
                    file_name=file_path.name,
                    error=str(exc),
                )
                raise

            except BulkWriteError as exc:
                log_event(
                    "ERROR",
                    stage,
                    "file_bulk_write_error",
                    run_id=run_id,
                    source=source,
                    endpoint=endpoint,
                    file_name=file_path.name,
                    details=exc.details,
                )
                raise

            except Exception as exc:
                log_event(
                    "ERROR",
                    stage,
                    "file_processing_failed",
                    run_id=run_id,
                    source=source,
                    endpoint=endpoint,
                    file_name=file_path.name,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                raise

        duration_seconds = round(time.perf_counter() - started_at, 3)

        log_event(
            "INFO",
            stage,
            "pipeline_summary",
            run_id=run_id,
            source=source,
            endpoint=endpoint,
            files=total_files,
            records=total_records,
            valid=total_valid_records,
            invalid=total_invalid_records,
            inserted=total_inserted,
            updated=total_updated,
            unchanged=total_unchanged,
            errors=0,
            duration_seconds=duration_seconds,
        )

        emit_pipeline_metrics(
            source=source,
            endpoint=endpoint,
            stage=stage,
            run_id=run_id,
            files=total_files,
            records=total_records,
            valid=total_valid_records,
            invalid=total_invalid_records,
            inserted=total_inserted,
            updated=total_updated,
            unchanged=total_unchanged,
            duration_seconds=duration_seconds,
        )

        emit_pipeline_status(
            source=source,
            endpoint=endpoint,
            stage=stage,
            run_id=run_id,
            status="success",
            value=1,
        )

    except Exception as exc:
        duration_seconds = round(time.perf_counter() - started_at, 3)

        log_event(
            "ERROR",
            stage,
            "pipeline_failed",
            run_id=run_id,
            source=source,
            endpoint=endpoint,
            error_type=type(exc).__name__,
            error=str(exc),
            files=total_files,
            records=total_records,
            valid=total_valid_records,
            invalid=total_invalid_records,
            inserted=total_inserted,
            updated=total_updated,
            unchanged=total_unchanged,
            duration_seconds=duration_seconds,
        )

        emit_pipeline_status(
            source=source,
            endpoint=endpoint,
            stage=stage,
            run_id=run_id,
            status="failed",
            value=0,
        )
        raise

    finally:
        client.close()
        log_event(
            "INFO",
            stage,
            "mongodb_connection_closed",
            run_id=run_id,
            source=source,
            endpoint=endpoint,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--run-id", required=False)
    args = parser.parse_args()

    sync_flights_to_mongo(
        source=args.source.strip().lower(),
        endpoint=args.endpoint.strip().lower(),
        run_id=args.run_id,
    )