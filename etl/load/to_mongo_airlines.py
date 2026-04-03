import argparse
from ast import pattern
import json
import os
import shutil
import time
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from etl.load.common import (
    build_document_meta,
    build_incoming_pattern,
    build_processed_destination,
    generate_run_id,
    log_event,
)
from etl.load.metrics import (
    emit_file_metrics,
    emit_pipeline_metrics,
    emit_pipeline_status,
)

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def extract_records(payload):
    records = []

    if isinstance(payload, list):
        for page in payload:
            if isinstance(page, dict):
                data = page.get("data") or page.get("results")
                if isinstance(data, list):
                    records.extend(data)
    elif isinstance(payload, dict):
        data = payload.get("data") or payload.get("results")
        if isinstance(data, list):
            records.extend(data)

    return records


def build_airline_key(item):
    return {
        "airline_iata": item.get("iata_code"),
        "airline_icao": item.get("icao_code"),
        "airline_name": item.get("airline_name"),
    }


def is_valid_airline_key(airline_key):
    return any(
        airline_key.get(field) is not None
        for field in ("airline_iata", "airline_icao", "airline_name")
    )


def sync_airlines_to_mongo(
    source: str = "aviationstack",
    endpoint: str = "airlines",
    run_id: str | None = None,
    mongodb_uri=None,
    mongodb_db=None,
    mongodb_collection=None,
):
    run_id = run_id or generate_run_id()
    stage = "load_airlines"
    started_at = time.perf_counter()

    mongodb_uri = mongodb_uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    mongodb_db = mongodb_db or os.getenv("MONGODB_DB", "dev")
    mongodb_collection = mongodb_collection or os.getenv("MONGODB_COLLECTION_AIRLINES", "airlines")

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
                ("airline_iata", 1),
                ("airline_icao", 1),
                ("airline_name", 1),
            ],
            unique=True,
            name="uniq_airline_record",
        )

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

        pattern = build_incoming_pattern(source, endpoint, run_id=run_id)
        target_files = sorted(INCOMING_DIR.glob(pattern))

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

                    airline_key = build_airline_key(item)

                    if not is_valid_airline_key(airline_key):
                        invalid_in_file += 1
                        continue

                    valid_in_file += 1
                    item.update(airline_key)
                    item["_meta"] = build_document_meta(run_id, source, endpoint)

                    updates.append(
                        UpdateOne(
                            airline_key,
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

    sync_airlines_to_mongo(
        source=args.source.strip().lower(),
        endpoint=args.endpoint.strip().lower(),
        run_id=args.run_id,
    )