import json
import os
import shutil
from pathlib import Path

from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


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


def sync_flights_to_mongo(mongodb_uri=None, mongodb_db=None, mongodb_collection=None):
    mongodb_uri = mongodb_uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    mongodb_db = mongodb_db or os.getenv("MONGODB_DB", "dev")
    mongodb_collection = mongodb_collection or os.getenv("MONGODB_COLLECTION", "raw_flights")

    client = MongoClient(mongodb_uri)

    total_files = 0
    total_records = 0
    total_valid_records = 0
    total_invalid_records = 0
    total_inserted = 0
    total_updated = 0

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
        target_files = sorted(INCOMING_DIR.glob("aviationstack_incoming*.json"))

        if not target_files:
            raise FileNotFoundError(
                "No files found in data/incoming matching aviationstack_incoming*.json"
            )

        for file_path in target_files:
            total_files += 1

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

            if updates:
                result = collection.bulk_write(updates)
                inserted_in_file = result.upserted_count
                updated_in_file = result.modified_count

                total_inserted += inserted_in_file
                total_updated += updated_in_file

            print(
                f"[load] file={file_path.name} "
                f"records={len(data)} "
                f"valid={valid_in_file} "
                f"invalid={invalid_in_file} "
                f"inserted={inserted_in_file} "
                f"updated={updated_in_file}"
            )

            shutil.move(str(file_path), str(PROCESSED_DIR / file_path.name))
            print(f"[load] moved to processed: {PROCESSED_DIR / file_path.name}")

        print(
            f"[load] summary files={total_files} "
            f"records={total_records} "
            f"valid={total_valid_records} "
            f"invalid={total_invalid_records} "
            f"inserted={total_inserted} "
            f"updated={total_updated}"
        )

    finally:
        client.close()


if __name__ == "__main__":
    sync_flights_to_mongo()