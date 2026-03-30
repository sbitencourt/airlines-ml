import json
import os
import shutil
from pathlib import Path
from pymongo import MongoClient, UpdateOne

# Path configurations
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def extract_records(payload):
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return payload["data"]
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
        target_files = list(INCOMING_DIR.glob("aviationstack*.json"))

        if not target_files:
            print("No files found in data/incoming.")
            return

        for file_path in target_files:
            with open(file_path, "r", encoding="utf-8") as f:
                payload = json.load(f)

            data = extract_records(payload)

            updates = []
            for item in data:
                flight_key = build_flight_key(item)

                if not is_valid_flight_key(flight_key):
                    continue

                item.update(flight_key)

                updates.append(
                    UpdateOne(
                        flight_key,
                        {"$set": item},
                        upsert=True,
                    )
                )

            if updates:
                result = collection.bulk_write(updates)
                print(
                    f"File {file_path.name}: "
                    f"{result.upserted_count} inserted, {result.modified_count} updated."
                )

                shutil.move(str(file_path), str(PROCESSED_DIR / file_path.name))

    finally:
        client.close()


if __name__ == "__main__":
    sync_flights_to_mongo()