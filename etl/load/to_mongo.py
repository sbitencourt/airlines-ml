import json
import os
import shutil
from pathlib import Path
from pymongo import MongoClient, UpdateOne


# Path configurations
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")



def extract_records(payload):
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return payload["data"]
        return [payload]

    return []

def sync_airports_to_mongo():
    client = MongoClient(MONGODB_URI)
    db = client["dev"]
    collection = db["raw_airports"]

    # Ensure a unique index on the custom field
    collection.create_index("airport_id", unique=True)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    target_files = list(INCOMING_DIR.glob("airports_processed*.json"))

    if not target_files:
        print("No files found in data/incoming.")
        return

    for file_path in target_files:
        with open(file_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        
        data = extract_records(payload)

        updates = []
        for item in data:
            # Extract API 'id' and map it to our internal 'airport_id'
            api_id = item.get("id")
            if api_id:
                item["airport_id"] = api_id  # Explicit mapping

                updates.append(
                    UpdateOne(
                        {"airport_id": api_id},  # Match using our business key
                        {"$set": item},
                        upsert=True
                    )
                )

        if updates:
            result = collection.bulk_write(updates)
            print(
                f"File {file_path.name}: "
                f"{result.upserted_count} inserted, {result.modified_count} updated."
            )

            shutil.move(str(file_path), str(PROCESSED_DIR / file_path.name))

    client.close()

if __name__ == "__main__":
    sync_airports_to_mongo()