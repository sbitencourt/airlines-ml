import json
import requests
from pathlib import Path
from datetime import datetime
from typing import Any

# Import your credential utility
from dst_airlines.utils.get_tool_fernet import get_credentials

# ==========================================================
# Helpers
# ==========================================================

def prune(obj: Any) -> Any:
    """Recursively remove None, empty strings, lists, or dicts."""
    if isinstance(obj, dict):
        return {k: prune(v) for k, v in obj.items() if prune(v) not in (None, "", [], {})}
    if isinstance(obj, list):
        return [prune(x) for x in obj if prune(x) not in (None, "", [], {})]
    return obj

def save_to_incoming(data: Any, prefix: str) -> Path:
    """Saves data to ../../../data/incoming/ with a timestamp."""
    # Adjusted to match your project structure (3 levels up from this file)
    project_root = Path(__file__).resolve().parents[3]
    incoming_dir = project_root / "data" / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = incoming_dir / f"{prefix}_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return file_path

# ==========================================================
# Main Logic
# ==========================================================

def fetch_and_save_airports():
    host, token, _ = get_credentials()

    base_url = host.split("/v1/")[0]
    endpoint_url = f"{base_url}/v1/airports"

    limit = 100
    offset = 0

    all_cleaned_records = []
    all_raw_responses = []

    print(f"Fetching from: {endpoint_url}...")

    try:
        while True:
            params = {
                "access_key": token,
                "limit": limit,
                "offset": offset
            }

            print(f"Requesting page with offset={offset}...")

            response = requests.get(endpoint_url, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()

            # Handle API-level errors
            if "error" in payload:
                print(f"API Error: {payload['error'].get('message')}")
                break

            records = payload.get("data", [])

            # Stop condition: no more records returned
            if not records:
                print("No more data to fetch.")
                break

            # Clean records
            cleaned_records = [prune(record) for record in records if record]

            # Accumulate results
            all_cleaned_records.extend(cleaned_records)
            all_raw_responses.append(payload)

            print(f"Fetched {len(cleaned_records)} records.")

            # Move to next page
            offset += limit

        if not all_cleaned_records:
            print("No data collected.")
            return

        # Save consolidated results
        # raw_path = save_to_incoming(all_raw_responses, "aviationstack_airports_incoming")
        processed_path = save_to_incoming(all_cleaned_records, "airports_processed")

        print(f"\nTotal records saved: {len(all_cleaned_records)}")
        # print(f"Raw data file: {raw_path.name}")
        print(f"Processed data file: {processed_path.name}")

    except Exception as e:
        print(f"Failed to fetch airports: {e}")

if __name__ == "__main__":
    fetch_and_save_airports()