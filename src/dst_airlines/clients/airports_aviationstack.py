import json
import requests
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

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
    host, token, limit = get_credentials()

    # URL Correction: Ensure it hits /v1/airports directly
    base_url = host.split("/v1/")[0]
    final_url = f"{base_url}/v1/airports"

    params = {
        "access_key": token,
        "limit": limit
    }

    print(f"Fetching from: {final_url}...")

    try:
        response = requests.get(final_url, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()

        if "error" in payload:
            print(f"API Error: {payload['error'].get('message')}")
            return

        # Extract and Prune
        incoming_data = payload.get("data", [])
        if not incoming_data:
            print("No data found in response.")
            return

        cleaned_data = [prune(item) for item in incoming_data if item]

        # Save both incoming and Cleaned (Optional)
        incoming_path = save_to_incoming(payload, "aviationstack_airports_incoming")
        clean_path = save_to_incoming(cleaned_data, "airports_processed")

        print(f"Successfully saved {len(cleaned_data)} airports.")
        print(f"incoming: {incoming_path.name}")
        print(f"Cleaned: {clean_path.name}")

    except Exception as e:
        print(f"Failed to fetch airports: {e}")

if __name__ == "__main__":
    fetch_and_save_airports()