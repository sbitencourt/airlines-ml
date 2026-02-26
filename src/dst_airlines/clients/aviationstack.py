from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import json
from collections import Counter

import requests

from dst_airlines.utils.get_tool_fernet import get_credentials


# ==========================================================
# Data structure to report detailed fetch status
# ==========================================================

@dataclass
class FetchStatus:
    connected: bool
    http_ok: bool
    json_ok: bool
    api_ok: bool
    has_flights_array: bool
    extracted_any: bool
    error_message: Optional[str] = None
    flights_extracted: Optional[List[Dict[str, Any]]] = None
    raw: Optional[Dict[str, Any]] = None  # for diagnostics


# ==========================================================
# Utility helpers
# ==========================================================

def prune(obj: Any) -> Any:
    """
    Recursively remove keys with:
        - None
        - empty dict
        - empty list
        - empty string
    """
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            pv = prune(v)
            if pv in (None, "", [], {}):
                continue
            cleaned[k] = pv
        return cleaned if cleaned else None

    if isinstance(obj, list):
        cleaned_list = [prune(x) for x in obj]
        cleaned_list = [x for x in cleaned_list if x not in (None, "", [], {})]
        return cleaned_list if cleaned_list else None

    return obj


def is_in_air(flight: Dict[str, Any]) -> bool:
    """
    Determine if a flight is currently in air.

    Priority:
    1. live.is_ground == False
    2. fallback: flight_status == "active"
    """
    live = flight.get("live")

    if isinstance(live, dict) and "is_ground" in live:
        return live.get("is_ground") is False

    return flight.get("flight_status") == "active"


# ==========================================================
# Core API calls
# ==========================================================

def fetch_partial_in_air(timeout: int = 15):
    """
    Simple fetch returning:
        - extracted in-air flights
        - raw payload
    """
    host, token, limit = get_credentials()

    params = {
        "access_key": token,
        "limit": limit,
    }

    resp = requests.get(host, params=params, timeout=timeout)
    resp.raise_for_status()

    payload = resp.json()

    if isinstance(payload, dict) and "error" in payload:
        raise RuntimeError(f"API error: {payload['error']}")

    flights = payload.get("data") or payload.get("results") or []
    extracted = []

    for f in flights:
        if isinstance(f, dict) and is_in_air(f):
            extracted.append(prune(f) or f)

    return extracted, payload


def fetch_in_air_flights(timeout: int = 15) -> FetchStatus:
    """
    Robust fetch with detailed status tracking.
    """
    status = FetchStatus(
        connected=False,
        http_ok=False,
        json_ok=False,
        api_ok=False,
        has_flights_array=False,
        extracted_any=False,
        flights_extracted=[],
    )

    host, token, limit = get_credentials()

    params = {
        "access_key": token,
        "limit": limit,
    }

    try:
        resp = requests.get(host, params=params, timeout=timeout)
        status.connected = True

        if resp.status_code != 200:
            status.error_message = f"HTTP error {resp.status_code}: {resp.text[:300]}"
            return status
        status.http_ok = True

        try:
            payload = resp.json()
        except ValueError:
            status.error_message = "Response is not valid JSON."
            return status

        status.json_ok = True
        status.raw = payload

        if isinstance(payload, dict) and "error" in payload:
            err = payload.get("error") or {}
            status.error_message = (
                f"API error: {err.get('code')} - {err.get('message')}"
            )
            return status

        status.api_ok = True

        flights = None
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                flights = payload["data"]
            elif isinstance(payload.get("results"), list):
                flights = payload["results"]

        if not flights:
            status.error_message = (
                'No flights list found (expected "data" or "results").'
            )
            return status

        status.has_flights_array = True

        extracted: List[Dict[str, Any]] = []

        for flight in flights:
            if isinstance(flight, dict) and is_in_air(flight):
                extracted.append(prune(flight) or flight)

        status.flights_extracted = extracted
        status.extracted_any = len(extracted) > 0

        if not status.extracted_any:
            status.error_message = (
                "Request succeeded, but no in-air flights matched criteria."
            )

        return status

    except requests.exceptions.Timeout:
        status.error_message = "Request timed out."
        return status

    except requests.exceptions.ConnectionError:
        status.error_message = "Connection error (could not reach the API)."
        return status

    except requests.exceptions.RequestException as e:
        status.error_message = f"Unexpected request error: {e}"
        return status


# ==========================================================
# Diagnostics
# ==========================================================

def debug_payload(payload: dict) -> None:
    flights = payload.get("data") or payload.get("results") or []
    print("Flights count:", len(flights))

    live_null = 0
    live_dict = 0
    status_counts = Counter()

    for f in flights:
        if not isinstance(f, dict):
            continue

        live = f.get("live")
        if live is None:
            live_null += 1
        elif isinstance(live, dict):
            live_dict += 1

        status_counts[f.get("flight_status")] += 1

    print("live is null:", live_null)
    print("live is dict:", live_dict)
    print("flight_status counts:", dict(status_counts))

    if flights:
        sample = flights[0]
        print("\nSample keys:", list(sample.keys()))
        print("Sample flight_status:", sample.get("flight_status"))
        print("Sample live:", sample.get("live"))


def print_status_and_sample(status: FetchStatus):
    if not status.connected:
        verdict = "FAIL (could not connect)"
    elif not status.http_ok:
        verdict = "FAIL (HTTP error)"
    elif not status.json_ok:
        verdict = "FAIL (invalid JSON)"
    elif not status.api_ok:
        verdict = "FAIL (API returned error)"
    elif not status.has_flights_array:
        verdict = 'FAIL (missing "data"/"results")'
    elif not status.extracted_any:
        verdict = "WARNING (no matches extracted)"
    else:
        verdict = "OK"

    print("=== STATUS SUMMARY ===")
    print("overall:", verdict)
    print("error_message:", status.error_message)

    print("\n=== SAMPLE ===")
    if not status.flights_extracted:
        print("(none)")
    else:
        for f in status.flights_extracted[:3]:
            print(json.dumps(f, indent=2, ensure_ascii=False))


from pathlib import Path
from datetime import datetime
import json


def save_raw_data(data: Any, filename_prefix: str = "dump") -> Path:
    """
    Save extracted flights into data/raw directory.
    File name includes timestamp for traceability.
    """

    # Project root = 3 levels above this file
    project_root = Path(__file__).resolve().parents[3]

    raw_dir = project_root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = raw_dir / f"{filename_prefix}_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return file_path



if __name__ == "__main__":
    print("Fetching in-air flights...\n")

    status = fetch_in_air_flights()
    print_status_and_sample(status)

 
    if status.raw:
        raw_path = save_raw_data(status.raw, filename_prefix="aviationstack_raw")
        print(f"\nRaw payload saved to: {raw_path}")


    if status.extracted_any:
        extracted_path = save_raw_data(status.flights_extracted, filename_prefix="in_air_flights")
        print(f"Extracted in-air flights saved to: {extracted_path}")
    else:
        print("\nNo in-air flights extracted, so no extracted file saved.")