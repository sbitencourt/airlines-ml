from __future__ import annotations

from typing import Any, Dict, List


def prune(obj: Any) -> Any:
    if isinstance(obj, dict):
        cleaned = {}
        for key, value in obj.items():
            pruned_value = prune(value)
            if pruned_value in (None, "", [], {}):
                continue
            cleaned[key] = pruned_value
        return cleaned if cleaned else None

    if isinstance(obj, list):
        cleaned_list = [prune(item) for item in obj]
        cleaned_list = [item for item in cleaned_list if item not in (None, "", [], {})]
        return cleaned_list if cleaned_list else None

    return obj


def is_in_air(flight: Dict[str, Any]) -> bool:
    live = flight.get("live")

    if isinstance(live, dict) and "is_ground" in live:
        return live.get("is_ground") is False

    return flight.get("flight_status") == "active"


def extract_flights(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    results = payload.get("results")
    if isinstance(results, list):
        return [item for item in results if isinstance(item, dict)]

    return []


def extract_in_air_flights(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    flights = extract_flights(payload)

    extracted: List[Dict[str, Any]] = []
    for flight in flights:
        if is_in_air(flight):
            extracted.append(prune(flight) or flight)

    return extracted


def build_flights_metrics(payload: Dict[str, Any], extracted: List[Dict[str, Any]]) -> Dict[str, int]:
    raw_flights = extract_flights(payload)
    return {
        "raw_count": len(raw_flights),
        "extracted_count": len(extracted),
    }