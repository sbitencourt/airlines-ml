from __future__ import annotations

from typing import Any, Dict, List

from dst_airlines.utils.normalize import prune

# List of French domestic airport IATA codes
FRENCH_AIRPORTS_IATA = {
    "CDG", "ORY", "NCE", "MRS", "LYS", "TLS", "BOD", "NTE",
    "SXB", "MPL", "BIA", "AJA", "BES", "RNS", "PUF", "LIL", "BIQ"
}


def is_french_domestic(flight: Dict[str, Any]) -> bool:
    """
    Validates whether both departure and arrival airports are located in France.
    """
    departure = flight.get("departure") or {}
    arrival = flight.get("arrival") or {}

    dep_iata = (departure.get("iata") or "").upper()
    arr_iata = (arrival.get("iata") or "").upper()

    return dep_iata in FRENCH_AIRPORTS_IATA and arr_iata in FRENCH_AIRPORTS_IATA


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
        if is_french_domestic(flight) and is_in_air(flight):
            extracted.append(prune(flight) or flight)

    return extracted


def build_flights_metrics(payload: Dict[str, Any], extracted: List[Dict[str, Any]]) -> Dict[str, int]:
    raw_flights = extract_flights(payload)
    return {
        "raw_count": len(raw_flights),
        "extracted_count": len(extracted),
    }