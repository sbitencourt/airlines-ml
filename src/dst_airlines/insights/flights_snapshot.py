from collections import Counter
from datetime import datetime, timezone
from typing import Any


def extract_records(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return [item for item in payload["data"] if isinstance(item, dict)]

        if isinstance(payload.get("results"), list):
            return [item for item in payload["results"] if isinstance(item, dict)]

        return [payload]

    return []


def _is_positive_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and value > 0


def build_flight_snapshot_insights(
    payload: Any,
    run_id: str,
    source: str = "aviationstack",
    endpoint: str = "flights",
) -> dict:
    records = extract_records(payload)

    statuses = Counter()
    airlines = set()
    routes = set()
    departure_airports = set()
    arrival_airports = set()

    departure_delays = 0
    arrival_delays = 0
    delayed_flights = 0

    for item in records:
        flight = item.get("flight") or {}
        airline = item.get("airline") or {}
        departure = item.get("departure") or {}
        arrival = item.get("arrival") or {}

        status = (
            flight.get("flight_status")
            or item.get("flight_status")
            or "unknown"
        )
        statuses[status] += 1

        airline_iata = airline.get("iata")
        departure_iata = departure.get("iata")
        arrival_iata = arrival.get("iata")

        if airline_iata:
            airlines.add(airline_iata)

        if departure_iata:
            departure_airports.add(departure_iata)

        if arrival_iata:
            arrival_airports.add(arrival_iata)

        if departure_iata and arrival_iata:
            routes.add(f"{departure_iata}-{arrival_iata}")

        dep_delay = departure.get("delay")
        arr_delay = arrival.get("delay")

        has_dep_delay = _is_positive_number(dep_delay)
        has_arr_delay = _is_positive_number(arr_delay)

        if has_dep_delay:
            departure_delays += 1

        if has_arr_delay:
            arrival_delays += 1

        if has_dep_delay or has_arr_delay:
            delayed_flights += 1

    total = len(records)
    active = statuses.get("active", 0)

    return {
        "run_id": run_id,
        "snapshot_at": datetime.now(timezone.utc),
        "source": source,
        "endpoint": endpoint,

        "observed_flights_total": total,

        "observed_flights_active": active,
        "observed_flights_scheduled": statuses.get("scheduled", 0),
        "observed_flights_landed": statuses.get("landed", 0),
        "observed_flights_cancelled": statuses.get("cancelled", 0),
        "observed_flights_diverted": statuses.get("diverted", 0),
        "observed_flights_incident": statuses.get("incident", 0),
        "observed_flights_unknown": statuses.get("unknown", 0),

        "observed_flights_active_pct": round((active / total) * 100, 2) if total else 0,

        "observed_departure_delays_total": departure_delays,
        "observed_arrival_delays_total": arrival_delays,
        "observed_delayed_flights_total": delayed_flights,

        "observed_unique_airlines": len(airlines),
        "observed_unique_routes": len(routes),
        "observed_unique_departure_airports": len(departure_airports),
        "observed_unique_arrival_airports": len(arrival_airports),
    }