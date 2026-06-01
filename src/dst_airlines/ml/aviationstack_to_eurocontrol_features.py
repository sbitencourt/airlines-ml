"""
AviationStack to EUROCONTROL feature normalizer.

Purpose:
    Convert raw AviationStack flight documents into the exact feature schema used
    by the current ML model trained with historical EUROCONTROL-like variables.

Model feature contract:
    - Latitude
    - Longitude
    - Requested FL
    - Actual Distance Flown (nm)
    - AC Operator
    - month
    - hour
    - day_of_week

Design decision:
    For the model:
        Use departure airport coordinates and origin-destination distance.

    For Streamlit:
        live.latitude / live.longitude are not used as model features.
        They are preserved separately as operational flight information.
"""

from __future__ import annotations

import math
import os
from typing import Any

import pandas as pd


EUROCONTROL_FEATURE_COLUMNS = [
    "Latitude",
    "Longitude",
    "Requested FL",
    "Actual Distance Flown (nm)",
    "AC Operator",
    "month",
    "hour",
    "day_of_week",
]

DEFAULT_REQUESTED_FL = int(os.getenv("DEFAULT_REQUESTED_FL", "350"))

AIRPORTS_STATIC_CSV_PATH = os.getenv(
    "AIRPORTS_STATIC_CSV_PATH",
    "/app/data/airports.csv",
)

def extract_flight_records(
    payload: dict[str, Any] | list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Accept one MongoDB document, a list of documents, or a full AviationStack response."""
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]

    if isinstance(payload, dict):
        return [payload]

    return []


def get_nested_value(data: dict[str, Any], path: str, default: Any = None) -> Any:
    """Read a nested value from a dictionary using dot notation."""
    current: Any = data

    for key in path.split("."):
        if not isinstance(current, dict):
            return default

        current = current.get(key)

        if current is None:
            return default

    return current


def normalize_text(value: Any) -> str | None:
    """Normalize empty strings and NaN values to None."""
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    text = str(value).strip()
    return text or None


def safe_float(value: Any) -> float | None:
    """Convert a value to float when possible."""
    if value in (None, "", "null"):
        return None

    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    """Convert a value to int when possible."""
    value_float = safe_float(value)

    if value_float is None:
        return None

    return int(value_float)


def parse_datetime(value: Any) -> pd.Timestamp | None:
    """Parse datetime values as UTC timestamps."""
    if value in (None, "", "null"):
        return None

    parsed = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed


def parse_date(value: Any):
    """Parse a flight date value."""
    if value in (None, "", "null"):
        return None

    parsed = pd.to_datetime(value, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed.date()


def get_departure_iata(raw_flight: dict[str, Any]) -> str | None:
    """Return departure airport IATA from nested or flattened fields."""
    return normalize_text(
        get_nested_value(raw_flight, "departure.iata")
        or raw_flight.get("departure_iata")
    )


def get_arrival_iata(raw_flight: dict[str, Any]) -> str | None:
    """Return arrival airport IATA from nested or flattened fields."""
    return normalize_text(
        get_nested_value(raw_flight, "arrival.iata")
        or raw_flight.get("arrival_iata")
    )


def get_airline_iata(raw_flight: dict[str, Any]) -> str | None:
    """Return airline IATA from nested or flattened fields."""
    return normalize_text(
        get_nested_value(raw_flight, "airline.iata_code")
        or get_nested_value(raw_flight, "airline.iata")
        or raw_flight.get("airline_iata")
    )


def load_airport_coordinates_from_static_csv(
    path: str = AIRPORTS_STATIC_CSV_PATH,
) -> dict[str, tuple[float, float]]:
    """Load airport coordinates from the global airports CSV.

    Expected CSV columns:
        id, ident, type, name, latitude_deg, longitude_deg, elevation_ft,
        continent, iso_country, iso_region, municipality, scheduled_service,
        icao_code, iata_code, gps_code, local_code, home_link,
        wikipedia_link, keywords

    Output:
        {
            "CDG": (49.012798, 2.55),
            "JFK": (40.639801, -73.7789),
            ...
        }

    Notes:
        - Only rows with iata_code are useful for this project.
        - latitude_deg / longitude_deg are mapped to Latitude / Longitude.
        - Duplicate IATA codes are resolved by keeping the first valid record.
    """
    if not os.path.exists(path):
        print(f"[airport_coordinates] Static airport CSV not found: {path}")
        return {}

    try:
        airports_df = pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        airports_df = pd.read_csv(path, encoding="latin1")

    airports_df.columns = [str(column).strip() for column in airports_df.columns]

    required_columns = {"iata_code", "latitude_deg", "longitude_deg"}
    missing_columns = required_columns - set(airports_df.columns)

    if missing_columns:
        raise ValueError(
            f"Static airport CSV is missing columns: {sorted(missing_columns)}"
        )

    coordinates: dict[str, tuple[float, float]] = {}

    for _, row in airports_df.iterrows():
        iata = normalize_text(row.get("iata_code"))

        if not iata:
            continue

        latitude = safe_float(row.get("latitude_deg"))
        longitude = safe_float(row.get("longitude_deg"))

        if latitude is None or longitude is None:
            continue

        iata = iata.upper()

        # Keep the first valid coordinate found for each IATA.
        if iata not in coordinates:
            coordinates[iata] = (latitude, longitude)

    return coordinates


def get_airport_coordinate(
    airport_iata: str | None,
    airport_coordinates: dict[str, tuple[float, float]],
) -> tuple[float | None, float | None]:
    """Return airport latitude and longitude by IATA code."""
    if not airport_iata:
        return None, None

    coordinate = airport_coordinates.get(airport_iata.upper())

    if not coordinate:
        return None, None

    return coordinate


def haversine_distance_nm(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """Calculate great-circle distance in nautical miles."""
    earth_radius_nm = 3440.065

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad)
        * math.cos(lat2_rad)
        * math.sin(delta_lon / 2) ** 2
    )

    c = 2 * math.asin(math.sqrt(a))

    return earth_radius_nm * c


def infer_operator(raw_flight: dict[str, Any]) -> str | None:
    """Map AviationStack airline fields into EUROCONTROL AC Operator."""
    return normalize_text(
        get_nested_value(raw_flight, "airline.iata_code")
        or get_nested_value(raw_flight, "airline.iata")
        or raw_flight.get("airline_iata")
        or get_nested_value(raw_flight, "airline.icao_code")
        or get_nested_value(raw_flight, "airline.icao")
        or get_nested_value(raw_flight, "airline.airline_name")
        or get_nested_value(raw_flight, "airline.name")
    )


def infer_requested_fl(raw_flight: dict[str, Any]) -> int:
    """Map or impute EUROCONTROL Requested FL.

    AviationStack usually does not provide requested flight level.
    If an enriched/custom field exists, use it. Otherwise use DEFAULT_REQUESTED_FL.
    """
    requested_fl = (
        get_nested_value(raw_flight, "flight.requested_fl")
        or raw_flight.get("requested_fl")
        or raw_flight.get("requested_flight_level")
        or get_nested_value(raw_flight, "eurocontrol.requested_fl")
    )

    requested_fl_value = safe_float(requested_fl)

    if requested_fl_value is not None:
        return int(requested_fl_value)

    return DEFAULT_REQUESTED_FL


def infer_origin_coordinates(
    raw_flight: dict[str, Any],
    airport_coordinates: dict[str, tuple[float, float]],
) -> tuple[float | None, float | None]:
    """Infer EUROCONTROL origin Latitude and Longitude.

    Priority:
        1. Departure airport coordinates from static CSV.
        2. Enriched departure latitude/longitude if present.

    Important:
        live.latitude/live.longitude are intentionally not used here because
        the current model expects origin trajectory coordinates, not the current
        position of an active aircraft.
    """
    departure_iata = get_departure_iata(raw_flight)

    lat, lon = get_airport_coordinate(departure_iata, airport_coordinates)

    if lat is not None and lon is not None:
        return lat, lon

    departure_lat = safe_float(
        get_nested_value(raw_flight, "departure.latitude")
        or get_nested_value(raw_flight, "departure.lat")
        or raw_flight.get("departure_latitude")
    )
    departure_lon = safe_float(
        get_nested_value(raw_flight, "departure.longitude")
        or get_nested_value(raw_flight, "departure.lon")
        or raw_flight.get("departure_longitude")
    )

    if departure_lat is not None and departure_lon is not None:
        return departure_lat, departure_lon

    return None, None


def infer_distance_flown_nm(
    raw_flight: dict[str, Any],
    airport_coordinates: dict[str, tuple[float, float]],
) -> float | None:
    """Map or approximate EUROCONTROL Actual Distance Flown (nm).

    Priority:
        1. Use enriched/custom distance field if available.
        2. Approximate with departure-arrival great-circle distance.

    Important:
        live latitude/longitude are intentionally not used to calculate this
        value because the current model expects route-level distance, not
        distance already flown at the moment of API extraction.
    """
    distance = (
        get_nested_value(raw_flight, "flight.actual_distance_flown_nm")
        or raw_flight.get("actual_distance_flown_nm")
        or raw_flight.get("distance_nm")
        or get_nested_value(raw_flight, "eurocontrol.actual_distance_flown_nm")
    )

    distance_value = safe_float(distance)

    if distance_value is not None:
        return distance_value

    departure_iata = get_departure_iata(raw_flight)
    arrival_iata = get_arrival_iata(raw_flight)

    dep_lat, dep_lon = get_airport_coordinate(departure_iata, airport_coordinates)
    arr_lat, arr_lon = get_airport_coordinate(arrival_iata, airport_coordinates)

    if None in (dep_lat, dep_lon, arr_lat, arr_lon):
        return None

    return haversine_distance_nm(dep_lat, dep_lon, arr_lat, arr_lon)


def build_eurocontrol_features_from_aviationstack(
    raw_flight: dict[str, Any],
    airport_coordinates: dict[str, tuple[float, float]] | None = None,
) -> dict[str, Any]:
    """Convert one AviationStack flight document into EUROCONTROL model features."""
    airport_coordinates = airport_coordinates or {}

    departure_scheduled = parse_datetime(
        get_nested_value(raw_flight, "departure.scheduled")
        or raw_flight.get("departure_scheduled")
    )

    if departure_scheduled is not None:
        month = int(departure_scheduled.month)
        hour = int(departure_scheduled.hour)

        # Spark F.dayofweek convention from notebook:
        # 1 = Sunday, 2 = Monday, ..., 7 = Saturday.
        day_of_week = int((departure_scheduled.dayofweek + 1) % 7 + 1)
    else:
        month = None
        hour = None
        day_of_week = None

    latitude, longitude = infer_origin_coordinates(raw_flight, airport_coordinates)
    distance_nm = infer_distance_flown_nm(raw_flight, airport_coordinates)

    return {
        "Latitude": latitude,
        "Longitude": longitude,
        "Requested FL": infer_requested_fl(raw_flight),
        "Actual Distance Flown (nm)": distance_nm,
        "AC Operator": infer_operator(raw_flight),
        "month": month,
        "hour": hour,
        "day_of_week": day_of_week,
    }


def build_feature_index_mapping_from_coordinates(
    raw_flights: list[dict[str, Any]],
    airport_coordinates: dict[str, tuple[float, float]],
) -> tuple[pd.DataFrame, list[int]]:
    """Build valid features using an already loaded airport coordinate dictionary."""
    rows: list[dict[str, Any]] = []
    valid_indexes: list[int] = []

    for index, raw_flight in enumerate(raw_flights):
        row = build_eurocontrol_features_from_aviationstack(
            raw_flight=raw_flight,
            airport_coordinates=airport_coordinates,
        )

        if all(row.get(column) is not None for column in EUROCONTROL_FEATURE_COLUMNS):
            rows.append(row)
            valid_indexes.append(index)

    features_df = pd.DataFrame(rows)

    if features_df.empty:
        return pd.DataFrame(columns=EUROCONTROL_FEATURE_COLUMNS), []

    return features_df[EUROCONTROL_FEATURE_COLUMNS], valid_indexes


def extract_live_info(raw_flight: dict[str, Any]) -> dict[str, Any]:
    """Extract live operational data for Streamlit display, not for model features."""
    return {
        "live_latitude": safe_float(get_nested_value(raw_flight, "live.latitude")),
        "live_longitude": safe_float(get_nested_value(raw_flight, "live.longitude")),
        "live_altitude": safe_float(get_nested_value(raw_flight, "live.altitude")),
        "live_direction": safe_float(get_nested_value(raw_flight, "live.direction")),
        "live_speed_horizontal": safe_float(
            get_nested_value(raw_flight, "live.speed_horizontal")
        ),
        "live_speed_vertical": safe_float(
            get_nested_value(raw_flight, "live.speed_vertical")
        ),
        "live_is_ground": get_nested_value(raw_flight, "live.is_ground"),
        "live_updated": parse_datetime(get_nested_value(raw_flight, "live.updated")),
    }