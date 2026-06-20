"""
AviationStack to EUROCONTROL feature normalizer.

Purpose
-------
Convert raw AviationStack flight documents into the exact feature schema used
by the delay prediction model trained with historical EUROCONTROL-like data.

Model feature contract
----------------------
- Latitude
- Longitude
- Requested FL
- Actual Distance Flown (nm)
- AC Operator
- month
- hour
- day_of_week

Important design decision
-------------------------
The corrected training script uses departure-airport coordinates and an
origin-destination great-circle distance estimate. This normalizer applies the
same logic at inference time, so the model sees features built consistently in
training and production.

Notes
-----
- live.latitude / live.longitude are not used as model features.
- They are preserved separately as operational display information for Streamlit.
- AviationStack usually provides airline IATA codes, but the EUROCONTROL
  historical field "AC Operator" commonly uses ICAO operator codes such as
  DAL, AFR, BAW, PGT. Therefore this script prioritizes ICAO when available.
"""

from __future__ import annotations

import math
import os
from functools import lru_cache
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

# By default, production uses the same distance logic as training:
# origin-destination estimated distance. Set this to true only if you have a
# trusted enriched field that was also used during model training.
USE_ENRICHED_DISTANCE_IF_AVAILABLE = os.getenv(
    "USE_ENRICHED_DISTANCE_IF_AVAILABLE",
    "false",
).strip().lower() in {"1", "true", "yes", "y"}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def extract_flight_records(
    payload: dict[str, Any] | list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Accept one MongoDB document, a list of documents, or an AviationStack response."""
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]

    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return [record for record in payload["data"] if isinstance(record, dict)]

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


def normalize_text(value: Any, *, uppercase: bool = False) -> str | None:
    """Normalize empty strings and NaN values to None."""
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    text = str(value).strip()
    if not text:
        return None

    return text.upper() if uppercase else text


def safe_float(value: Any) -> float | None:
    """Convert a value to float when possible."""
    if value in (None, "", "null", "None"):
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
    """Parse datetime values as UTC timestamps when possible."""
    if value in (None, "", "null", "None"):
        return None

    parsed = pd.to_datetime(value, utc=True, dayfirst=True, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed


def parse_date(value: Any):
    """Parse a flight date value."""
    if value in (None, "", "null", "None"):
        return None

    parsed = pd.to_datetime(value, dayfirst=True, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed.date()


def spark_day_of_week(timestamp: pd.Timestamp) -> int:
    """Return Spark dayofweek convention: 1=Sunday, 2=Monday, ..., 7=Saturday."""
    return int((timestamp.dayofweek + 1) % 7 + 1)


# ---------------------------------------------------------------------------
# AviationStack field extraction
# ---------------------------------------------------------------------------


def get_airline_iata(raw_flight: dict[str, Any]) -> str | None:
    """Return airline IATA from nested or flattened fields.

    Kept for backward compatibility with run_predictions.py.
    The ML feature 'AC Operator' should still be generated through infer_operator(),
    which prioritizes ICAO when available.
    """
    return normalize_text(
        get_nested_value(raw_flight, "airline.iata_code")
        or get_nested_value(raw_flight, "airline.iata")
        or raw_flight.get("airline_iata")
    )


def get_departure_iata(raw_flight: dict[str, Any]) -> str | None:
    """Return departure airport IATA from nested or flattened fields."""
    return normalize_text(
        get_nested_value(raw_flight, "departure.iata")
        or get_nested_value(raw_flight, "departure.iata_code")
        or raw_flight.get("departure_iata")
        or raw_flight.get("departure_iata_code"),
        uppercase=True,
    )


def get_arrival_iata(raw_flight: dict[str, Any]) -> str | None:
    """Return arrival airport IATA from nested or flattened fields."""
    return normalize_text(
        get_nested_value(raw_flight, "arrival.iata")
        or get_nested_value(raw_flight, "arrival.iata_code")
        or raw_flight.get("arrival_iata")
        or raw_flight.get("arrival_iata_code"),
        uppercase=True,
    )


def get_departure_icao(raw_flight: dict[str, Any]) -> str | None:
    """Return departure airport ICAO from nested or flattened fields when available."""
    return normalize_text(
        get_nested_value(raw_flight, "departure.icao")
        or get_nested_value(raw_flight, "departure.icao_code")
        or raw_flight.get("departure_icao")
        or raw_flight.get("departure_icao_code"),
        uppercase=True,
    )


def get_arrival_icao(raw_flight: dict[str, Any]) -> str | None:
    """Return arrival airport ICAO from nested or flattened fields when available."""
    return normalize_text(
        get_nested_value(raw_flight, "arrival.icao")
        or get_nested_value(raw_flight, "arrival.icao_code")
        or raw_flight.get("arrival_icao")
        or raw_flight.get("arrival_icao_code"),
        uppercase=True,
    )


def infer_operator(raw_flight: dict[str, Any]) -> str | None:
    """Map AviationStack airline fields into EUROCONTROL AC Operator.

    EUROCONTROL AC Operator is usually an ICAO operator code, for example:
    DAL, AFR, BAW, PGT. Therefore ICAO is prioritized over IATA.
    """
    return normalize_text(
        get_nested_value(raw_flight, "airline.icao_code")
        or get_nested_value(raw_flight, "airline.icao")
        or raw_flight.get("airline_icao_code")
        or raw_flight.get("airline_icao")
        or get_nested_value(raw_flight, "airline.iata_code")
        or get_nested_value(raw_flight, "airline.iata")
        or raw_flight.get("airline_iata_code")
        or raw_flight.get("airline_iata")
        or get_nested_value(raw_flight, "airline.airline_name")
        or get_nested_value(raw_flight, "airline.name"),
        uppercase=True,
    )


def infer_requested_fl(raw_flight: dict[str, Any]) -> int:
    """Map or impute EUROCONTROL Requested FL.

    AviationStack usually does not provide requested flight level. If an enriched
    custom field exists, it is used. Otherwise DEFAULT_REQUESTED_FL is returned.
    """
    requested_fl = (
        get_nested_value(raw_flight, "flight.requested_fl")
        or get_nested_value(raw_flight, "flight.requested_flight_level")
        or get_nested_value(raw_flight, "flight.flight_level")
        or raw_flight.get("requested_fl")
        or raw_flight.get("requested_flight_level")
        or raw_flight.get("flight_level")
        or get_nested_value(raw_flight, "eurocontrol.requested_fl")
    )

    requested_fl_value = safe_float(requested_fl)

    if requested_fl_value is not None:
        return int(requested_fl_value)

    return DEFAULT_REQUESTED_FL


def infer_departure_datetime(raw_flight: dict[str, Any]) -> pd.Timestamp | None:
    """Infer the scheduled departure datetime used for calendar features."""
    return parse_datetime(
        get_nested_value(raw_flight, "departure.scheduled")
        or raw_flight.get("departure_scheduled")
        or get_nested_value(raw_flight, "departure.estimated")
        or raw_flight.get("departure_estimated")
        or get_nested_value(raw_flight, "departure.actual")
        or raw_flight.get("departure_actual")
    )


# ---------------------------------------------------------------------------
# Airport coordinates and distance
# ---------------------------------------------------------------------------


@lru_cache(maxsize=4)
def load_airport_coordinates_from_static_csv(
    path: str = AIRPORTS_STATIC_CSV_PATH,
) -> dict[str, tuple[float, float]]:
    """Load airport coordinates from a static global airports CSV.

    Expected CSV columns include:
        iata_code, icao_code, latitude_deg, longitude_deg

    Output keys include both IATA and ICAO codes when available:
        {
            "CDG": (49.012798, 2.55),
            "LFPG": (49.012798, 2.55),
            ...
        }
    """
    if not os.path.exists(path):
        print(f"[airport_coordinates] Static airport CSV not found: {path}")
        return {}

    try:
        airports_df = pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        airports_df = pd.read_csv(path, encoding="latin1")

    airports_df.columns = [str(column).strip() for column in airports_df.columns]

    required_columns = {"latitude_deg", "longitude_deg"}
    missing_columns = required_columns - set(airports_df.columns)

    if missing_columns:
        raise ValueError(
            f"Static airport CSV is missing columns: {sorted(missing_columns)}"
        )

    coordinates: dict[str, tuple[float, float]] = {}

    for _, row in airports_df.iterrows():
        latitude = safe_float(row.get("latitude_deg"))
        longitude = safe_float(row.get("longitude_deg"))

        if latitude is None or longitude is None:
            continue

        for code_column in ("iata_code", "icao_code", "gps_code", "ident"):
            airport_code = normalize_text(row.get(code_column), uppercase=True)

            if not airport_code:
                continue

            # Keep the first valid coordinate found for each code.
            coordinates.setdefault(airport_code, (latitude, longitude))

    return coordinates


def get_airport_coordinate(
    airport_code: str | None,
    airport_coordinates: dict[str, tuple[float, float]],
) -> tuple[float | None, float | None]:
    """Return airport latitude and longitude by IATA or ICAO code."""
    if not airport_code:
        return None, None

    coordinate = airport_coordinates.get(airport_code.upper())

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


def infer_origin_coordinates(
    raw_flight: dict[str, Any],
    airport_coordinates: dict[str, tuple[float, float]],
) -> tuple[float | None, float | None]:
    """Infer EUROCONTROL origin Latitude and Longitude.

    Priority:
        1. Departure IATA in static CSV.
        2. Departure ICAO in static CSV.
        3. Enriched departure latitude/longitude if present.
    """
    for airport_code in (get_departure_iata(raw_flight), get_departure_icao(raw_flight)):
        lat, lon = get_airport_coordinate(airport_code, airport_coordinates)
        if lat is not None and lon is not None:
            return lat, lon

    departure_lat = safe_float(
        get_nested_value(raw_flight, "departure.latitude")
        or get_nested_value(raw_flight, "departure.lat")
        or raw_flight.get("departure_latitude")
        or raw_flight.get("departure_lat")
    )
    departure_lon = safe_float(
        get_nested_value(raw_flight, "departure.longitude")
        or get_nested_value(raw_flight, "departure.lon")
        or raw_flight.get("departure_longitude")
        or raw_flight.get("departure_lon")
    )

    if departure_lat is not None and departure_lon is not None:
        return departure_lat, departure_lon

    return None, None


def infer_destination_coordinates(
    raw_flight: dict[str, Any],
    airport_coordinates: dict[str, tuple[float, float]],
) -> tuple[float | None, float | None]:
    """Infer destination airport latitude and longitude."""
    for airport_code in (get_arrival_iata(raw_flight), get_arrival_icao(raw_flight)):
        lat, lon = get_airport_coordinate(airport_code, airport_coordinates)
        if lat is not None and lon is not None:
            return lat, lon

    arrival_lat = safe_float(
        get_nested_value(raw_flight, "arrival.latitude")
        or get_nested_value(raw_flight, "arrival.lat")
        or raw_flight.get("arrival_latitude")
        or raw_flight.get("arrival_lat")
    )
    arrival_lon = safe_float(
        get_nested_value(raw_flight, "arrival.longitude")
        or get_nested_value(raw_flight, "arrival.lon")
        or raw_flight.get("arrival_longitude")
        or raw_flight.get("arrival_lon")
    )

    if arrival_lat is not None and arrival_lon is not None:
        return arrival_lat, arrival_lon

    return None, None


def infer_enriched_distance_nm(raw_flight: dict[str, Any]) -> float | None:
    """Read an enriched distance field when one is explicitly available."""
    distance = (
        get_nested_value(raw_flight, "flight.actual_distance_flown_nm")
        or get_nested_value(raw_flight, "flight.route_distance_nm")
        or raw_flight.get("actual_distance_flown_nm")
        or raw_flight.get("route_distance_nm")
        or raw_flight.get("distance_nm")
        or get_nested_value(raw_flight, "eurocontrol.actual_distance_flown_nm")
        or get_nested_value(raw_flight, "eurocontrol.route_distance_nm")
    )

    return safe_float(distance)


def infer_distance_flown_nm(
    raw_flight: dict[str, Any],
    airport_coordinates: dict[str, tuple[float, float]],
) -> float | None:
    """Infer the model distance feature.

    For consistency with the corrected training script, this function defaults to
    origin-destination great-circle distance. The output keeps the historical
    feature name "Actual Distance Flown (nm)" to avoid breaking the trained
    model pipeline contract.
    """
    if USE_ENRICHED_DISTANCE_IF_AVAILABLE:
        enriched_distance = infer_enriched_distance_nm(raw_flight)
        if enriched_distance is not None:
            return enriched_distance

    dep_lat, dep_lon = infer_origin_coordinates(raw_flight, airport_coordinates)
    arr_lat, arr_lon = infer_destination_coordinates(raw_flight, airport_coordinates)

    if None in (dep_lat, dep_lon, arr_lat, arr_lon):
        return None

    return haversine_distance_nm(dep_lat, dep_lon, arr_lat, arr_lon)


# ---------------------------------------------------------------------------
# Feature builder
# ---------------------------------------------------------------------------


def build_eurocontrol_features_from_aviationstack(
    raw_flight: dict[str, Any],
    airport_coordinates: dict[str, tuple[float, float]] | None = None,
) -> dict[str, Any]:
    """Convert one AviationStack flight document into EUROCONTROL model features."""
    airport_coordinates = airport_coordinates or {}

    departure_scheduled = infer_departure_datetime(raw_flight)

    if departure_scheduled is not None:
        month = int(departure_scheduled.month)
        hour = int(departure_scheduled.hour)
        day_of_week = spark_day_of_week(departure_scheduled)
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


def get_missing_feature_columns(feature_row: dict[str, Any]) -> list[str]:
    """Return the feature columns that are missing or null."""
    return [
        column
        for column in EUROCONTROL_FEATURE_COLUMNS
        if feature_row.get(column) is None
    ]


def build_feature_index_mapping_from_coordinates(
    raw_flights: list[dict[str, Any]],
    airport_coordinates: dict[str, tuple[float, float]],
) -> tuple[pd.DataFrame, list[int]]:
    """Build valid model features and keep the index mapping to raw records."""
    rows: list[dict[str, Any]] = []
    valid_indexes: list[int] = []

    for index, raw_flight in enumerate(raw_flights):
        row = build_eurocontrol_features_from_aviationstack(
            raw_flight=raw_flight,
            airport_coordinates=airport_coordinates,
        )

        if not get_missing_feature_columns(row):
            rows.append(row)
            valid_indexes.append(index)

    features_df = pd.DataFrame(rows)

    if features_df.empty:
        return pd.DataFrame(columns=EUROCONTROL_FEATURE_COLUMNS), []

    return features_df[EUROCONTROL_FEATURE_COLUMNS], valid_indexes


def build_feature_index_mapping(
    raw_flights: list[dict[str, Any]],
    airports_csv_path: str = AIRPORTS_STATIC_CSV_PATH,
) -> tuple[pd.DataFrame, list[int]]:
    """Build valid model features loading the airport coordinates internally."""
    airport_coordinates = load_airport_coordinates_from_static_csv(airports_csv_path)
    return build_feature_index_mapping_from_coordinates(raw_flights, airport_coordinates)


# ---------------------------------------------------------------------------
# Streamlit display helpers
# ---------------------------------------------------------------------------


def extract_live_info(raw_flight: dict[str, Any]) -> dict[str, Any]:
    """Extract live operational data for display, not for model features."""
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


def extract_operational_info(raw_flight: dict[str, Any]) -> dict[str, Any]:
    """Extract non-model flight fields useful for Streamlit display/debugging."""
    return {
        "flight_iata": normalize_text(
            get_nested_value(raw_flight, "flight.iata")
            or raw_flight.get("flight_iata"),
            uppercase=True,
        ),
        "flight_icao": normalize_text(
            get_nested_value(raw_flight, "flight.icao")
            or raw_flight.get("flight_icao"),
            uppercase=True,
        ),
        "departure_iata": get_departure_iata(raw_flight),
        "departure_icao": get_departure_icao(raw_flight),
        "arrival_iata": get_arrival_iata(raw_flight),
        "arrival_icao": get_arrival_icao(raw_flight),
        "airline_operator": infer_operator(raw_flight),
        "departure_scheduled": infer_departure_datetime(raw_flight),
    }
