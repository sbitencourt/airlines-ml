from typing import Any

import pandas as pd


PREFERRED_FLIGHT_COLUMNS = [
    "flight_date",
    "flight_status",

    "flight.iata",
    "flight.icao",
    "flight.number",

    "airline.airline_name",
    "airline.name",
    "airline.iata_code",
    "airline.icao_code",
    "airline.country_name",
    "airline.status",
    "airline.type",

    "departure.airport",
    "departure.timezone",
    "departure.iata",
    "departure.icao",
    "departure.terminal",
    "departure.gate",
    "departure.delay",
    "departure.scheduled",
    "departure.estimated",
    "departure.actual",
    "departure.estimated_runway",
    "departure.actual_runway",
    "departure.baggage",

    "arrival.airport",
    "arrival.timezone",
    "arrival.iata",
    "arrival.icao",
    "arrival.terminal",
    "arrival.gate",
    "arrival.delay",
    "arrival.scheduled",
    "arrival.estimated",
    "arrival.actual",
    "arrival.estimated_runway",
    "arrival.actual_runway",
    "arrival.baggage",

    "aircraft.registration",
    "aircraft.iata",
    "aircraft.icao",
    "aircraft.icao24",

    "live.updated",
    "live.latitude",
    "live.longitude",
    "live.altitude",
    "live.direction",
    "live.speed_horizontal",
    "live.speed_vertical",
    "live.is_ground",
]


def normalize_flights(data: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert nested flight JSON records into a flat pandas DataFrame."""
    if not data:
        return pd.DataFrame()

    df = pd.json_normalize(data)

    existing_preferred = [col for col in PREFERRED_FLIGHT_COLUMNS if col in df.columns]
    other_columns = [col for col in df.columns if col not in existing_preferred]

    return df[existing_preferred + other_columns]


def find_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first candidate column that exists in the DataFrame."""
    for column in candidates:
        if column in df.columns:
            return column

    return None


def calculate_status_kpis(
    df: pd.DataFrame,
    status_column: str | None,
) -> tuple[int, int, int]:
    """Calculate active, delayed and scheduled flight counters."""
    if df.empty or not status_column or status_column not in df.columns:
        return 0, 0, 0

    statuses = df[status_column].astype(str).str.lower()

    active_count = int((statuses == "active").sum())
    delayed_count = int(
        statuses.str.contains(
            "delay|delayed|late|demorado|atrasado",
            na=False,
            regex=True,
        ).sum()
    )
    scheduled_count = int((statuses == "scheduled").sum())

    return active_count, delayed_count, scheduled_count


def get_source_config(data_source: str) -> tuple[str, str]:
    """Map a UI data source option to its FastAPI endpoint and description."""
    if data_source == "MongoDB raw flights":
        return (
            "/mongo/flights/latest",
            "MongoDB contains raw semi-structured flight records ingested from AviationStack. "
            "This source is useful to validate ingestion and recent operational data.",
        )

    return (
        "/flights/latest",
        "SQL contains curated flight data prepared for business consumption, dashboards, "
        "and analytical use cases.",
    )


def build_flight_search_mask(df: pd.DataFrame, search_value: str) -> pd.Series:
    """Build a boolean mask to find a specific flight by IATA, ICAO, number or text fields."""
    if df.empty or not search_value:
        return pd.Series([False] * len(df), index=df.index)

    search_value = search_value.strip().lower()

    candidate_columns = [
        "flight.iata",
        "flight.icao",
        "flight.number",
        "flight_number",
        "iata",
        "icao",
        "number",
    ]

    available_candidate_columns = [
        column for column in candidate_columns if column in df.columns
    ]

    if available_candidate_columns:
        mask = pd.Series([False] * len(df), index=df.index)

        for column in available_candidate_columns:
            column_values = df[column].fillna("").astype(str).str.lower()
            mask = mask | column_values.str.contains(search_value, na=False, regex=False)

        return mask

    return df.astype(str).apply(
        lambda row: row.str.lower().str.contains(search_value, na=False, regex=False).any(),
        axis=1,
    )


def get_display_value(row: pd.Series, candidates: list[str], default: str = "Not available") -> str:
    """Return the first non-empty value found in a row for the candidate columns."""
    for column in candidates:
        if column in row.index:
            value = row.get(column)

            if pd.notna(value) and str(value).strip():
                return str(value)

    return default


def infer_flight_status_message(row: pd.Series) -> str:
    """Create a readable status message for a selected flight."""
    status = get_display_value(row, ["flight_status", "status", "flight.status"])
    departure_delay = get_display_value(
        row,
        ["departure.delay", "departure_delay"],
        default="0",
    )
    arrival_delay = get_display_value(
        row,
        ["arrival.delay", "arrival_delay"],
        default="0",
    )

    status_lower = status.lower()

    if "delay" in status_lower or "late" in status_lower:
        return "The flight appears to be delayed according to its reported status."

    try:
        departure_delay_minutes = int(float(departure_delay))
        arrival_delay_minutes = int(float(arrival_delay))
    except ValueError:
        departure_delay_minutes = 0
        arrival_delay_minutes = 0

    if departure_delay_minutes > 0 or arrival_delay_minutes > 0:
        return (
            "The flight has delay information reported. "
            f"Departure delay: {departure_delay_minutes} min. "
            f"Arrival delay: {arrival_delay_minutes} min."
        )

    if status_lower in {"active", "scheduled", "landed"}:
        return f"The flight status is currently reported as '{status}'."

    return "The flight status could not be fully interpreted from the available fields."
