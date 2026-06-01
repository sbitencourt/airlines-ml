"""
Batch prediction pipeline using the current EUROCONTROL-trained ML model.

Responsibility:
1. Read recent AviationStack raw flights from MongoDB.
2. Normalize AviationStack records into the EUROCONTROL feature schema.
3. Load the trained ML model.
4. Apply delay prediction.
5. Save prediction results into PostgreSQL.

Recommended execution:
    python -m dst_airlines.pipelines.run_predictions
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import joblib
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values
from pymongo import DESCENDING

from dst_airlines.api.config import get_settings
from dst_airlines.api.db import get_mongo_client, postgres_connection
from dst_airlines.ml.aviationstack_to_eurocontrol_features import (
    build_feature_index_mapping_from_coordinates,
    extract_flight_records,
    extract_live_info,
    get_airline_iata,
    get_arrival_iata,
    get_departure_iata,
    get_nested_value,
    load_airport_coordinates_from_static_csv,
    normalize_text,
    parse_date,
    parse_datetime,
    safe_int,
)


settings = get_settings()

DELAY_MODEL_PATH = os.getenv("DELAY_MODEL_PATH", "/app/models/delay_model.joblib")
MODEL_NAME = os.getenv("MODEL_NAME", "eurocontrol_delay_classifier")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1")
PREDICTION_LIMIT = int(os.getenv("PREDICTION_LIMIT", "100"))
FLIGHT_PREDICTIONS_TABLE = os.getenv("FLIGHT_PREDICTIONS_TABLE", "flight_predictions")

AIRPORTS_STATIC_CSV_PATH = os.getenv(
    "AIRPORTS_STATIC_CSV_PATH",
    "/app/data/airports.csv",
)

def clean_value(value: Any) -> Any:
    """Convert pandas/numpy scalar values into PostgreSQL-friendly Python values."""
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    try:
        return value.item()
    except AttributeError:
        return value


def read_recent_raw_flights(limit: int = PREDICTION_LIMIT) -> list[dict[str, Any]]:
    """Read recent raw AviationStack flight documents from MongoDB."""
    client = get_mongo_client()
    db = client[settings.mongodb_db]
    collection = db[settings.mongodb_collection_flights]

    sort_candidates = [
        "_meta.loaded_at",
        "ingested_at",
        "created_at",
        "flight_date",
        "departure.scheduled",
    ]

    for sort_field in sort_candidates:
        try:
            documents = list(
                collection.find({}, {"_id": 0})
                .sort(sort_field, DESCENDING)
                .limit(limit)
            )
            return [
                record
                for document in documents
                for record in extract_flight_records(document)
            ]
        except Exception:
            continue

    documents = list(collection.find({}, {"_id": 0}).limit(limit))

    return [
        record
        for document in documents
        for record in extract_flight_records(document)
    ]


def load_delay_model(model_path: str = DELAY_MODEL_PATH) -> Any:
    """Load the trained EUROCONTROL-compatible model."""
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"Delay model not found at {model_path}. "
            "Set DELAY_MODEL_PATH or generate the model artifact first."
        )

    return joblib.load(model_path)


def predict_delay(
    model: Any,
    features_df: pd.DataFrame,
) -> tuple[list[bool], list[float | None]]:
    """Apply prediction using the already-trained model."""
    if features_df.empty:
        return [], []

    predicted_labels = model.predict(features_df)
    predicted_is_delayed = [bool(label) for label in predicted_labels]

    if hasattr(model, "predict_proba"):
        predicted_proba = model.predict_proba(features_df)
        probabilities = [float(row[1]) for row in predicted_proba]
    else:
        probabilities = [None] * len(predicted_is_delayed)

    return predicted_is_delayed, probabilities


def build_prediction_label(
    predicted_is_delayed: bool,
    delay_probability: float | None,
) -> str:
    """Build a user-facing risk label."""
    if delay_probability is None:
        return "Delayed" if predicted_is_delayed else "On time"

    if delay_probability >= 0.70:
        return "High risk of delay"

    if delay_probability >= 0.40:
        return "Medium risk of delay"

    return "Low risk of delay"


def build_serving_record(
    raw_flight: dict[str, Any],
    model_features: dict[str, Any],
    predicted_is_delayed: bool,
    delay_probability: float | None,
) -> dict[str, Any]:
    """Build the enriched PostgreSQL serving record."""
    live_info = extract_live_info(raw_flight)

    prediction_label = build_prediction_label(
        predicted_is_delayed=predicted_is_delayed,
        delay_probability=delay_probability,
    )

    return {
        "flight_iata": normalize_text(
            get_nested_value(raw_flight, "flight.iata")
            or raw_flight.get("flight_iata")
            or raw_flight.get("flight_number")
        ),
        "flight_icao": normalize_text(
            get_nested_value(raw_flight, "flight.icao")
            or raw_flight.get("flight_icao")
        ),
        "flight_number": normalize_text(
            get_nested_value(raw_flight, "flight.number")
            or raw_flight.get("flight_number")
        ),
        "flight_date": parse_date(raw_flight.get("flight_date")),
        "flight_status": normalize_text(raw_flight.get("flight_status")),
        "airline_name": normalize_text(
            get_nested_value(raw_flight, "airline.airline_name")
            or get_nested_value(raw_flight, "airline.name")
        ),
        "airline_iata": get_airline_iata(raw_flight),
        "departure_iata": get_departure_iata(raw_flight),
        "arrival_iata": get_arrival_iata(raw_flight),
        "departure_scheduled": parse_datetime(
            get_nested_value(raw_flight, "departure.scheduled")
            or raw_flight.get("departure_scheduled")
        ),
        "departure_estimated": parse_datetime(
            get_nested_value(raw_flight, "departure.estimated")
            or raw_flight.get("departure_estimated")
        ),
        "departure_actual": parse_datetime(
            get_nested_value(raw_flight, "departure.actual")
            or raw_flight.get("departure_actual")
        ),
        "departure_delay_min": safe_int(
            get_nested_value(raw_flight, "departure.delay")
            or raw_flight.get("departure_delay_min")
        ),
        "arrival_scheduled": parse_datetime(
            get_nested_value(raw_flight, "arrival.scheduled")
            or raw_flight.get("arrival_scheduled")
        ),
        "arrival_estimated": parse_datetime(
            get_nested_value(raw_flight, "arrival.estimated")
            or raw_flight.get("arrival_estimated")
        ),
        "arrival_actual": parse_datetime(
            get_nested_value(raw_flight, "arrival.actual")
            or raw_flight.get("arrival_actual")
        ),
        "arrival_delay_min": safe_int(
            get_nested_value(raw_flight, "arrival.delay")
            or raw_flight.get("arrival_delay_min")
        ),
        "live_latitude": live_info["live_latitude"],
        "live_longitude": live_info["live_longitude"],
        "live_altitude": live_info["live_altitude"],
        "live_direction": live_info["live_direction"],
        "live_speed_horizontal": live_info["live_speed_horizontal"],
        "live_speed_vertical": live_info["live_speed_vertical"],
        "live_is_ground": live_info["live_is_ground"],
        "live_updated": live_info["live_updated"],
        "model_latitude": clean_value(model_features.get("Latitude")),
        "model_longitude": clean_value(model_features.get("Longitude")),
        "model_requested_fl": clean_value(model_features.get("Requested FL")),
        "model_actual_distance_flown_nm": clean_value(
            model_features.get("Actual Distance Flown (nm)")
        ),
        "model_ac_operator": clean_value(model_features.get("AC Operator")),
        "model_month": clean_value(model_features.get("month")),
        "model_hour": clean_value(model_features.get("hour")),
        "model_day_of_week": clean_value(model_features.get("day_of_week")),
        "predicted_is_delayed": predicted_is_delayed,
        "delay_probability": delay_probability,
        "prediction_label": prediction_label,
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "prediction_created_at": datetime.now(UTC),
    }


def ensure_prediction_table_exists() -> None:
    """Create the prediction serving table if it does not exist."""
    create_table_query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table} (
            id SERIAL PRIMARY KEY,

            flight_iata TEXT,
            flight_icao TEXT,
            flight_number TEXT,

            flight_date DATE,
            flight_status TEXT,

            airline_name TEXT,
            airline_iata TEXT,
            departure_iata TEXT,
            arrival_iata TEXT,

            departure_scheduled TIMESTAMPTZ,
            departure_estimated TIMESTAMPTZ,
            departure_actual TIMESTAMPTZ,
            departure_delay_min INTEGER,

            arrival_scheduled TIMESTAMPTZ,
            arrival_estimated TIMESTAMPTZ,
            arrival_actual TIMESTAMPTZ,
            arrival_delay_min INTEGER,

            live_latitude DOUBLE PRECISION,
            live_longitude DOUBLE PRECISION,
            live_altitude DOUBLE PRECISION,
            live_direction DOUBLE PRECISION,
            live_speed_horizontal DOUBLE PRECISION,
            live_speed_vertical DOUBLE PRECISION,
            live_is_ground BOOLEAN,
            live_updated TIMESTAMPTZ,

            model_latitude DOUBLE PRECISION,
            model_longitude DOUBLE PRECISION,
            model_requested_fl INTEGER,
            model_actual_distance_flown_nm DOUBLE PRECISION,
            model_ac_operator TEXT,
            model_month INTEGER,
            model_hour INTEGER,
            model_day_of_week INTEGER,

            predicted_is_delayed BOOLEAN,
            delay_probability NUMERIC(6, 5),
            prediction_label TEXT,

            model_name TEXT,
            model_version TEXT,
            prediction_created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

            UNIQUE (
                flight_iata,
                flight_icao,
                flight_number,
                flight_date,
                departure_scheduled
            )
        )
        """
    ).format(table=sql.Identifier(FLIGHT_PREDICTIONS_TABLE))

    with postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
        conn.commit()


def save_predictions_to_postgres(records: list[dict[str, Any]]) -> int:
    """Upsert prediction records into PostgreSQL."""
    if not records:
        return 0

    columns = [
        "flight_iata",
        "flight_icao",
        "flight_number",
        "flight_date",
        "flight_status",
        "airline_name",
        "airline_iata",
        "departure_iata",
        "arrival_iata",
        "departure_scheduled",
        "departure_estimated",
        "departure_actual",
        "departure_delay_min",
        "arrival_scheduled",
        "arrival_estimated",
        "arrival_actual",
        "arrival_delay_min",
        "live_latitude",
        "live_longitude",
        "live_altitude",
        "live_direction",
        "live_speed_horizontal",
        "live_speed_vertical",
        "live_is_ground",
        "live_updated",
        "model_latitude",
        "model_longitude",
        "model_requested_fl",
        "model_actual_distance_flown_nm",
        "model_ac_operator",
        "model_month",
        "model_hour",
        "model_day_of_week",
        "predicted_is_delayed",
        "delay_probability",
        "prediction_label",
        "model_name",
        "model_version",
        "prediction_created_at",
    ]

    values = [tuple(record.get(column) for column in columns) for record in records]

    insert_query = sql.SQL(
        """
        INSERT INTO {table} ({columns})
        VALUES %s
        ON CONFLICT (
            flight_iata,
            flight_icao,
            flight_number,
            flight_date,
            departure_scheduled
        )
        DO UPDATE SET
            flight_status = EXCLUDED.flight_status,
            airline_name = EXCLUDED.airline_name,
            airline_iata = EXCLUDED.airline_iata,
            departure_iata = EXCLUDED.departure_iata,
            arrival_iata = EXCLUDED.arrival_iata,
            departure_estimated = EXCLUDED.departure_estimated,
            departure_actual = EXCLUDED.departure_actual,
            departure_delay_min = EXCLUDED.departure_delay_min,
            arrival_estimated = EXCLUDED.arrival_estimated,
            arrival_actual = EXCLUDED.arrival_actual,
            arrival_delay_min = EXCLUDED.arrival_delay_min,
            live_latitude = EXCLUDED.live_latitude,
            live_longitude = EXCLUDED.live_longitude,
            live_altitude = EXCLUDED.live_altitude,
            live_direction = EXCLUDED.live_direction,
            live_speed_horizontal = EXCLUDED.live_speed_horizontal,
            live_speed_vertical = EXCLUDED.live_speed_vertical,
            live_is_ground = EXCLUDED.live_is_ground,
            live_updated = EXCLUDED.live_updated,
            model_latitude = EXCLUDED.model_latitude,
            model_longitude = EXCLUDED.model_longitude,
            model_requested_fl = EXCLUDED.model_requested_fl,
            model_actual_distance_flown_nm = EXCLUDED.model_actual_distance_flown_nm,
            model_ac_operator = EXCLUDED.model_ac_operator,
            model_month = EXCLUDED.model_month,
            model_hour = EXCLUDED.model_hour,
            model_day_of_week = EXCLUDED.model_day_of_week,
            predicted_is_delayed = EXCLUDED.predicted_is_delayed,
            delay_probability = EXCLUDED.delay_probability,
            prediction_label = EXCLUDED.prediction_label,
            model_name = EXCLUDED.model_name,
            model_version = EXCLUDED.model_version,
            prediction_created_at = EXCLUDED.prediction_created_at
        """
    ).format(
        table=sql.Identifier(FLIGHT_PREDICTIONS_TABLE),
        columns=sql.SQL(", ").join(sql.Identifier(column) for column in columns),
    )

    with postgres_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_query.as_string(conn), values)
        conn.commit()

    return len(records)


def main() -> None:
    """Run the complete prediction batch."""
    print("[run_predictions] Starting EUROCONTROL-compatible prediction batch...")

    raw_flights = read_recent_raw_flights(limit=PREDICTION_LIMIT)
    print(f"[run_predictions] Raw flight records read from MongoDB: {len(raw_flights)}")

    if not raw_flights:
        print("[run_predictions] No raw flights found. Nothing to predict.")
        return

    airport_coordinates = load_airport_coordinates_from_static_csv(
        AIRPORTS_STATIC_CSV_PATH
    )

    print(
        "[run_predictions] Airport coordinates loaded from static CSV: "
        f"{len(airport_coordinates)}"
    )

    if not airport_coordinates:
        print(
            "[run_predictions] No airport coordinates found in static CSV. "
            "Cannot build EUROCONTROL-compatible features."
        )
        return

    features_df, valid_indexes = build_feature_index_mapping_from_coordinates(
        raw_flights=raw_flights,
        airport_coordinates=airport_coordinates,
    )

    print(f"[run_predictions] EUROCONTROL feature matrix shape: {features_df.shape}")
    print(
        "[run_predictions] Dropped records due to missing EUROCONTROL-compatible "
        f"features: {len(raw_flights) - len(valid_indexes)}"
    )

    if features_df.empty:
        print("[run_predictions] No valid rows after EUROCONTROL normalization.")
        return

    model = load_delay_model(DELAY_MODEL_PATH)
    print(f"[run_predictions] Model loaded from: {DELAY_MODEL_PATH}")

    predicted_labels, probabilities = predict_delay(model, features_df)
    print(f"[run_predictions] Predictions generated: {len(predicted_labels)}")

    serving_records = []

    for feature_row_position, raw_flight_index in enumerate(valid_indexes):
        raw_flight = raw_flights[raw_flight_index]
        model_features = features_df.iloc[feature_row_position].to_dict()

        serving_records.append(
            build_serving_record(
                raw_flight=raw_flight,
                model_features=model_features,
                predicted_is_delayed=predicted_labels[feature_row_position],
                delay_probability=probabilities[feature_row_position],
            )
        )

    ensure_prediction_table_exists()
    inserted_count = save_predictions_to_postgres(serving_records)

    print(
        "[run_predictions] Prediction batch finished successfully. "
        f"Rows upserted into {FLIGHT_PREDICTIONS_TABLE}: {inserted_count}"
    )


if __name__ == "__main__":
    main()