"""
Train the DST Airlines delay prediction model using historical EUROCONTROL-like data.

Responsibility:
1. Read historical EUROCONTROL CSV files.
2. Build the target variable: is_delayed.
3. Build the same feature set used by the production inference pipeline.
4. Train a scikit-learn Pipeline: preprocessing + classifier.
5. Save the trained model artifact to models/delay_model.joblib.

Recommended execution:
    python -m dst_airlines.ml.train_delay_model

Environment variables:
    EUROCONTROL_DATA_PATH=/app/data/eurocontrol/*
    DELAY_MODEL_PATH=/app/models/delay_model.joblib
    MODEL_TYPE=random_forest
    MODEL_RANDOM_STATE=84

Expected historical files:
    Flights_*.csv
    Flight_Points_Filed_*.csv

Model feature contract:
    - Latitude
    - Longitude
    - Requested FL
    - Actual Distance Flown (nm)
    - AC Operator
    - month
    - hour
    - day_of_week
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


EUROCONTROL_DATA_PATH = os.getenv("EUROCONTROL_DATA_PATH", "data/eurocontrol/*")
DELAY_MODEL_PATH = os.getenv("DELAY_MODEL_PATH", "models/delay_model.joblib")
MODEL_TYPE = os.getenv("MODEL_TYPE", "random_forest")
MODEL_RANDOM_STATE = int(os.getenv("MODEL_RANDOM_STATE", "84"))

FEATURE_COLUMNS = [
    "Latitude",
    "Longitude",
    "Requested FL",
    "Actual Distance Flown (nm)",
    "AC Operator",
    "month",
    "hour",
    "day_of_week",
]

NUMERIC_COLUMNS = [
    "Latitude",
    "Longitude",
    "Requested FL",
    "Actual Distance Flown (nm)",
    "month",
    "hour",
    "day_of_week",
]

CATEGORICAL_COLUMNS = [
    "AC Operator",
]

TARGET_COLUMN = "is_delayed"


def find_csv_files(base_path: str, pattern: str) -> list[str]:
    """Find CSV files recursively using a base wildcard path and filename pattern."""
    search_pattern = os.path.join(base_path, pattern)
    return sorted(glob.glob(search_pattern, recursive=True))


def read_historical_csvs(files: list[str]) -> pd.DataFrame:
    """Read and concatenate historical CSV files."""
    if not files:
        raise FileNotFoundError("No historical CSV files found.")

    frames = []

    for file in files:
        print(f"[train_delay_model] Reading: {file}")
        frames.append(pd.read_csv(file))

    return pd.concat(frames, ignore_index=True)


def load_historical_data(base_path: str = EUROCONTROL_DATA_PATH) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load EUROCONTROL historical flight and filed trajectory point datasets."""
    flights_files = find_csv_files(base_path, "Flights_*.csv")
    points_files = find_csv_files(base_path, "Flight_Points_Filed_*.csv")

    if not flights_files:
        raise FileNotFoundError(
            f"No Flights_*.csv files found under EUROCONTROL_DATA_PATH={base_path}"
        )

    if not points_files:
        raise FileNotFoundError(
            f"No Flight_Points_Filed_*.csv files found under EUROCONTROL_DATA_PATH={base_path}"
        )

    flights_df = read_historical_csvs(flights_files)
    points_filed_df = read_historical_csvs(points_files)

    print(f"[train_delay_model] Flights rows: {len(flights_df)}")
    print(f"[train_delay_model] Filed points rows: {len(points_filed_df)}")

    return flights_df, points_filed_df


def prepare_training_dataset(
    flights_df: pd.DataFrame,
    points_filed_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build the training dataset using the original EUROCONTROL feature logic."""
    required_flight_columns = [
        "ECTRL ID",
        "FILED OFF BLOCK TIME",
        "ACTUAL OFF BLOCK TIME",
        "Requested FL",
        "Actual Distance Flown (nm)",
        "AC Operator",
    ]

    required_point_columns = [
        "ECTRL ID",
        "Sequence Number",
        "Latitude",
        "Longitude",
    ]

    missing_flight_columns = set(required_flight_columns) - set(flights_df.columns)
    missing_point_columns = set(required_point_columns) - set(points_filed_df.columns)

    if missing_flight_columns:
        raise ValueError(
            f"Missing required flight columns: {sorted(missing_flight_columns)}"
        )

    if missing_point_columns:
        raise ValueError(
            f"Missing required point columns: {sorted(missing_point_columns)}"
        )

    processed_flights = flights_df.copy()

    processed_flights["filed_dep"] = pd.to_datetime(
        processed_flights["FILED OFF BLOCK TIME"],
        format="%d-%m-%Y %H:%M:%S",
        errors="coerce",
    )

    processed_flights["actual_dep"] = pd.to_datetime(
        processed_flights["ACTUAL OFF BLOCK TIME"],
        format="%d-%m-%Y %H:%M:%S",
        errors="coerce",
    )

    processed_flights["departure_delay_min"] = (
        processed_flights["actual_dep"] - processed_flights["filed_dep"]
    ).dt.total_seconds() / 60

    processed_flights[TARGET_COLUMN] = (
        processed_flights["departure_delay_min"] > 15
    ).astype(int)

    processed_flights["month"] = processed_flights["filed_dep"].dt.month
    processed_flights["hour"] = processed_flights["filed_dep"].dt.hour

    # Spark F.dayofweek convention used in the notebook:
    # 1 = Sunday, 2 = Monday, ..., 7 = Saturday.
    processed_flights["day_of_week"] = (
        (processed_flights["filed_dep"].dt.dayofweek + 2 - 1) % 7 + 1
    )

    origin_nodes = (
        points_filed_df[points_filed_df["Sequence Number"] == 0]
        .loc[:, ["ECTRL ID", "Latitude", "Longitude"]]
        .rename(columns={"ECTRL ID": "points_id"})
    )

    ml_ready_df = processed_flights.merge(
        origin_nodes,
        left_on="ECTRL ID",
        right_on="points_id",
        how="inner",
    )

    final_dataset = ml_ready_df[
        [TARGET_COLUMN, *FEATURE_COLUMNS]
    ].dropna()

    print(f"[train_delay_model] Final training dataset shape: {final_dataset.shape}")
    print(
        "[train_delay_model] Target distribution:\n"
        f"{final_dataset[TARGET_COLUMN].value_counts(normalize=True).sort_index()}"
    )

    return final_dataset


def build_model_pipeline() -> Pipeline:
    """Build the preprocessing + classifier pipeline."""
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric_scaling", StandardScaler(), NUMERIC_COLUMNS),
            (
                "categorical_encoding",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                CATEGORICAL_COLUMNS,
            ),
        ]
    )

    if MODEL_TYPE == "random_forest":
        classifier = RandomForestClassifier(
            n_estimators=100,
            max_depth=15,
            random_state=MODEL_RANDOM_STATE,
            n_jobs=-1,
        )
    else:
        raise ValueError(
            f"Unsupported MODEL_TYPE={MODEL_TYPE}. Supported values: random_forest"
        )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", classifier),
        ]
    )


def train_and_evaluate(dataset: pd.DataFrame) -> Pipeline:
    """Train the model and print evaluation metrics."""
    X = dataset[FEATURE_COLUMNS]
    y = dataset[TARGET_COLUMN]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=MODEL_RANDOM_STATE,
        stratify=y,
    )

    model_pipeline = build_model_pipeline()

    print("[train_delay_model] Training model...")
    model_pipeline.fit(X_train, y_train)

    y_pred = model_pipeline.predict(X_test)

    print("[train_delay_model] Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    print("[train_delay_model] Classification Report:")
    print(classification_report(y_test, y_pred, target_names=["On-Time (0)", "Delayed (1)"]))

    return model_pipeline


def save_model(model_pipeline: Pipeline, model_path: str = DELAY_MODEL_PATH) -> None:
    """Persist the trained model pipeline."""
    output_path = Path(model_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(model_pipeline, output_path)

    print(f"[train_delay_model] Model saved to: {output_path}")


def main() -> None:
    """Run the full model training process."""
    print("[train_delay_model] Starting EUROCONTROL model training...")

    flights_df, points_filed_df = load_historical_data(EUROCONTROL_DATA_PATH)
    dataset = prepare_training_dataset(flights_df, points_filed_df)
    model_pipeline = train_and_evaluate(dataset)
    save_model(model_pipeline, DELAY_MODEL_PATH)

    print("[train_delay_model] Training finished successfully.")


if __name__ == "__main__":
    main()
