"""
Train the DST Airlines delay prediction model using historical EUROCONTROL-like data.

Responsibility
--------------
1. Read historical EUROCONTROL CSV files.
2. Build the target variable: is_delayed.
3. Build the same feature set used by the production AviationStack inference
   normalizer.
4. Train a scikit-learn Pipeline: preprocessing + classifier.
5. Save the trained model artifact to models/delay_model.joblib.

Recommended execution
---------------------
python -m dst_airlines.ml.train_delay_model

Environment variables
---------------------
EUROCONTROL_DATA_PATH=data/eurocontrol
DELAY_MODEL_PATH=models/delay_model.joblib
MODEL_TYPE=random_forest
MODEL_RANDOM_STATE=84
USE_ESTIMATED_ROUTE_DISTANCE=true

Expected historical files
-------------------------
Required:
- Flights_*.csv

Optional:
- Flight_Points_Filed_*.csv

Why Flight_Points_Filed is optional now
--------------------------------------
The original version used Sequence Number == 0 from Flight_Points_Filed as
Latitude / Longitude. The corrected version uses ADEP Latitude / ADEP Longitude
from Flights_*.csv because that is the same type of feature we can reproduce at
production inference time from AviationStack + airport coordinates.

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

Important
---------
For compatibility with the existing model contract, the distance feature keeps
its historical name "Actual Distance Flown (nm)". By default, however, it is
calculated as estimated origin-destination great-circle distance to align
training with production inference.
"""

from __future__ import annotations

import glob
import json
import math
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


EUROCONTROL_DATA_PATH = os.getenv("EUROCONTROL_DATA_PATH", "data/eurocontrol")
DELAY_MODEL_PATH = os.getenv("DELAY_MODEL_PATH", "models/delay_model.joblib")
MODEL_TYPE = os.getenv("MODEL_TYPE", "random_forest")
MODEL_RANDOM_STATE = int(os.getenv("MODEL_RANDOM_STATE", "84"))
USE_ESTIMATED_ROUTE_DISTANCE = os.getenv(
    "USE_ESTIMATED_ROUTE_DISTANCE",
    "true",
).strip().lower() in {"1", "true", "yes", "y"}


MAX_TRAINING_ROWS = int(os.getenv("MAX_TRAINING_ROWS", "300000"))
RF_N_ESTIMATORS = int(os.getenv("RF_N_ESTIMATORS", "50"))
RF_MAX_DEPTH = int(os.getenv("RF_MAX_DEPTH", "12"))
RF_N_JOBS = int(os.getenv("RF_N_JOBS", "1"))


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
DEPARTURE_DELAY_THRESHOLD_MINUTES = 15


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------


def find_csv_files(base_path: str, pattern: str) -> list[str]:
    """Find CSV files using a base path that may or may not contain wildcards."""
    search_patterns: list[str] = []

    if glob.has_magic(base_path):
        search_patterns.append(os.path.join(base_path, pattern))
        search_patterns.append(os.path.join(base_path, "**", pattern))
    else:
        search_patterns.append(os.path.join(base_path, pattern))
        search_patterns.append(os.path.join(base_path, "**", pattern))

    files: set[str] = set()

    for search_pattern in search_patterns:
        files.update(glob.glob(search_pattern, recursive=True))

    return sorted(files)


def read_historical_csvs(files: list[str]) -> pd.DataFrame:
    """Read and concatenate historical CSV files."""
    if not files:
        raise FileNotFoundError("No historical CSV files found.")

    frames: list[pd.DataFrame] = []

    for file in files:
        print(f"[train_delay_model] Reading: {file}")
        try:
            frame = pd.read_csv(file, encoding="utf-8-sig")
        except UnicodeDecodeError:
            frame = pd.read_csv(file, encoding="latin1")

        frame.columns = [str(column).strip() for column in frame.columns]
        frames.append(frame)

    return pd.concat(frames, ignore_index=True)


def load_historical_data(
    base_path: str = EUROCONTROL_DATA_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """Load EUROCONTROL historical flight dataset and optional filed points dataset."""
    flights_files = find_csv_files(base_path, "Flights_*.csv")
    points_files = find_csv_files(base_path, "Flight_Points_Filed_*.csv")

    if not flights_files:
        raise FileNotFoundError(
            f"No Flights_*.csv files found under EUROCONTROL_DATA_PATH={base_path}"
        )

    flights_df = read_historical_csvs(flights_files)
    points_filed_df = read_historical_csvs(points_files) if points_files else None

    print(f"[train_delay_model] Flights rows: {len(flights_df)}")

    if points_filed_df is not None:
        print(
            "[train_delay_model] Filed points rows: "
            f"{len(points_filed_df)}. File loaded for compatibility, "
            "but not used by the corrected feature logic."
        )
    else:
        print(
            "[train_delay_model] No Flight_Points_Filed_*.csv files found. "
            "Continuing because the corrected feature logic uses Flights_*.csv."
        )

    return flights_df, points_filed_df


# ---------------------------------------------------------------------------
# Feature engineering helpers
# ---------------------------------------------------------------------------


def normalize_text(value: object, *, uppercase: bool = False) -> str | None:
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


def to_numeric(series: pd.Series) -> pd.Series:
    """Convert a pandas Series to numeric values."""
    return pd.to_numeric(series, errors="coerce")


def parse_eurocontrol_datetime(series: pd.Series) -> pd.Series:
    """Parse EUROCONTROL datetime strings like 01-06-2023 00:00:00."""
    parsed = pd.to_datetime(
        series,
        format="%d-%m-%Y %H:%M:%S",
        errors="coerce",
    )

    # Fallback for unexpected but still parseable formats.
    if parsed.isna().any():
        fallback = pd.to_datetime(series, dayfirst=True, errors="coerce")
        parsed = parsed.fillna(fallback)

    return parsed


def spark_day_of_week(datetime_series: pd.Series) -> pd.Series:
    """Return Spark dayofweek convention: 1=Sunday, 2=Monday, ..., 7=Saturday."""
    return (datetime_series.dt.dayofweek + 1) % 7 + 1


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


def calculate_route_distance_nm(df: pd.DataFrame) -> pd.Series:
    """Calculate origin-destination great-circle distance using ADEP/ADES coordinates."""
    required_columns = [
        "ADEP Latitude",
        "ADEP Longitude",
        "ADES Latitude",
        "ADES Longitude",
    ]

    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(
            "Cannot calculate estimated route distance. Missing columns: "
            f"{sorted(missing_columns)}"
        )

    adep_lat = to_numeric(df["ADEP Latitude"])
    adep_lon = to_numeric(df["ADEP Longitude"])
    ades_lat = to_numeric(df["ADES Latitude"])
    ades_lon = to_numeric(df["ADES Longitude"])

    distances: list[float | None] = []

    for lat1, lon1, lat2, lon2 in zip(adep_lat, adep_lon, ades_lat, ades_lon):
        if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
            distances.append(None)
            continue

        distances.append(
            haversine_distance_nm(
                float(lat1),
                float(lon1),
                float(lat2),
                float(lon2),
            )
        )

    return pd.Series(distances, index=df.index, dtype="float64")


def build_distance_feature(processed_flights: pd.DataFrame) -> pd.Series:
    """Build the distance feature aligned with production inference."""
    if USE_ESTIMATED_ROUTE_DISTANCE:
        print(
            "[train_delay_model] Using estimated origin-destination distance "
            "for feature 'Actual Distance Flown (nm)'."
        )
        return calculate_route_distance_nm(processed_flights)

    if "Actual Distance Flown (nm)" not in processed_flights.columns:
        raise ValueError(
            "Missing 'Actual Distance Flown (nm)' and "
            "USE_ESTIMATED_ROUTE_DISTANCE=false."
        )

    print(
        "[train_delay_model] Using historical 'Actual Distance Flown (nm)' "
        "directly. Warning: this may differ from production inference."
    )
    return to_numeric(processed_flights["Actual Distance Flown (nm)"])


# ---------------------------------------------------------------------------
# Training dataset preparation
# ---------------------------------------------------------------------------


def validate_required_columns(flights_df: pd.DataFrame) -> None:
    """Validate input columns required by the corrected training logic."""
    required_flight_columns = [
        "ECTRL ID",
        "ADEP Latitude",
        "ADEP Longitude",
        "ADES Latitude",
        "ADES Longitude",
        "FILED OFF BLOCK TIME",
        "ACTUAL OFF BLOCK TIME",
        "Requested FL",
        "AC Operator",
    ]

    if not USE_ESTIMATED_ROUTE_DISTANCE:
        required_flight_columns.append("Actual Distance Flown (nm)")

    missing_flight_columns = set(required_flight_columns) - set(flights_df.columns)

    if missing_flight_columns:
        raise ValueError(
            f"Missing required flight columns: {sorted(missing_flight_columns)}"
        )


def prepare_training_dataset(flights_df: pd.DataFrame) -> pd.DataFrame:
    """Build the training dataset using features reproducible in production."""
    validate_required_columns(flights_df)

    processed_flights = flights_df.copy()

    processed_flights["filed_dep"] = parse_eurocontrol_datetime(
        processed_flights["FILED OFF BLOCK TIME"]
    )
    processed_flights["actual_dep"] = parse_eurocontrol_datetime(
        processed_flights["ACTUAL OFF BLOCK TIME"]
    )

    processed_flights["departure_delay_min"] = (
        processed_flights["actual_dep"] - processed_flights["filed_dep"]
    ).dt.total_seconds() / 60

    processed_flights[TARGET_COLUMN] = (
        processed_flights["departure_delay_min"] > DEPARTURE_DELAY_THRESHOLD_MINUTES
    ).astype("int64")

    # Same calendar features expected by the inference pipeline.
    processed_flights["month"] = processed_flights["filed_dep"].dt.month
    processed_flights["hour"] = processed_flights["filed_dep"].dt.hour
    processed_flights["day_of_week"] = spark_day_of_week(processed_flights["filed_dep"])

    # Same origin-coordinate logic used by AviationStack inference.
    processed_flights["Latitude"] = to_numeric(processed_flights["ADEP Latitude"])
    processed_flights["Longitude"] = to_numeric(processed_flights["ADEP Longitude"])

    processed_flights["Requested FL"] = to_numeric(processed_flights["Requested FL"])
    processed_flights["Actual Distance Flown (nm)"] = build_distance_feature(
        processed_flights
    )
    processed_flights["AC Operator"] = processed_flights["AC Operator"].apply(
        lambda value: normalize_text(value, uppercase=True)
    )

    candidate_dataset = processed_flights[[TARGET_COLUMN, *FEATURE_COLUMNS]].copy()

    print(f"[train_delay_model] Rows before dropna: {len(candidate_dataset)}")
    print(
        "[train_delay_model] Missing values before dropna:\n"
        f"{candidate_dataset.isna().sum()}"
    )

    final_dataset = candidate_dataset.dropna()

    print(f"[train_delay_model] Final training dataset shape: {final_dataset.shape}")
    print(
        "[train_delay_model] Target distribution absolute:\n"
        f"{final_dataset[TARGET_COLUMN].value_counts().sort_index()}"
    )
    print(
        "[train_delay_model] Target distribution normalized:\n"
        f"{final_dataset[TARGET_COLUMN].value_counts(normalize=True).sort_index()}"
    )

    if final_dataset.empty:
        raise ValueError("Final training dataset is empty after dropna().")

    if final_dataset[TARGET_COLUMN].nunique() < 2:
        raise ValueError(
            "Target has only one class after preprocessing. "
            "Model training requires both delayed and non-delayed examples."
        )
    
    if MAX_TRAINING_ROWS > 0 and len(final_dataset) > MAX_TRAINING_ROWS:
        print(
            f"[train_delay_model] Sampling training dataset from "
            f"{len(final_dataset)} to {MAX_TRAINING_ROWS} rows."
        )

        final_dataset = (
            final_dataset
            .groupby(TARGET_COLUMN, group_keys=False)
            .apply(
                lambda group: group.sample(
                    n=min(
                        len(group),
                        max(1, int(MAX_TRAINING_ROWS * len(group) / len(final_dataset)))
                    ),
                    random_state=MODEL_RANDOM_STATE,
                )
            )
            .sample(frac=1, random_state=MODEL_RANDOM_STATE)
            .reset_index(drop=True)
        )

        print(f"[train_delay_model] Sampled dataset shape: {final_dataset.shape}")
        print(
            "[train_delay_model] Sampled target distribution normalized:\n"
            f"{final_dataset[TARGET_COLUMN].value_counts(normalize=True).sort_index()}"
        )

    return final_dataset


# ---------------------------------------------------------------------------
# Model pipeline
# ---------------------------------------------------------------------------


def build_one_hot_encoder() -> OneHotEncoder:
    """Build a OneHotEncoder compatible with different scikit-learn versions."""
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=True)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def build_model_pipeline() -> Pipeline:
    """Build the preprocessing + classifier pipeline."""
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric_scaling", StandardScaler(), NUMERIC_COLUMNS),
            (
                "categorical_encoding",
                build_one_hot_encoder(),
                CATEGORICAL_COLUMNS,
            ),
        ]
    )

    if MODEL_TYPE == "random_forest":
        classifier = RandomForestClassifier(
            n_estimators=RF_N_ESTIMATORS,
            max_depth=RF_MAX_DEPTH,
            min_samples_leaf=20,
            class_weight="balanced",
            random_state=MODEL_RANDOM_STATE,
            n_jobs=RF_N_JOBS,
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


def get_stratify_target(y: pd.Series) -> pd.Series | None:
    """Return y for stratification only when each class has enough samples."""
    class_counts = y.value_counts()

    if len(class_counts) < 2:
        return None

    if class_counts.min() < 2:
        return None

    return y


def train_and_evaluate(dataset: pd.DataFrame) -> Pipeline:
    """Train the model and print evaluation metrics."""
    X = dataset[FEATURE_COLUMNS]
    y = dataset[TARGET_COLUMN]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=MODEL_RANDOM_STATE,
        stratify=get_stratify_target(y),
    )

    model_pipeline = build_model_pipeline()

    print("[train_delay_model] Training model...")
    model_pipeline.fit(X_train, y_train)

    y_pred = model_pipeline.predict(X_test)

    print("[train_delay_model] Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    print("[train_delay_model] Classification Report:")
    print(
        classification_report(
            y_test,
            y_pred,
            target_names=["On-Time (0)", "Delayed (1)"],
            zero_division=0,
        )
    )

    return model_pipeline


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def save_model(model_pipeline: Pipeline, model_path: str = DELAY_MODEL_PATH) -> None:
    """Persist the trained model pipeline."""
    output_path = Path(model_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(model_pipeline, output_path)

    print(f"[train_delay_model] Model saved to: {output_path}")


def save_model_metadata(model_path: str = DELAY_MODEL_PATH) -> None:
    """Persist lightweight metadata next to the model artifact."""
    output_path = Path(model_path)
    metadata_path = output_path.with_suffix(".metadata.json")

    metadata = {
        "feature_columns": FEATURE_COLUMNS,
        "numeric_columns": NUMERIC_COLUMNS,
        "categorical_columns": CATEGORICAL_COLUMNS,
        "target_column": TARGET_COLUMN,
        "departure_delay_threshold_minutes": DEPARTURE_DELAY_THRESHOLD_MINUTES,
        "model_type": MODEL_TYPE,
        "model_random_state": MODEL_RANDOM_STATE,
        "use_estimated_route_distance": USE_ESTIMATED_ROUTE_DISTANCE,
        "latitude_longitude_source": "Flights_*.csv ADEP Latitude / ADEP Longitude",
        "distance_source": (
            "Estimated great-circle ADEP-ADES distance"
            if USE_ESTIMATED_ROUTE_DISTANCE
            else "Historical Actual Distance Flown (nm)"
        ),
        "operator_source": "Flights_*.csv AC Operator normalized uppercase",
        "day_of_week_convention": "Spark: 1=Sunday, 2=Monday, ..., 7=Saturday",
    }

    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"[train_delay_model] Metadata saved to: {metadata_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the full model training process."""
    print("[train_delay_model] Starting EUROCONTROL model training...")
    print(f"[train_delay_model] EUROCONTROL_DATA_PATH={EUROCONTROL_DATA_PATH}")
    print(f"[train_delay_model] DELAY_MODEL_PATH={DELAY_MODEL_PATH}")
    print(
        "[train_delay_model] USE_ESTIMATED_ROUTE_DISTANCE="
        f"{USE_ESTIMATED_ROUTE_DISTANCE}"
    )

    flights_df, _points_filed_df = load_historical_data(EUROCONTROL_DATA_PATH)
    dataset = prepare_training_dataset(flights_df)
    model_pipeline = train_and_evaluate(dataset)
    save_model(model_pipeline, DELAY_MODEL_PATH)
    save_model_metadata(DELAY_MODEL_PATH)

    print("[train_delay_model] Training finished successfully.")


if __name__ == "__main__":
    main()
