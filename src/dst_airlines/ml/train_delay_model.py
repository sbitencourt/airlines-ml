"""
Train the DST Airlines delay prediction model using pre-processed data from PostgreSQL.

Responsibility
--------------
1. Connect to PostgreSQL and read the engineered historical data table.
2. Validate the presence of required feature and target columns.
3. Perform stratified sampling if the dataset exceeds MAX_TRAINING_ROWS.
4. Train a scikit-learn Pipeline: preprocessing + classifier.
5. Save the trained model artifact and metadata to models/.

Recommended execution
---------------------
# Run locally (outside docker, connects to localhost:5432)
python -m dst_airlines.ml.train_delay_model

Environment variables
---------------------
POSTGRES_URI=postgresql://root:passwd@localhost:5432/airflow
TARGET_TABLE=eurocontrol_historical_data
DELAY_MODEL_PATH=models/delay_model.joblib
MODEL_TYPE=random_forest
MODEL_RANDOM_STATE=84
MAX_TRAINING_ROWS=300000
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import joblib
import pandas as pd
from sqlalchemy import create_engine
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Database configuration (Defaults to local Docker exposed port)
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://root:passwd@localhost:5432/airflow")
TARGET_TABLE = os.getenv("TARGET_TABLE", "eurocontrol_historical_data")

# ML configuration
DELAY_MODEL_PATH = os.getenv("DELAY_MODEL_PATH", "models/delay_model.joblib")
MODEL_TYPE = os.getenv("MODEL_TYPE", "random_forest")
MODEL_RANDOM_STATE = int(os.getenv("MODEL_RANDOM_STATE", "84"))
MAX_TRAINING_ROWS = int(os.getenv("MAX_TRAINING_ROWS", "300000"))

# Random Forest Hyperparameters
RF_N_ESTIMATORS = int(os.getenv("RF_N_ESTIMATORS", "50"))
RF_MAX_DEPTH = int(os.getenv("RF_MAX_DEPTH", "12"))
RF_N_JOBS = int(os.getenv("RF_N_JOBS", "1"))

# Model feature contract
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


# ---------------------------------------------------------------------------
# Database Loading
# ---------------------------------------------------------------------------

def load_data_from_db(uri: str, table_name: str) -> pd.DataFrame:
    """Load the pre-processed historical dataset directly from PostgreSQL."""
    print(f"[train_delay_model] Connecting to database...")
    engine = create_engine(uri)
    
    # We query only the columns needed for training to save memory
    columns_to_fetch = [TARGET_COLUMN] + FEATURE_COLUMNS
    columns_sql = ", ".join([f'"{col}"' for col in columns_to_fetch])
    
    query = f"SELECT {columns_sql} FROM {table_name}"
    
    print(f"[train_delay_model] Fetching data from table '{table_name}'...")
    df = pd.read_sql(query, engine)
    
    print(f"[train_delay_model] Loaded {len(df)} rows from PostgreSQL.")
    return df


# ---------------------------------------------------------------------------
# Dataset Preparation
# ---------------------------------------------------------------------------

def prepare_training_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Drop missing values and apply stratified sampling if necessary."""
    
    # Check for required columns
    missing = set([TARGET_COLUMN] + FEATURE_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in database table: {sorted(missing)}")

    print(f"[train_delay_model] Rows before dropna: {len(df)}")
    
    # Because columns in Postgres might have been stored as TEXT, ensure numeric types
    for col in NUMERIC_COLUMNS + [TARGET_COLUMN]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    final_dataset = df.dropna().copy()
    final_dataset[TARGET_COLUMN] = final_dataset[TARGET_COLUMN].astype("int64")

    print(f"[train_delay_model] Final training dataset shape: {final_dataset.shape}")
    print(
        "[train_delay_model] Target distribution normalized:\n"
        f"{final_dataset[TARGET_COLUMN].value_counts(normalize=True).sort_index()}"
    )

    if final_dataset.empty:
        raise ValueError("Final training dataset is empty after dropna().")

    if final_dataset[TARGET_COLUMN].nunique() < 2:
        raise ValueError("Target has only one class. Model training requires both delayed and non-delayed examples.")
    
    if MAX_TRAINING_ROWS > 0 and len(final_dataset) > MAX_TRAINING_ROWS:
        print(f"[train_delay_model] Sampling training dataset from {len(final_dataset)} to {MAX_TRAINING_ROWS} rows.")

        final_dataset = (
            final_dataset
            .groupby(TARGET_COLUMN, group_keys=True) 
            .apply(
                lambda group: group.sample(
                    n=min(
                        len(group),
                        max(1, int(MAX_TRAINING_ROWS * len(group) / len(final_dataset)))
                    ),
                    random_state=MODEL_RANDOM_STATE,
                ),
                include_groups=False 
            )
            .reset_index(level=0) 
            .sample(frac=1, random_state=MODEL_RANDOM_STATE)
            .reset_index(drop=True)
        )

        print(f"[train_delay_model] Sampled dataset shape: {final_dataset.shape}")

    return final_dataset


# ---------------------------------------------------------------------------
# Model Pipeline
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
        raise ValueError(f"Unsupported MODEL_TYPE={MODEL_TYPE}.")

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", classifier),
        ]
    )


def get_stratify_target(y: pd.Series) -> pd.Series | None:
    """Return y for stratification only when each class has enough samples."""
    class_counts = y.value_counts()
    if len(class_counts) < 2 or class_counts.min() < 2:
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
        "model_type": MODEL_TYPE,
        "model_random_state": MODEL_RANDOM_STATE,
        "data_source": f"PostgreSQL table: {TARGET_TABLE}",
        "feature_engineering": "Pre-processed via dst_airlines.etl.transform.feature_engineering"
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
    print("[train_delay_model] Starting ML model training from PostgreSQL data...")
    print(f"[train_delay_model] TARGET_TABLE={TARGET_TABLE}")
    print(f"[train_delay_model] DELAY_MODEL_PATH={DELAY_MODEL_PATH}")

    raw_df = load_data_from_db(POSTGRES_URI, TARGET_TABLE)
    dataset = prepare_training_dataset(raw_df)
    
    model_pipeline = train_and_evaluate(dataset)
    
    save_model(model_pipeline, DELAY_MODEL_PATH)
    save_model_metadata(DELAY_MODEL_PATH)

    print("[train_delay_model] Training finished successfully.")


if __name__ == "__main__":
    main()