"""
Feature Engineering module for EUROCONTROL historical flight data.

Responsibility
--------------
1. Validate required columns in raw datasets.
2. Filter for specific domains (e.g., French domestic flights).
3. Compute target variables (e.g., departure_delay_min, is_delayed).
4. Engineer temporal features (month, hour, Spark-convention day of week).
5. Compute spatial features (Haversine great-circle distance, coordinates).
6. Normalize categorical variables (e.g., AC Operator).

This module can be used both to prepare data for bulk database insertion
and to generate the final feature matrix for Machine Learning training.
"""

import math
import os
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------

FRENCH_ICAO_PREFIX = "LF"
TARGET_COLUMN = "is_delayed"
DEPARTURE_DELAY_THRESHOLD_MINUTES = 15

# Align distance calculation with production inference by default
USE_ESTIMATED_ROUTE_DISTANCE = os.getenv(
    "USE_ESTIMATED_ROUTE_DISTANCE",
    "true",
).strip().lower() in {"1", "true", "yes", "y"}

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


# ---------------------------------------------------------------------------
# Helper Functions
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
    """Convert a pandas Series to numeric values, coercing errors to NaN."""
    return pd.to_numeric(series, errors="coerce")


def parse_eurocontrol_datetime(series: pd.Series) -> pd.Series:
    """Parse EUROCONTROL datetime strings (e.g., 01-06-2023 00:00:00)."""
    parsed = pd.to_datetime(
        series,
        format="%d-%m-%Y %H:%M:%S",
        errors="coerce",
    )

    # Fallback for unexpected but parseable formats
    if parsed.isna().any():
        fallback = pd.to_datetime(series, dayfirst=True, errors="coerce")
        parsed = parsed.fillna(fallback)

    return parsed


def spark_day_of_week(datetime_series: pd.Series) -> pd.Series:
    """Return Spark dayofweek convention: 1=Sunday, 2=Monday, ..., 7=Saturday."""
    return (datetime_series.dt.dayofweek + 1) % 7 + 1


def haversine_distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in nautical miles between two points."""
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

    distances = []

    for lat1, lon1, lat2, lon2 in zip(adep_lat, adep_lon, ades_lat, ades_lon):
        if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
            distances.append(None)
            continue

        distances.append(
            haversine_distance_nm(float(lat1), float(lon1), float(lat2), float(lon2))
        )

    return pd.Series(distances, index=df.index, dtype="float64")


def build_distance_feature(df: pd.DataFrame) -> pd.Series:
    """Build the distance feature aligned with production inference."""
    if USE_ESTIMATED_ROUTE_DISTANCE:
        return calculate_route_distance_nm(df)

    if "Actual Distance Flown (nm)" not in df.columns:
        raise ValueError(
            "Missing 'Actual Distance Flown (nm)' and USE_ESTIMATED_ROUTE_DISTANCE is false."
        )

    return to_numeric(df["Actual Distance Flown (nm)"])


# ---------------------------------------------------------------------------
# Main Engineering Pipeline
# ---------------------------------------------------------------------------

def validate_required_columns(df: pd.DataFrame) -> None:
    """Validate input columns required for feature engineering."""
    required_flight_columns = [
        "ADEP",
        "ADES",
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

    missing_flight_columns = set(required_flight_columns) - set(df.columns)

    if missing_flight_columns:
        raise ValueError(
            f"Missing required flight columns: {sorted(missing_flight_columns)}"
        )


def filter_french_domestic_flights(df: pd.DataFrame) -> pd.DataFrame:
    """Filter dataset for French domestic flights (ADEP and ADES start with 'LF')."""
    adep = df["ADEP"].astype(str).str.strip().str.upper()
    ades = df["ADES"].astype(str).str.strip().str.upper()

    is_french_domestic = adep.str.startswith(FRENCH_ICAO_PREFIX) & ades.str.startswith(FRENCH_ICAO_PREFIX)
    return df[is_french_domestic].copy()


def run_feature_engineering(
    raw_df: pd.DataFrame, 
    keep_only_ml_features: bool = False
) -> pd.DataFrame:
    """
    Execute the full feature engineering pipeline on raw EUROCONTROL data.
    
    Parameters:
    -----------
    raw_df : pd.DataFrame
        The raw dataframe loaded from EUROCONTROL CSV files.
    keep_only_ml_features : bool
        If True, returns only the columns required for ML training and drops NAs.
        If False, returns all original columns plus the engineered ones (ideal for DB insertion).
    
    Returns:
    --------
    pd.DataFrame
        The processed and engineered dataframe.
    """
    validate_required_columns(raw_df)

    # 1. Domain Filtering
    df = filter_french_domestic_flights(raw_df)

    # 2. Target Engineering (Delay Calculation)
    df["filed_dep"] = parse_eurocontrol_datetime(df["FILED OFF BLOCK TIME"])
    df["actual_dep"] = parse_eurocontrol_datetime(df["ACTUAL OFF BLOCK TIME"])

    df["departure_delay_min"] = (df["actual_dep"] - df["filed_dep"]).dt.total_seconds() / 60
    df[TARGET_COLUMN] = (df["departure_delay_min"] > DEPARTURE_DELAY_THRESHOLD_MINUTES).astype("Int64")

    # 3. Temporal Features
    df["month"] = df["filed_dep"].dt.month.astype("Int64")
    df["hour"] = df["filed_dep"].dt.hour.astype("Int64")
    df["day_of_week"] = spark_day_of_week(df["filed_dep"]).astype("Int64")

    # 4. Spatial & Distance Features
    df["Latitude"] = to_numeric(df["ADEP Latitude"])
    df["Longitude"] = to_numeric(df["ADEP Longitude"])
    df["Actual Distance Flown (nm)"] = build_distance_feature(df)

    # 5. Categorical normalization
    df["Requested FL"] = to_numeric(df["Requested FL"])
    df["AC Operator"] = df["AC Operator"].apply(
        lambda value: normalize_text(value, uppercase=True)
    )

    # 6. Output shaping
    if keep_only_ml_features:
        final_df = df[[TARGET_COLUMN, *FEATURE_COLUMNS]].copy()
        # Drop rows with missing values strictly for ML training
        return final_df.dropna()

    return df