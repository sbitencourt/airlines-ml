import os
import time
import io
from pathlib import Path
import pandas as pd
import requests

# Import the feature engineering module
from dst_airlines.etl.transform.feature_engineering import run_feature_engineering

# Use environment variable to allow running both inside and outside Docker
# Inside Docker, it will use 'http://fastapi:8000'
API_HOST = os.getenv("API_HOST", "http://localhost:8000")
API_BASE_URL = f"{API_HOST}/upload-eurocontrol"

TARGET_TABLE = "eurocontrol_historical_data"


def wait_for_api(max_retries: int = 15, delay_seconds: int = 2) -> bool:
    """
    Ping the FastAPI health/docs endpoint until it responds, 
    ensuring the API is fully ready to receive requests.
    """
    health_url = f"{API_HOST}/docs"
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Checking API status at {API_HOST}... (Attempt {attempt}/{max_retries})")
            response = requests.get(health_url, timeout=2)
            if response.status_code == 200:
                print("API is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        time.sleep(delay_seconds)
        
    return False


def main():
    base_dir = Path("data/eurocontrol")
    
    if not base_dir.exists():
        print(f"Directory {base_dir} does not exist.")
        return

    # We specifically filter for Flights_*.csv because Flight_Points_Filed_*.csv 
    # lacks the required columns (ADEP, ADES, etc.) to pass the feature engineering validation.
    csv_files = list(base_dir.rglob("Flights_*.csv"))
    
    if not csv_files:
        print(f"No Flights_*.csv files found in {base_dir}")
        return

    # Wait for the FastAPI server to be fully initialized
    if not wait_for_api():
        print("API did not become ready in time. Exiting upload process.")
        return

    endpoint = f"{API_BASE_URL}/{TARGET_TABLE}"

    for csv_path in csv_files:
        print(f"Loading and processing '{csv_path.name}'...")
        
        try:
            # 1. Read the raw CSV using pandas (handling potential encoding issues)
            try:
                raw_df = pd.read_csv(csv_path, encoding="utf-8-sig")
            except UnicodeDecodeError:
                raw_df = pd.read_csv(csv_path, encoding="latin1")
            
            # Clean column names just like in the ML training script
            raw_df.columns = [str(col).strip() for col in raw_df.columns]

            # 2. Apply feature engineering
            # keep_only_ml_features=False ensures we keep all original columns PLUS the new ML features.
            # If you want ONLY the strict ML columns in PostgreSQL, change this to True.
            processed_df = run_feature_engineering(raw_df, keep_only_ml_features=False)
            
            if processed_df.empty:
                print(f" [!] Skipping '{csv_path.name}': DataFrame is empty after filtering (no French domestic flights found).")
                continue

            # 3. Save the processed DataFrame to an in-memory CSV buffer
            csv_buffer = io.BytesIO()
            processed_df.to_csv(csv_buffer, index=False, encoding="utf-8")
            
            # Reset the buffer pointer to the beginning before sending
            csv_buffer.seek(0)
            
            print(f"Uploading transformed data for '{csv_path.name}' to table '{TARGET_TABLE}'...")
            
            # 4. Send the in-memory CSV to the FastAPI endpoint
            files = {'file': (csv_path.name, csv_buffer, 'text/csv')}
            response = requests.post(endpoint, files=files)
            
            if response.status_code == 200:
                print(f" [✓] Success: {response.json().get('message')}")
            else:
                print(f" [X] Upload failed ({response.status_code}): {response.text}")
                
        except Exception as exc:
            print(f" [!] Error processing or uploading {csv_path.name}: {exc}")


if __name__ == "__main__":
    main()