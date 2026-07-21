import json
import os
import urllib.request
from urllib.parse import urlencode
from typing import Any, Dict, List

from dst_airlines.extractors.aviationstack.flights import (
    build_flights_metrics,
    extract_in_air_flights,
)
from dst_airlines.io.raw_writer import save_raw_data

# Key French domestic routes (Departure IATA, Arrival IATA)
FRENCH_DOMESTIC_ROUTES = [
    ("ORY", "MRS"), ("ORY", "NCE"), ("ORY", "TLS"), ("ORY", "BOD"), ("ORY", "MPL"),
    ("CDG", "NCE"), ("CDG", "TLS"), ("CDG", "LYS"), ("CDG", "MRS"), ("CDG", "BOD"),
    ("LYS", "BOD"), ("LYS", "NTE"), ("LYS", "TLS"), ("MRS", "BOD"), ("NCE", "NTE"),
]

def fetch_french_domestic_flights_payload() -> Dict[str, Any]:
    """
    Queries Aviationstack for specific French domestic routes (dep_iata & arr_iata)
    and filters by 'scheduled' status, bypassing AviationstackClient to guarantee 
    query parameters are sent correctly.
    """
    combined_data: List[Dict[str, Any]] = []
    
    # Retrieves the API Key directly from the environment
    access_key = os.getenv("AVIATIONSTACK_ACCESS_KEY", "2bceda447a72d7e8f24804c3b0a2d4de")
    base_url = "http://api.aviationstack.com/v1/flights"

    for dep_iata, arr_iata in FRENCH_DOMESTIC_ROUTES:
        try:
            # Build the query string with the scheduled status filter
            query_params = urlencode({
                "access_key": access_key,
                "dep_iata": dep_iata,
                "arr_iata": arr_iata,
                "flight_status": "scheduled"
            })
            url = f"{base_url}?{query_params}"
            
            req = urllib.request.Request(url, headers={'Accept': 'application/json'})
            
            with urllib.request.urlopen(req) as response:
                response_data = json.loads(response.read().decode("utf-8"))
                flights = response_data.get("data", [])
                
                if isinstance(flights, list):
                    combined_data.extend(flights)
                    
        except Exception as exc:
            print(f"[extract] Error fetching route {dep_iata} -> {arr_iata}: {exc}")

    return {
        "pagination": {
            "limit": len(combined_data),
            "offset": 0,
            "count": len(combined_data),
            "total": len(combined_data),
        },
        "data": combined_data,
    }


def main(run_id: str | None = None) -> None:
    print("[extract] Fetching scheduled French domestic aviationstack flights...")

    # Fetch payload directly via native urllib to ensure parameters work
    payload = fetch_french_domestic_flights_payload()

    raw_path = save_raw_data(
        payload,
        source="aviationstack",
        endpoint="flights",
        run_id=run_id,
    )

    extracted = extract_in_air_flights(payload)
    metrics = build_flights_metrics(payload, extracted)

    print(f"[extract] Raw payload saved to: {raw_path}")
    print(f"[extract] Total raw domestic scheduled flights in payload: {metrics['raw_count']}")
    
    # Note: If extract_in_air_flights looks for 'active' flights, this will likely be 0
    print(f"[extract] Flights detected by extractor: {metrics['extracted_count']}")


if __name__ == "__main__":
    main()