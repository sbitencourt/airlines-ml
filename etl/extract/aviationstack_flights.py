from dst_airlines.clients.aviationstack import AviationstackClient
from dst_airlines.extractors.aviationstack.flights import (
    build_flights_metrics,
    extract_in_air_flights,
)
from dst_airlines.io.raw_writer import save_raw_data


def main(run_id: str | None = None) -> None:
    print("[extract] Fetching aviationstack flights...")

    client = AviationstackClient.from_env("API_URL_AVIATIONSTACK_FLIGHTS")
    payload = client.fetch_flights_raw()

    raw_path = save_raw_data(
        payload,
        source="aviationstack",
        endpoint="flights",
        run_id=run_id,
    )

    extracted = extract_in_air_flights(payload)
    metrics = build_flights_metrics(payload, extracted)

    print(f"[extract] Raw payload saved to: {raw_path}")
    print(f"[extract] Flights in payload: {metrics['raw_count']}")
    print(f"[extract] In-air flights detected: {metrics['extracted_count']}")


if __name__ == "__main__":
    main()