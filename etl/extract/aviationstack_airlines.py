import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from dst_airlines.clients.aviationstack import AviationstackClient
from dst_airlines.extractors.aviationstack.airlines import (
    build_airlines_metrics,
    extract_airlines,
)
from dst_airlines.io.raw_writer import save_raw_data


def main() -> None:
    print("[extract] Fetching aviationstack airlines...")

    client = AviationstackClient.from_env()
    payloads = client.fetch_airlines_raw_all(limit=100, max_pages=1)

    raw_path = save_raw_data(
        payloads,
        source="aviationstack",
        endpoint="airlines",
    )

    extracted = extract_airlines(payloads)
    metrics = build_airlines_metrics(payloads, extracted)

    print(f"[extract] Raw payload saved to: {raw_path}")
    print(f"[extract] Pages fetched: {metrics['pages_fetched']}")
    print(f"[extract] Airlines in payloads: {metrics['raw_count']}")
    print(f"[extract] Airlines extracted: {metrics['extracted_count']}")


if __name__ == "__main__":
    main()