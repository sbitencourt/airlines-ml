from pathlib import Path
import argparse
import json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"


def build_raw_pattern(source: str, endpoint: str) -> str:
    return f"{source}_{endpoint}_raw_*.json"


def main(source: str, endpoint: str):
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)

    pattern = build_raw_pattern(source, endpoint)
    raw_files = sorted(RAW_DIR.glob(pattern))

    print(f"[transform] source={source} endpoint={endpoint}")
    print(f"[transform] searching pattern={pattern}")

    if not raw_files:
        raise FileNotFoundError(
            f"No raw files found in data/raw matching {pattern}"
        )

    generated = 0

    for file_path in raw_files:
        with open(file_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        target_name = file_path.name.replace("_raw_", "_incoming_", 1)
        target_path = INCOMING_DIR / target_name

        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

        generated += 1
        print(f"[transform] generated: {target_path}")

    if generated == 0:
        raise RuntimeError(
            f"Transform finished but produced no incoming files for {source}/{endpoint}"
        )

    print(f"[transform] total generated files: {generated}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform raw aviationstack data into incoming format"
    )

    parser.add_argument(
        "--source",
        required=True,
        help="Data source (e.g. aviationstack)",
    )

    parser.add_argument(
        "--endpoint",
        required=True,
        help="Endpoint name (e.g. flights, airlines, airports)",
    )

    args = parser.parse_args()

    main(
        source=args.source.strip().lower(),
        endpoint=args.endpoint.strip().lower(),
    )