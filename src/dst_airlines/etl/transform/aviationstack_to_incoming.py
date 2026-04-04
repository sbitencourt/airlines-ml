from pathlib import Path
import argparse
import json

def get_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("Project root not found")


PROJECT_ROOT = get_project_root()

RAW_DIR = PROJECT_ROOT / "data" / "raw"
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"


def build_raw_pattern(source: str, endpoint: str, run_id: str | None = None) -> str:
    source = source.strip().lower()
    endpoint = endpoint.strip().lower()

    if run_id:
        return f"{source}_{endpoint}_raw_{run_id}.json"

    return f"{source}_{endpoint}_raw_*.json"


def main(source: str, endpoint: str, run_id: str | None = None):
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)

    pattern = build_raw_pattern(source, endpoint, run_id=run_id)
    raw_files = sorted(RAW_DIR.glob(pattern))

    print(f"[transform] source={source} endpoint={endpoint} run_id={run_id}")
    print(f"[transform] searching pattern={pattern}")

    if not raw_files:
        raise FileNotFoundError(
            f"No raw files found in data/raw matching {pattern}"
        )

    generated = 0
    generated_files: list[str] = []

    for file_path in raw_files:
        with open(file_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        target_name = file_path.name.replace("_raw_", "_incoming_", 1)
        target_path = INCOMING_DIR / target_name

        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

        generated += 1
        generated_files.append(str(target_path))
        print(f"[transform] generated: {target_path}")

    if generated == 0:
        raise RuntimeError(
            f"Transform finished but produced no incoming files for {source}/{endpoint}"
        )

    print(f"[transform] total generated files: {generated}")

    return {
        "source": source,
        "endpoint": endpoint,
        "run_id": run_id,
        "generated_files": generated,
        "files": generated_files,
    }


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

    parser.add_argument(
        "--run-id",
        required=False,
        help="Run id to transform only files from the current execution",
    )

    args = parser.parse_args()

    main(
        source=args.source.strip().lower(),
        endpoint=args.endpoint.strip().lower(),
        run_id=args.run_id,
    )