from pathlib import Path
import json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"


def main():
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)

    raw_files = sorted(RAW_DIR.glob("aviationstack_*_raw_*.json"))
    if not raw_files:
        raise FileNotFoundError(
            "No raw files found in data/raw matching aviationstack_*_raw_*.json"
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
        raise RuntimeError("Transform finished but produced no incoming files.")

    print(f"[transform] total generated files: {generated}")


if __name__ == "__main__":
    main()