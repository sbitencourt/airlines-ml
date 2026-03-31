# etl/transform/aviationstack_to_incoming.py

from pathlib import Path
import json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"

def main():
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)

    for file in RAW_DIR.glob("aviationstack_raw*.json"):
        with open(file, "r", encoding="utf-8") as f:
            payload = json.load(f)

        # Podés filtrar/transformar acá si querés
        target = INCOMING_DIR / file.name.replace("raw", "incoming")

        with open(target, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

if __name__ == "__main__":
    main()