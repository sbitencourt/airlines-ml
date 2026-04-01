from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def build_raw_filename(source: str, endpoint: str, timestamp: str | None = None) -> str:
    ts = timestamp or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{source}_{endpoint}_raw_{ts}.json"


def save_raw_data(
    data: Any,
    *,
    source: str,
    endpoint: str,
    raw_dir: Path | None = None,
) -> Path:
    target_dir = raw_dir or RAW_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    file_name = build_raw_filename(source=source, endpoint=endpoint)
    file_path = target_dir / file_name

    with open(file_path, "w", encoding="utf-8") as file_obj:
        json.dump(data, file_obj, indent=2, ensure_ascii=False)

    return file_path