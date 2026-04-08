from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dst_airlines.config import RAW_DIR


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def build_raw_filename(
    source: str,
    endpoint: str,
    *,
    run_id: str | None = None,
    timestamp: str | None = None,
) -> str:
    source = source.strip().lower()
    endpoint = endpoint.strip().lower()

    suffix = run_id or timestamp or utc_now_compact()
    return f"{source}_{endpoint}_raw_{suffix}.json"


def save_raw_data(
    data: Any,
    *,
    source: str,
    endpoint: str,
    run_id: str | None = None,
    raw_dir: Path | None = None,
) -> Path:
    target_dir = raw_dir or RAW_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    file_name = build_raw_filename(
        source=source,
        endpoint=endpoint,
        run_id=run_id,
    )
    file_path = target_dir / file_name

    with open(file_path, "w", encoding="utf-8") as file_obj:
        json.dump(data, file_obj, indent=2, ensure_ascii=False)

    return file_path