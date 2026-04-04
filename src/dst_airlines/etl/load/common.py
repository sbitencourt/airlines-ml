import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid4().hex[:8]}"


def log_event(level: str, stage: str, event: str, **kwargs) -> None:
    payload = {
        "timestamp": utc_now_iso(),
        "level": level.upper(),
        "stage": stage,
        "event": event,
        **kwargs,
    }
    print(json.dumps(payload, ensure_ascii=False))


def build_incoming_pattern(
    source: str,
    endpoint: str,
    run_id: str | None = None,
) -> str:
    source = source.strip().lower()
    endpoint = endpoint.strip().lower()

    if run_id:
        return f"{source}_{endpoint}_incoming_{run_id}.json"

    return f"{source}_{endpoint}_incoming_*.json"


def build_processed_destination(
    processed_dir: Path,
    run_id: str,
    file_name: str,
) -> Path:
    return processed_dir / f"{run_id}__{file_name}"


def build_document_meta(run_id: str, source: str, endpoint: str) -> dict:
    return {
        "run_id": run_id,
        "source": source,
        "endpoint": endpoint,
        "loaded_at": utc_now_iso(),
    }