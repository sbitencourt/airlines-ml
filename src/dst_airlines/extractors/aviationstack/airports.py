from __future__ import annotations

from typing import Any, Dict, List

from dst_airlines.utils.normalize import prune


def extract_airports_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    results = payload.get("results")
    if isinstance(results, list):
        return [item for item in results if isinstance(item, dict)]

    return []


def extract_airports(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []

    for payload in payloads:
        records = extract_airports_from_payload(payload)
        extracted.extend(prune(record) or record for record in records)

    return extracted


def build_airports_metrics(payloads: List[Dict[str, Any]], extracted: List[Dict[str, Any]]) -> Dict[str, int]:
    raw_count = 0
    for payload in payloads:
        raw_count += len(extract_airports_from_payload(payload))

    return {
        "pages_fetched": len(payloads),
        "raw_count": raw_count,
        "extracted_count": len(extracted),
    }