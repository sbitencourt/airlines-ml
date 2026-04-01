from __future__ import annotations

from typing import Dict, List

from dst_airlines.clients.aviationstack import AviationstackClient


def extract_airlines_from_payload(payload: Dict) -> List[Dict]:
    return AviationstackClient._extract_records(payload)


def extract_airlines(payloads: List[Dict]) -> List[Dict]:
    extracted: List[Dict] = []

    for payload in payloads:
        extracted.extend(extract_airlines_from_payload(payload))

    return extracted


def build_airlines_metrics(payloads: List[Dict], extracted: List[Dict]) -> Dict[str, int]:
    raw_count = 0
    for payload in payloads:
        raw_count += len(extract_airlines_from_payload(payload))

    return {
        "pages_fetched": len(payloads),
        "raw_count": raw_count,
        "extracted_count": len(extracted),
    }