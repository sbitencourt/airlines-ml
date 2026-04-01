from dst_airlines.extractors.aviationstack.airports import (
    build_airports_metrics,
    extract_airports,
    extract_airports_from_payload,
)


def test_extract_airports_from_data():
    payload = {"data": [{"iata_code": "EZE"}, {"iata_code": "AEP"}]}
    result = extract_airports_from_payload(payload)
    assert len(result) == 2


def test_extract_airports_from_results():
    payload = {"results": [{"iata_code": "EZE"}]}
    result = extract_airports_from_payload(payload)
    assert len(result) == 1


def test_extract_airports_from_multiple_payloads():
    payloads = [
        {"data": [{"iata_code": "EZE"}]},
        {"data": [{"iata_code": "AEP"}]},
    ]

    extracted = extract_airports(payloads)
    assert len(extracted) == 2


def test_build_airports_metrics():
    payloads = [
        {"data": [{"iata_code": "EZE"}, {"iata_code": "AEP"}]},
        {"data": [{"iata_code": "COR"}]},
    ]
    extracted = [{"iata_code": "EZE"}, {"iata_code": "AEP"}, {"iata_code": "COR"}]

    metrics = build_airports_metrics(payloads, extracted)

    assert metrics["pages_fetched"] == 2
    assert metrics["raw_count"] == 3
    assert metrics["extracted_count"] == 3