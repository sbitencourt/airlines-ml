from dst_airlines.extractors.aviationstack.airlines import (
    build_airlines_metrics,
    extract_airlines,
    extract_airlines_from_payload,
)


def test_extract_airlines_from_data():
    payload = {"data": [{"iata_code": "AR"}, {"iata_code": "AF"}]}
    result = extract_airlines_from_payload(payload)
    assert len(result) == 2


def test_extract_airlines_from_results():
    payload = {"results": [{"iata_code": "AR"}]}
    result = extract_airlines_from_payload(payload)
    assert len(result) == 1


def test_extract_airlines_from_multiple_payloads():
    payloads = [
        {"data": [{"iata_code": "AR"}]},
        {"data": [{"iata_code": "AF"}]},
    ]

    extracted = extract_airlines(payloads)
    assert len(extracted) == 2


def test_build_airlines_metrics():
    payloads = [
        {"data": [{"iata_code": "AR"}, {"iata_code": "AF"}]},
        {"data": [{"iata_code": "LH"}]},
    ]
    extracted = [
        {"iata_code": "AR"},
        {"iata_code": "AF"},
        {"iata_code": "LH"},
    ]

    metrics = build_airlines_metrics(payloads, extracted)

    assert metrics["pages_fetched"] == 2
    assert metrics["raw_count"] == 3
    assert metrics["extracted_count"] == 3