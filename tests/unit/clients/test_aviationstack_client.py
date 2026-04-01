from dst_airlines.clients.aviationstack import AviationstackClient


def test_extract_records_from_data():
    payload = {"data": [{"id": 1}, {"id": 2}]}
    result = AviationstackClient._extract_records(payload)
    assert len(result) == 2


def test_extract_records_from_results():
    payload = {"results": [{"id": 1}]}
    result = AviationstackClient._extract_records(payload)
    assert len(result) == 1


def test_extract_records_from_empty_payload():
    payload = {}
    result = AviationstackClient._extract_records(payload)
    assert result == []