from dst_airlines.etl.load.to_mongo_flights import (
    extract_records,
    build_flight_key,
    is_valid_flight_key,
)


# -------- extract_records --------

def test_extract_records_from_list():
    payload = [{"a": 1}, {"b": 2}]
    result = extract_records(payload)
    assert result == payload


def test_extract_records_from_dict_data():
    payload = {"data": [{"a": 1}]}
    result = extract_records(payload)
    assert result == payload["data"]


def test_extract_records_from_dict_results():
    payload = {"results": [{"a": 1}]}
    result = extract_records(payload)
    assert result == payload["results"]


def test_extract_records_fallback_to_single():
    payload = {"foo": "bar"}
    result = extract_records(payload)
    assert result == [payload]


def test_extract_records_invalid_type():
    payload = "invalid"
    result = extract_records(payload)
    assert result == []


# -------- build_flight_key --------

def test_build_flight_key_complete():
    item = {
        "flight_date": "2026-02-27",
        "flight": {"iata": "VN7091"},
        "departure": {"iata": "THD"},
        "arrival": {"iata": "SGN"},
        "airline": {"iata": "VN"},
    }

    key = build_flight_key(item)

    assert key["flight_date"] == "2026-02-27"
    assert key["flight_number"] == "VN7091"
    assert key["departure_iata"] == "THD"
    assert key["arrival_iata"] == "SGN"
    assert key["airline_iata"] == "VN"


def test_build_flight_key_fallback_number():
    item = {
        "flight_date": "2026-02-27",
        "flight": {"number": "7091"},
    }

    key = build_flight_key(item)

    assert key["flight_number"] == "7091"


def test_build_flight_key_fallback_icao():
    item = {
        "flight_date": "2026-02-27",
        "flight": {"icao": "HVN7091"},
    }

    key = build_flight_key(item)

    assert key["flight_number"] == "HVN7091"


# -------- is_valid_flight_key --------

def test_is_valid_flight_key_true():
    key = {
        "flight_date": "2026-02-27",
        "flight_number": "VN7091",
    }
    assert is_valid_flight_key(key)


def test_is_valid_flight_key_false_missing_date():
    key = {
        "flight_date": None,
        "flight_number": "VN7091",
    }
    assert not is_valid_flight_key(key)


def test_is_valid_flight_key_false_missing_number():
    key = {
        "flight_date": "2026-02-27",
        "flight_number": None,
    }
    assert not is_valid_flight_key(key)