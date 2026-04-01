from dst_airlines.extractors.aviationstack.flights import (
    build_flights_metrics,
    extract_flights,
    extract_in_air_flights,
    is_in_air,
)


def test_extract_flights_from_data():
    payload = {"data": [{"id": 1}, {"id": 2}]}
    result = extract_flights(payload)
    assert len(result) == 2


def test_extract_flights_from_results():
    payload = {"results": [{"id": 1}]}
    result = extract_flights(payload)
    assert len(result) == 1


def test_is_in_air_from_live_flag():
    flight = {"live": {"is_ground": False}}
    assert is_in_air(flight)


def test_is_in_air_from_status_fallback():
    flight = {"flight_status": "active"}
    assert is_in_air(flight)


def test_extract_in_air_flights_filters_non_active():
    payload = {
        "data": [
            {"flight_status": "active", "flight": {"iata": "AA100"}},
            {"flight_status": "landed", "flight": {"iata": "AA200"}},
        ]
    }

    extracted = extract_in_air_flights(payload)
    assert len(extracted) == 1
    assert extracted[0]["flight"]["iata"] == "AA100"


def test_build_flights_metrics():
    payload = {"data": [{"id": 1}, {"id": 2}, {"id": 3}]}
    extracted = [{"id": 1}, {"id": 2}]
    metrics = build_flights_metrics(payload, extracted)

    assert metrics["raw_count"] == 3
    assert metrics["extracted_count"] == 2