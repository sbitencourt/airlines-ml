import json
import pytest
from dst_airlines.etl.transform.aviationstack_to_incoming import main


def test_transform_raw_to_incoming(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    incoming_dir = tmp_path / "data" / "incoming"

    raw_dir.mkdir(parents=True)
    incoming_dir.mkdir(parents=True)

    sample = {
        "data": [
            {
                "flight_date": "2026-02-27",
                "departure": {"iata": "THD"},
                "arrival": {"iata": "SGN"},
                "airline": {"iata": "VN"},
                "flight": {"number": "7091", "iata": "VN7091"},
            }
        ]
    }

    raw_file = raw_dir / "aviationstack_flights_raw_test.json"
    raw_file.write_text(json.dumps(sample), encoding="utf-8")

    monkeypatch.setattr(
        "etl.transform.aviationstack_to_incoming.RAW_DIR", raw_dir
    )
    monkeypatch.setattr(
        "etl.transform.aviationstack_to_incoming.INCOMING_DIR", incoming_dir
    )

    main(source="aviationstack", endpoint="flights")

    files = list(incoming_dir.glob("aviationstack_flights_incoming*.json"))
    assert len(files) == 1

    incoming_file = files[0]

    with open(incoming_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "data" in data
    assert isinstance(data["data"], list)
    assert len(data["data"]) == 1

    record = data["data"][0]
    assert record["flight_date"] == "2026-02-27"
    assert record["flight"]["iata"] == "VN7091"


def test_transform_fails_without_raw(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    incoming_dir = tmp_path / "data" / "incoming"

    raw_dir.mkdir(parents=True)
    incoming_dir.mkdir(parents=True)

    monkeypatch.setattr(
        "etl.transform.aviationstack_to_incoming.RAW_DIR", raw_dir
    )
    monkeypatch.setattr(
        "etl.transform.aviationstack_to_incoming.INCOMING_DIR", incoming_dir
    )

    with pytest.raises(FileNotFoundError):
        main(source="aviationstack", endpoint="flights")


def test_transform_multiple_files(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    incoming_dir = tmp_path / "data" / "incoming"

    raw_dir.mkdir(parents=True)
    incoming_dir.mkdir(parents=True)

    sample = {"data": [{"flight_date": "2026-02-27"}]}

    for i in range(2):
        file = raw_dir / f"aviationstack_flights_raw_test_{i}.json"
        file.write_text(json.dumps(sample), encoding="utf-8")

    monkeypatch.setattr(
        "etl.transform.aviationstack_to_incoming.RAW_DIR", raw_dir
    )
    monkeypatch.setattr(
        "etl.transform.aviationstack_to_incoming.INCOMING_DIR", incoming_dir
    )

    main(source="aviationstack", endpoint="flights")

    files = list(incoming_dir.glob("aviationstack_flights_incoming*.json"))
    assert len(files) == 2