import json
from etl.transform.aviationstack_to_incoming import main


def test_transform_raw_to_incoming(tmp_path, monkeypatch):
    # Simulate project-like directory structure
    raw_dir = tmp_path / "data" / "raw"
    incoming_dir = tmp_path / "data" / "incoming"

    raw_dir.mkdir(parents=True)
    incoming_dir.mkdir(parents=True)

    # Sample payload (similar to Aviationstack response)
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

    raw_file = raw_dir / "aviationstack_raw_test.json"
    raw_file.write_text(json.dumps(sample), encoding="utf-8")

    # Monkeypatch paths
    monkeypatch.setattr(
        "etl.transform.aviationstack_to_incoming.RAW_DIR", raw_dir
    )
    monkeypatch.setattr(
        "etl.transform.aviationstack_to_incoming.INCOMING_DIR", incoming_dir
    )

    # Run transform
    main()

    # Verify that a file was created in incoming
    files = list(incoming_dir.glob("*.json"))
    assert len(files) == 1

    incoming_file = files[0]

    # Verify content
    with open(incoming_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "data" in data
    assert isinstance(data["data"], list)
    assert len(data["data"]) == 1

    record = data["data"][0]
    assert record["flight_date"] == "2026-02-27"
    assert record["flight"]["iata"] == "VN7091"