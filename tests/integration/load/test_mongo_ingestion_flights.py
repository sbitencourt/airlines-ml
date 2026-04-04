import json
from pymongo import MongoClient

from dst_airlines.etl.load.to_mongo_flights import sync_flights_to_mongo


def test_mongo_ingestion_flights(tmp_path, monkeypatch):
    incoming = tmp_path / "data" / "incoming"
    processed = tmp_path / "data" / "processed"

    incoming.mkdir(parents=True)
    processed.mkdir(parents=True)

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

    file_path = incoming / "aviationstack_flights_incoming_test.json"
    file_path.write_text(json.dumps(sample), encoding="utf-8")

    run_id = "test_run_001"

    monkeypatch.setattr("etl.load.to_mongo_flights.INCOMING_DIR", incoming)
    monkeypatch.setattr("etl.load.to_mongo_flights.PROCESSED_DIR", processed)

    mongodb_uri = "mongodb://root:passwd@localhost:27017/?authSource=admin"
    mongodb_db = "test_db"
    mongodb_collection = "test_flights"

    sync_flights_to_mongo(
        source="aviationstack",
        endpoint="flights",
        run_id=run_id,
        mongodb_uri=mongodb_uri,
        mongodb_db=mongodb_db,
        mongodb_collection=mongodb_collection,
    )

    client = MongoClient(mongodb_uri)
    try:
        collection = client[mongodb_db][mongodb_collection]

        doc = collection.find_one({"flight_number": "VN7091"})
        assert doc is not None
        assert doc["flight_date"] == "2026-02-27"
        assert doc["departure_iata"] == "THD"
        assert doc["arrival_iata"] == "SGN"
        assert doc["airline_iata"] == "VN"

        assert "_meta" in doc
        assert doc["_meta"]["run_id"] == run_id
        assert doc["_meta"]["source"] == "aviationstack"
        assert doc["_meta"]["endpoint"] == "flights"
        assert "loaded_at" in doc["_meta"]

    finally:
        client.close()

    assert not file_path.exists()

    moved_files = list(processed.glob(f"{run_id}__aviationstack_flights_incoming_test.json"))
    assert len(moved_files) == 1