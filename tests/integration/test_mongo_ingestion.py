import json
from pymongo import MongoClient
from etl.load.to_mongo import sync_flights_to_mongo


def test_mongo_ingestion(tmp_path, monkeypatch):
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

    file_path = incoming / "aviationstack_test.json"
    file_path.write_text(json.dumps(sample), encoding="utf-8")

    monkeypatch.setenv(
        "MONGODB_URI",
        "mongodb://root:passwd@localhost:27017/?authSource=admin"
    )
    monkeypatch.setenv("MONGODB_DB", "test_db")
    monkeypatch.setenv("MONGODB_COLLECTION", "test_flights")

    monkeypatch.setattr("etl.load.to_mongo.INCOMING_DIR", incoming)
    monkeypatch.setattr("etl.load.to_mongo.PROCESSED_DIR", processed)

    sync_flights_to_mongo()

    client = MongoClient("mongodb://root:passwd@localhost:27017/?authSource=admin")
    try:
        collection = client["test_db"]["test_flights"]

        count = collection.count_documents({})
        assert count == 1

        doc = collection.find_one({"flight_number": "VN7091"})
        assert doc is not None
        assert doc["flight_date"] == "2026-02-27"
        assert doc["departure_iata"] == "THD"
        assert doc["arrival_iata"] == "SGN"
        assert doc["airline_iata"] == "VN"

    finally:
        client.close()

    assert not file_path.exists()
    assert (processed / "aviationstack_test.json").exists()
