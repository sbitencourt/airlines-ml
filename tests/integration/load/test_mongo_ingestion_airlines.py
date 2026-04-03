import json
from pymongo import MongoClient


from etl.load.to_mongo_airlines import sync_airlines_to_mongo


def test_mongo_ingestion_airlines(tmp_path, monkeypatch):
    incoming = tmp_path / "data" / "incoming"
    processed = tmp_path / "data" / "processed"
    incoming.mkdir(parents=True)
    processed.mkdir(parents=True)

    sample = {
        "data": [
            {
                "iata_code": "AR",
                "icao_code": "ARG",
                "airline_name": "Aerolineas Argentinas",
            }
        ]
    }

    file_path = incoming / "aviationstack_airlines_incoming_test.json"
    file_path.write_text(json.dumps(sample), encoding="utf-8")

    run_id = "test_run_airlines"

    monkeypatch.setattr("etl.load.to_mongo_airlines.INCOMING_DIR", incoming)
    monkeypatch.setattr("etl.load.to_mongo_airlines.PROCESSED_DIR", processed)

    mongodb_uri = "mongodb://root:passwd@localhost:27017/?authSource=admin"
    mongodb_db = "test_db"
    mongodb_collection = "test_airlines"

    sync_airlines_to_mongo(
        source="aviationstack",
        endpoint="airlines",
        run_id=run_id,
        mongodb_uri=mongodb_uri,
        mongodb_db=mongodb_db,
        mongodb_collection=mongodb_collection,
    )

    client = MongoClient(mongodb_uri)
    try:
        collection = client[mongodb_db][mongodb_collection]
        doc = collection.find_one({"airline_iata": "AR"})
        assert doc is not None
        assert doc["airline_icao"] == "ARG"
        assert doc["airline_name"] == "Aerolineas Argentinas"
        assert doc["_meta"]["endpoint"] == "airlines"
    finally:
        client.close()

    assert not file_path.exists()
    assert len(list(processed.glob(f"{run_id}__aviationstack_airlines_incoming_test.json"))) == 1