import json
from pymongo import MongoClient

from etl.load.to_mongo_airports import sync_airports_to_mongo



def test_mongo_ingestion_airports(tmp_path, monkeypatch):
    incoming = tmp_path / "data" / "incoming"
    processed = tmp_path / "data" / "processed"
    incoming.mkdir(parents=True)
    processed.mkdir(parents=True)

    sample = {
        "data": [
            {
                "iata_code": "EZE",
                "icao_code": "SAEZ",
                "airport_name": "Ministro Pistarini International Airport",
            }
        ]
    }

    file_path = incoming / "aviationstack_airports_incoming_test.json"
    file_path.write_text(json.dumps(sample), encoding="utf-8")

    run_id = "test_run_airports"

    monkeypatch.setattr("etl.load.to_mongo_airports.INCOMING_DIR", incoming)
    monkeypatch.setattr("etl.load.to_mongo_airports.PROCESSED_DIR", processed)

    mongodb_uri = "mongodb://root:passwd@localhost:27017/?authSource=admin"
    mongodb_db = "test_db"
    mongodb_collection = "test_airports"

    sync_airports_to_mongo(
        source="aviationstack",
        endpoint="airports",
        run_id=run_id,
        mongodb_uri=mongodb_uri,
        mongodb_db=mongodb_db,
        mongodb_collection=mongodb_collection,
    )

    client = MongoClient(mongodb_uri)
    try:
        collection = client[mongodb_db][mongodb_collection]
        doc = collection.find_one({"airport_iata": "EZE"})
        assert doc is not None
        assert doc["airport_icao"] == "SAEZ"
        assert doc["airport_name"] == "Ministro Pistarini International Airport"
        assert doc["_meta"]["endpoint"] == "airports"
    finally:
        client.close()

    assert not file_path.exists()
    assert len(list(processed.glob(f"{run_id}__aviationstack_airports_incoming_test.json"))) == 1

