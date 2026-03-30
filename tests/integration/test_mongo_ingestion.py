import json
from pathlib import Path
from pymongo import MongoClient
from etl.load.to_mongo import sync_flights_to_mongo


def test_mongo_ingestion(tmp_path, monkeypatch):
    # Crear estructura temporal tipo proyecto
    incoming = tmp_path / "data" / "incoming"
    processed = tmp_path / "data" / "processed"

    incoming.mkdir(parents=True)
    processed.mkdir(parents=True)

    # Sample Aviationstack payload
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

    # Variables de entorno para test
    monkeypatch.setenv("MONGODB_URI", "mongodb://localhost:27017/")
    monkeypatch.setenv("MONGODB_DB", "test_db")
    monkeypatch.setenv("MONGODB_COLLECTION", "test_flights")

    # ⚠️ IMPORTANTE: redirigir paths del script
    monkeypatch.setattr("etl.load.to_mongo.INCOMING_DIR", incoming)
    monkeypatch.setattr("etl.load.to_mongo.PROCESSED_DIR", processed)

    # Ejecutar loader
    sync_flights_to_mongo()

    # Verificar en Mongo
    client = MongoClient("mongodb://localhost:27017/")
    collection = client["test_db"]["test_flights"]

    count = collection.count_documents({})
    assert count == 1