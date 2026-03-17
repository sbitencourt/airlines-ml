import json
import shutil
from pathlib import Path
from pymongo import MongoClient, UpdateOne

# Configurações de Caminho
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INCOMING_DIR = PROJECT_ROOT / "data" / "incoming"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

MONGO_URI = "mongodb://root:passwd@localhost:27017/"

def sync_airports_to_mongo():
    client = MongoClient(MONGO_URI)
    db = client["dev"]
    collection = db["raw_airports"]

    # Garante o índice único no campo customizado
    collection.create_index("airport_id", unique=True)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    target_files = list(INCOMING_DIR.glob("airports_processed*.json"))

    if not target_files:
        print("Nenhum arquivo em data/incoming.")
        return

    for file_path in target_files:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list): data = [data]

        updates = []
        for item in data:
            # Pegamos o 'id' da API e criamos o nosso 'airport_id'
            api_id = item.get("id")
            if api_id:
                item["airport_id"] = api_id # Mapeamento explícito
                
                updates.append(
                    UpdateOne(
                        {"airport_id": api_id}, # Busca pela nossa nova âncora
                        {"$set": item},
                        upsert=True
                    )
                )

        if updates:
            result = collection.bulk_write(updates)
            print(f"File {file_path.name}: {result.upserted_count} novos, {result.modified_count} atualizados.")
            
            shutil.move(str(file_path), str(PROCESSED_DIR / file_path.name))

    client.close()

if __name__ == "__main__":
    sync_airports_to_mongo()