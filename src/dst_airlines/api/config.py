from functools import lru_cache
import os

from pydantic import BaseModel


class Settings(BaseModel):
    """Runtime settings loaded from .env.docker or container environment.

    Expected variables from your .env.docker:
    - MONGODB_URI
    - MONGODB_DB
    - MONGODB_COLLECTION_FLIGHTS
    - MONGODB_COLLECTION_AIRLINES
    - MONGODB_COLLECTION_AIRPORTS
    - POSTGRES_URI

    Optional variables:
    - API_APP_NAME
    - API_APP_VERSION
    - SQL_BUSINESS_TABLE
    """

    app_name: str = os.getenv("API_APP_NAME", "DST Airlines API")
    app_version: str = os.getenv("API_APP_VERSION", "0.1.0")

    mongodb_uri: str = os.getenv(
        "MONGODB_URI",
        "mongodb://root:passwd@mongodb:27017/?authSource=admin",
    )
    mongodb_db: str = os.getenv("MONGODB_DB", "dst_airlines")
    mongodb_collection_flights: str = os.getenv("MONGODB_COLLECTION_FLIGHTS", "flights")
    mongodb_collection_airlines: str = os.getenv("MONGODB_COLLECTION_AIRLINES", "airlines")
    mongodb_collection_airports: str = os.getenv("MONGODB_COLLECTION_AIRPORTS", "airports")

    postgres_uri: str = os.getenv(
        "POSTGRES_URI",
        "postgresql://root:passwd@postgres:5432/dst_airlines",
    )

    # SQL table/view prepared for Grafana and API consumption.
    # Set this to the exact table or view name that contains your calculated business indicators.
    sql_business_table: str = os.getenv("SQL_BUSINESS_TABLE", "business_flights")


@lru_cache
def get_settings() -> Settings:
    return Settings()
