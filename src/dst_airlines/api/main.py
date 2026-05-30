from typing import Any

from fastapi import FastAPI, HTTPException, Query
from psycopg2 import sql
from pymongo.errors import PyMongoError

from .config import get_settings
from .db import (
    business_table_identifier,
    fetch_all,
    fetch_one,
    get_mongo_client,
    postgres_connection,
)

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Minimal REST API for DST Airlines. Grafana can continue reading directly "
        "from PostgreSQL and Prometheus, while this API exposes data for consumers "
        "such as Dash, Streamlit, Swagger, or external clients."
    ),
)


@app.get("/health", tags=["health"])
def health() -> dict[str, Any]:
    """Check API, PostgreSQL and MongoDB connectivity."""
    checks: dict[str, Any] = {
        "api": "ok",
        "postgres": "unknown",
        "mongodb": "unknown",
        "mongodb_db": settings.mongodb_db,
        "sql_business_table": settings.sql_business_table,
    }

    try:
        with postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"

    try:
        client = get_mongo_client()
        client.admin.command("ping")
        checks["mongodb"] = "ok"
    except Exception as exc:
        checks["mongodb"] = f"error: {exc}"

    checks["status"] = (
        "ok" if checks["postgres"] == "ok" and checks["mongodb"] == "ok" else "degraded"
    )
    return checks


@app.get("/business/kpis", tags=["business"])
def business_kpis() -> dict[str, int]:
    """Return main business KPIs from the SQL serving table/view.

    Expected column in SQL_BUSINESS_TABLE:
    - status: flight status, e.g. active, scheduled, delayed

    The table/view name is configurable with SQL_BUSINESS_TABLE.
    """
    query = sql.SQL(
        """
        SELECT
            COUNT(*)::int AS observed_flights,
            COUNT(*) FILTER (WHERE lower(coalesce(status, '')) = 'active')::int AS active_flights,
            COUNT(*) FILTER (WHERE lower(coalesce(status, '')) = 'delayed')::int AS delayed_flights,
            COUNT(*)::int AS route_records
        FROM {table}
        """
    ).format(table=business_table_identifier())

    try:
        row = fetch_one(query)
        return row or {
            "observed_flights": 0,
            "active_flights": 0,
            "delayed_flights": 0,
            "route_records": 0,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                "Unable to read business KPIs from PostgreSQL. "
                "Check SQL_BUSINESS_TABLE and required columns. "
                f"Original error: {exc}"
            ),
        )


@app.get("/flights/status-summary", tags=["flights"])
def flight_status_summary() -> list[dict[str, Any]]:
    """Count flights by status from the SQL serving table/view."""
    query = sql.SQL(
        """
        SELECT coalesce(status, 'unknown') AS status, COUNT(*)::int AS flights
        FROM {table}
        GROUP BY coalesce(status, 'unknown')
        ORDER BY flights DESC
        """
    ).format(table=business_table_identifier())

    try:
        return fetch_all(query)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Unable to read status summary: {exc}")


@app.get("/flights/latest", tags=["flights"])
def latest_flights(limit: int = Query(20, ge=1, le=200)) -> list[dict[str, Any]]:
    """Return recent rows from the SQL serving table/view.

    If your table has a date column, you can add an ORDER BY clause adapted to your schema.
    """
    query = sql.SQL("SELECT * FROM {table} LIMIT %s").format(table=business_table_identifier())

    try:
        return fetch_all(query, [limit])
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Unable to read latest flights: {exc}")


@app.get("/routes/regions", tags=["routes"])
def route_regions(limit: int = Query(10, ge=1, le=100)) -> list[dict[str, Any]]:
    """Return origin-region to destination-region route counts.

    Expected columns in SQL_BUSINESS_TABLE:
    - origin_region
    - destination_region
    """
    query = sql.SQL(
        """
        SELECT
            coalesce(origin_region, 'unknown') AS origin_region,
            coalesce(destination_region, 'unknown') AS destination_region,
            COUNT(*)::int AS flights
        FROM {table}
        GROUP BY coalesce(origin_region, 'unknown'), coalesce(destination_region, 'unknown')
        ORDER BY flights DESC
        LIMIT %s
        """
    ).format(table=business_table_identifier())

    try:
        return fetch_all(query, [limit])
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Unable to read route regions: {exc}")


def _mongo_find(collection_name: str, limit: int) -> list[dict[str, Any]]:
    client = get_mongo_client()
    db = client[settings.mongodb_db]
    collection = db[collection_name]
    return list(collection.find({}, {"_id": 0}).limit(limit))


@app.get("/mongo/flights/latest", tags=["mongo"])
def latest_raw_flights(limit: int = Query(10, ge=1, le=100)) -> list[dict[str, Any]]:
    """Return raw/semi-structured flight documents from MongoDB."""
    try:
        return _mongo_find(settings.mongodb_collection_flights, limit)
    except PyMongoError as exc:
        raise HTTPException(status_code=503, detail=f"Unable to read MongoDB flights: {exc}")


@app.get("/mongo/airlines/latest", tags=["mongo"])
def latest_raw_airlines(limit: int = Query(10, ge=1, le=100)) -> list[dict[str, Any]]:
    """Return raw/semi-structured airline documents from MongoDB."""
    try:
        return _mongo_find(settings.mongodb_collection_airlines, limit)
    except PyMongoError as exc:
        raise HTTPException(status_code=503, detail=f"Unable to read MongoDB airlines: {exc}")


@app.get("/mongo/airports/latest", tags=["mongo"])
def latest_raw_airports(limit: int = Query(10, ge=1, le=100)) -> list[dict[str, Any]]:
    """Return raw/semi-structured airport documents from MongoDB."""
    try:
        return _mongo_find(settings.mongodb_collection_airports, limit)
    except PyMongoError as exc:
        raise HTTPException(status_code=503, detail=f"Unable to read MongoDB airports: {exc}")
