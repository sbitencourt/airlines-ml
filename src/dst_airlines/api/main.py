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
    """Return main business KPIs from the SQL serving table/view."""

    query = sql.SQL(
        """
        SELECT
            COUNT(*)::int AS observed_flights,
            COUNT(*) FILTER (
                WHERE lower(coalesce(flight_status, '')) = 'active'
            )::int AS active_flights,
            COUNT(*) FILTER (
                WHERE lower(coalesce(flight_status, '')) = 'landed'
            )::int AS landed_flights,
            COUNT(*) FILTER (
                WHERE predicted_is_delayed = true
            )::int AS predicted_delayed_flights,
            COUNT(*)::int AS route_records
        FROM {table}
        """
    ).format(table=business_table_identifier())

    try:
        row = fetch_one(query)
        return row or {
            "observed_flights": 0,
            "active_flights": 0,
            "landed_flights": 0,
            "predicted_delayed_flights": 0,
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
        SELECT
            coalesce(flight_status, 'unknown') AS flight_status,
            COUNT(*)::int AS flights
        FROM {table}
        GROUP BY coalesce(flight_status, 'unknown')
        ORDER BY flights DESC
        """
    ).format(table=business_table_identifier())

    try:
        return fetch_all(query)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to read status summary: {exc}",
        )

@app.get("/flights/latest", tags=["flights"])
def latest_flights(limit: int = Query(20, ge=1, le=200)) -> list[dict[str, Any]]:
    """Return latest prediction rows from the SQL serving table/view."""
    query = sql.SQL(
        """
        SELECT *
        FROM {table}
        ORDER BY prediction_created_at DESC NULLS LAST,
                 departure_scheduled DESC NULLS LAST
        LIMIT %s
        """
    ).format(table=business_table_identifier())

    try:
        return fetch_all(query, [limit])
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to read latest flights: {exc}",
        )



def _get_business_table_columns() -> set[str]:
    """Return the available columns from the configured SQL business table/view."""
    query = sql.SQL("SELECT * FROM {table} LIMIT 0").format(
        table=business_table_identifier()
    )

    with postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return {column[0] for column in cur.description}


@app.get("/flights/search", tags=["flights"])
def search_flights(
    flight_iata: str = Query(
        ...,
        min_length=1,
        description="Flight identifier to search. Example: AF123, DL45, U24567.",
    ),
    limit: int = Query(10, ge=1, le=50),
) -> list[dict[str, Any]]:
    """Search flights in the SQL serving table/view by flight identifier.

    The endpoint checks which identifier columns exist in SQL_BUSINESS_TABLE and
    searches only those columns. This avoids hardcoding one specific schema.

    Recommended SQL column names, if available:
    - flight_iata
    - flight_icao
    - flight_number
    - iata
    - icao
    - number
    """
    search_value = f"%{flight_iata.strip()}%"

    candidate_columns = [
        "flight_iata",
        "flight_icao",
        "flight_number",
        "iata",
        "icao",
        "number",
    ]

    try:
        available_columns = _get_business_table_columns()
        searchable_columns = [
            column for column in candidate_columns if column in available_columns
        ]

        if not searchable_columns:
            raise HTTPException(
                status_code=400,
                detail=(
                    "No searchable flight identifier column was found in SQL_BUSINESS_TABLE. "
                    "Expected one of: flight_iata, flight_icao, flight_number, iata, icao, number."
                ),
            )

        where_clause = sql.SQL(" OR ").join(
            sql.SQL("CAST({column} AS TEXT) ILIKE %s").format(
                column=sql.Identifier(column)
            )
            for column in searchable_columns
        )

        query = sql.SQL(
            """
            SELECT *
            FROM {table}
            WHERE {where_clause}
            ORDER BY prediction_created_at DESC NULLS LAST,
                    departure_scheduled DESC NULLS LAST
            LIMIT %s
            """
        ).format(
            table=business_table_identifier(),
            where_clause=where_clause,
        )

        params = [search_value] * len(searchable_columns) + [limit]

        return fetch_all(query, params)

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to search flights in PostgreSQL: {exc}",
        )


@app.get("/mongo/flights/search", tags=["mongo"])
def search_raw_flights(
    flight_iata: str = Query(
        ...,
        min_length=1,
        description="Flight identifier to search in raw MongoDB documents.",
    ),
    limit: int = Query(10, ge=1, le=50),
) -> list[dict[str, Any]]:
    """Search raw/semi-structured flight documents from MongoDB by flight identifier."""
    search_value = flight_iata.strip()

    mongo_query = {
        "$or": [
            {"flight.iata": {"$regex": search_value, "$options": "i"}},
            {"flight.icao": {"$regex": search_value, "$options": "i"}},
            {"flight.number": {"$regex": search_value, "$options": "i"}},
            {"flight_iata": {"$regex": search_value, "$options": "i"}},
            {"flight_icao": {"$regex": search_value, "$options": "i"}},
            {"flight_number": {"$regex": search_value, "$options": "i"}},
        ]
    }

    try:
        client = get_mongo_client()
        db = client[settings.mongodb_db]
        collection = db[settings.mongodb_collection_flights]

        return list(
            collection.find(mongo_query, {"_id": 0}).limit(limit)
        )
    except PyMongoError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to search MongoDB flights: {exc}",
        )


@app.get("/routes/top", tags=["routes"])
def top_routes(limit: int = Query(10, ge=1, le=100)) -> list[dict[str, Any]]:
    """Return top routes based on departure and arrival IATA."""
    query = sql.SQL(
        """
        SELECT
            coalesce(departure_iata, 'unknown') AS departure_iata,
            coalesce(arrival_iata, 'unknown') AS arrival_iata,
            COUNT(*)::int AS flights,
            COUNT(*) FILTER (
                WHERE predicted_is_delayed = true
            )::int AS predicted_delayed_flights,
            AVG(delay_probability)::float AS avg_delay_probability
        FROM {table}
        GROUP BY coalesce(departure_iata, 'unknown'),
                 coalesce(arrival_iata, 'unknown')
        ORDER BY flights DESC
        LIMIT %s
        """
    ).format(table=business_table_identifier())

    try:
        return fetch_all(query, [limit])
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to read top routes: {exc}",
        )

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
