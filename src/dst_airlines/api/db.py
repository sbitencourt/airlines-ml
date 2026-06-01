from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from pymongo import MongoClient

from .config import get_settings


@contextmanager
def postgres_connection():
    """Create a PostgreSQL connection using POSTGRES_URI."""
    settings = get_settings()
    conn = psycopg2.connect(settings.postgres_uri)
    try:
        yield conn
    finally:
        conn.close()


def fetch_all(query: Any, params: list[Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with postgres_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or [])
            return [dict(row) for row in cur.fetchall()]


def fetch_one(query: Any, params: list[Any] | tuple[Any, ...] | None = None) -> dict[str, Any] | None:
    with postgres_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or [])
            row = cur.fetchone()
            return dict(row) if row else None


def business_table_identifier():
    settings = get_settings()
    return sql.Identifier(settings.sql_business_table)


def get_mongo_client() -> MongoClient:
    settings = get_settings()
    return MongoClient(settings.mongodb_uri, serverSelectionTimeoutMS=3000)
