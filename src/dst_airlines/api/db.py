import csv
import io
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from pymongo import MongoClient

from .config import get_settings


@contextmanager
def postgres_connection():
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


def insert_bulk_csv(table_name: str, file_obj: io.StringIO):
    """
    Read the CSV header to dynamically create the table if it does not exist,
    then insert bulk data using COPY.
    """
    reader = csv.reader(file_obj)
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("The provided CSV file is empty.")
        
    file_obj.seek(0)
    
    column_definitions = [f'"{col.strip()}" TEXT' for col in header]
    create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(map(sql.SQL, column_definitions))
    )

    with postgres_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(create_table_sql)
                copy_sql = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER DELIMITER ','").format(
                    sql.Identifier(table_name)
                )
                cur.copy_expert(copy_sql, file_obj)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e