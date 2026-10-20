"""Postgres (Neon) persistence for order status."""

import json
import os
from typing import Any

import psycopg
from psycopg.rows import dict_row

_SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id     TEXT PRIMARY KEY,
    customer_id  TEXT NOT NULL,
    items        JSONB NOT NULL,
    status       TEXT NOT NULL DEFAULT 'queued',
    message_id   TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_schema_ready = False


async def connect() -> psycopg.AsyncConnection[dict[str, Any]]:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be set")
    conn = await psycopg.AsyncConnection.connect(url, row_factory=dict_row)
    global _schema_ready
    if not _schema_ready:
        await conn.execute(_SCHEMA)
        await conn.commit()
        _schema_ready = True
    return conn


async def insert_order(order_id: str, customer_id: str, items: list[dict[str, Any]]) -> bool:
    """Insert a queued order. Returns False if the order_id already exists."""
    async with await connect() as conn:
        cur = await conn.execute(
            """
            INSERT INTO orders (order_id, customer_id, items)
            VALUES (%s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """,
            (order_id, customer_id, json.dumps(items)),
        )
        return cur.rowcount == 1


async def set_message_id(order_id: str, message_id: str | None) -> None:
    async with await connect() as conn:
        await conn.execute(
            "UPDATE orders SET message_id = %s, updated_at = now() WHERE order_id = %s",
            (message_id, order_id),
        )


async def delete_order(order_id: str) -> None:
    async with await connect() as conn:
        await conn.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))


async def set_status(
    order_id: str,
    status: str,
    *,
    attempts: int | None = None,
    error: str | None = None,
) -> None:
    async with await connect() as conn:
        await conn.execute(
            """
            UPDATE orders
               SET status = %s,
                   attempts = COALESCE(%s, attempts),
                   error = %s,
                   updated_at = now()
             WHERE order_id = %s
            """,
            (status, attempts, error, order_id),
        )


async def list_orders(limit: int = 100) -> list[dict[str, Any]]:
    async with await connect() as conn:
        cur = await conn.execute(
            """
            SELECT order_id, customer_id, items, status, message_id, attempts,
                   error, created_at, updated_at
              FROM orders
             ORDER BY created_at DESC
             LIMIT %s
            """,
            (limit,),
        )
        return await cur.fetchall()
