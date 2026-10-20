"""Public Vercel Function that enqueues orders for the queue-consumer daemon."""

import functools
import logging
import os

from fastapi import FastAPI, Header, status
from vercel.queue import QueueClient

from app import db
from app.orders import EnqueueResponse, Order, check_api_token, enqueue_order

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())

app = FastAPI(title="Orders API")


@functools.cache
def get_queue() -> QueueClient:
    return QueueClient(region=os.getenv("VERCEL_REGION"))


@app.post(
    "/api/orders",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=EnqueueResponse,
)
async def create_order(
    order: Order,
    idempotency_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> EnqueueResponse:
    check_api_token(authorization)
    return await enqueue_order(get_queue(), order, idempotency_key)


@app.get("/api/orders")
async def list_orders(limit: int = 100) -> dict[str, list[dict[str, object]]]:
    return {"orders": await db.list_orders(min(max(limit, 1), 500))}
