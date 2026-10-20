"""Order payloads and enqueue logic shared by the API and the consumer."""

import hmac
import logging
import os

from fastapi import HTTPException, status
from pydantic import BaseModel, Field
from vercel.queue import DuplicateIdempotencyKeyError, QueueClient, QueueError

from app import db

logger = logging.getLogger("orders")

QUEUE_TOPIC = os.getenv("QUEUE_TOPIC", "orders")


class OrderItem(BaseModel):
    sku: str = Field(min_length=1)
    quantity: int = Field(gt=0)


class Order(BaseModel):
    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    items: list[OrderItem] = Field(min_length=1)


class EnqueueResponse(BaseModel):
    status: str
    topic: str
    order_id: str
    message_id: str | None


def check_api_token(authorization: str | None) -> None:
    """Require `Authorization: Bearer <ORDERS_API_TOKEN>` when the token is set."""
    expected = os.getenv("ORDERS_API_TOKEN")
    if not expected:
        return
    scheme, _, provided = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not hmac.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def enqueue_order(
    queue: QueueClient,
    order: Order,
    idempotency_key: str | None = None,
) -> EnqueueResponse:
    """Record an order as queued and publish it to the topic consumed by the daemon."""
    key = idempotency_key or order.order_id
    payload = order.model_dump(mode="json")
    if not await db.insert_order(order.order_id, order.customer_id, payload["items"]):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Order {order.order_id!r} already exists",
        )
    try:
        message_id = await queue.send(
            QUEUE_TOPIC,
            payload,
            idempotency_key=key,
        )
    except DuplicateIdempotencyKeyError as exc:
        await db.delete_order(order.order_id)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Order with idempotency key {key!r} was already enqueued",
        ) from exc
    except QueueError as exc:
        logger.exception("Failed to enqueue order_id=%s", order.order_id)
        await db.delete_order(order.order_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to enqueue order",
        ) from exc

    await db.set_message_id(order.order_id, str(message_id) if message_id else None)
    logger.info(
        "Enqueued order_id=%s message_id=%s topic=%s",
        order.order_id,
        message_id,
        QUEUE_TOPIC,
    )
    return EnqueueResponse(
        status="queued",
        topic=QUEUE_TOPIC,
        order_id=order.order_id,
        message_id=str(message_id) if message_id is not None else None,
    )
