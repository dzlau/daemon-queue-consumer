import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator

import uvicorn
from fastapi import FastAPI
from vercel.queue import Message, QueueClient, subscribe

from app import db

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("queue-consumer")

QUEUE_TOPIC = os.getenv("QUEUE_TOPIC", "orders")
QUEUE_CONSUMER_GROUP = os.getenv("QUEUE_CONSUMER_GROUP", "fastapi-consumer")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1"))
# Simulated work time so orders are visibly "processing" on the /orders page.
PROCESSING_DELAY_SECONDS = float(os.getenv("PROCESSING_DELAY_SECONDS", "5"))


@subscribe(topic=QUEUE_TOPIC, consumer_group=QUEUE_CONSUMER_GROUP)
async def process_message(message: Message[dict[str, object]]) -> None:
    """Replace the simulated work below with the application's processing logic."""
    order_id = str(message.payload.get("order_id", ""))
    attempt = message.metadata.delivery_count
    logger.info(
        "Processing message id=%s order_id=%s attempt=%s payload=%s",
        message.message_id,
        order_id,
        attempt,
        message.payload,
    )
    await db.set_status(order_id, "processing", attempts=attempt)
    try:
        await asyncio.sleep(PROCESSING_DELAY_SECONDS)
    except Exception as exc:
        # Leave the message unacknowledged so it is redelivered.
        await db.set_status(order_id, "failed", attempts=attempt, error=str(exc))
        raise
    await db.set_status(order_id, "processed", attempts=attempt)
    logger.info("Processed order_id=%s", order_id)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    region = os.getenv("VERCEL_REGION")
    if not region:
        raise RuntimeError("VERCEL_REGION must be set to the queue's region")

    queue = QueueClient(region=region)
    poller = asyncio.create_task(
        queue.poll_and_handle(
            process_message,
            interval=POLL_INTERVAL_SECONDS,
        ),
        name="vercel-queue-poller",
    )
    app.state.queue_poller = poller
    logger.info(
        "Polling topic=%s consumer_group=%s region=%s",
        QUEUE_TOPIC,
        QUEUE_CONSUMER_GROUP,
        region,
    )

    try:
        yield
    finally:
        poller.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poller


app = FastAPI(title="Vercel Queue Consumer", lifespan=lifespan)


@app.get("/healthz")
async def health() -> dict[str, str]:
    poller: asyncio.Task[None] = app.state.queue_poller
    return {"status": "ok" if not poller.done() else "error"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
