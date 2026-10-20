# FastAPI Vercel Queue Consumer

A long-running FastAPI service that polls a Vercel Queue with the official Python SDK. Successful handlers are acknowledged automatically. If `process_message` raises, the message remains unacknowledged and can be redelivered.

## Configure

Set these environment variables:

- `VERCEL_QUEUE_TOKEN`: Vercel OIDC token used outside Vercel
- `VERCEL_REGION`: queue region; it must match the producer's region
- `QUEUE_TOPIC`: topic to consume, defaults to `orders`
- `QUEUE_CONSUMER_GROUP`: consumer group, defaults to `fastapi-consumer`
- `POLL_INTERVAL_SECONDS`: delay after an empty poll, defaults to `1`

Replace `process_message` in `app/main.py` with the application's processing logic.

## Run with Docker

```bash
docker build -t fastapi-queue-consumer .
docker run --rm --env-file .env -p 8000:8000 fastapi-queue-consumer
```

Check the service at `http://localhost:8000/healthz`.
