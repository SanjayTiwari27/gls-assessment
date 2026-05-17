"""Redis/arq queue helpers.

The receiver enqueues `webhook.process` jobs carrying only the `event_id` —
workers always re-read the immutable payload from `raw_events`.
"""

from __future__ import annotations

from typing import Any

from arq import create_pool
from arq.connections import ArqRedis, RedisSettings

from app.config import get_settings

_redis: ArqRedis | None = None


def redis_settings_from_url(url: str) -> RedisSettings:
    """Translate a redis:// URL into arq's RedisSettings."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    db = 0
    if parsed.path and parsed.path.strip("/").isdigit():
        db = int(parsed.path.strip("/"))
    return RedisSettings(
        host=parsed.hostname or "localhost",
        port=parsed.port or 6379,
        database=db,
        password=parsed.password or None,
    )


async def init_queue() -> ArqRedis:
    global _redis
    if _redis is not None:
        return _redis
    settings = get_settings()
    _redis = await create_pool(redis_settings_from_url(settings.redis_url))
    return _redis


async def close_queue() -> None:
    global _redis
    if _redis is not None:
        await _redis.close(close_connection_pool=True)
        _redis = None


def get_queue() -> ArqRedis:
    if _redis is None:
        raise RuntimeError("Queue not initialized; call init_queue() first.")
    return _redis


async def enqueue_process(event_id: str, *, trace_id: str | None = None) -> None:
    """Enqueue a webhook.process job. Job id is the event_id, so the queue itself
    is also idempotent — duplicate enqueues for the same event coalesce."""
    queue = get_queue()
    await queue.enqueue_job(
        "process_webhook",
        event_id,
        trace_id,
        _job_id=f"webhook.process:{event_id}",
    )


async def healthcheck() -> dict[str, Any]:
    try:
        queue = get_queue()
        pong = await queue.ping()
        return {"ok": bool(pong)}
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__}
