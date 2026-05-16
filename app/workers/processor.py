"""arq worker — runs `process_webhook(event_id, trace_id)` jobs.

Run with: `python -m arq app.workers.processor.WorkerSettings`.

The arq queue is configured for at-least-once delivery with retry + DLQ. The
*correctness* guarantee (no double application of the same event) lives in
``apply_event`` via the `applied_events (entity_id, event_id)` unique
constraint — the queue's role is throughput and back-pressure.
"""

from __future__ import annotations

from typing import Any

from arq.connections import RedisSettings

from app.config import get_settings
from app.db import close_pool, init_pool
from app.logging import configure_logging, get_logger, new_trace_id, set_trace_id
from app.queue import redis_settings_from_url
from app.workers.pipeline import process_event, reset_pipeline_singletons

log = get_logger("worker")


async def process_webhook(ctx: dict[str, Any], event_id: str, trace_id: str | None = None) -> str:
    pool = ctx["pool"]
    if not trace_id:
        trace_id = new_trace_id()
    set_trace_id(trace_id)
    log.info("worker_job_start", event_id=event_id)
    outcome = await process_event(pool, event_id, trace_id=trace_id)
    log.info("worker_job_done", event_id=event_id, outcome=outcome)
    return outcome


async def startup(ctx: dict[str, Any]) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    reset_pipeline_singletons()
    ctx["pool"] = await init_pool()
    log.info("worker_startup", llm_provider=settings.llm_provider)


async def shutdown(ctx: dict[str, Any]) -> None:
    await close_pool()
    reset_pipeline_singletons()
    log.info("worker_shutdown")


def _build_redis_settings() -> RedisSettings:
    settings = get_settings()
    return redis_settings_from_url(settings.redis_url)


class WorkerSettings:
    """arq worker config. Picked up by `python -m arq app.workers.processor.WorkerSettings`."""

    functions = [process_webhook]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = _build_redis_settings()
    max_jobs = 16
    job_timeout = 60
    keep_result = 30
    queue_name = "arq:queue"
    max_tries = get_settings().worker_max_retries
