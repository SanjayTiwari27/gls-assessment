"""FastAPI app entrypoint."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.receiver import router as receiver_router
from app.config import get_settings
from app.db import close_pool, init_pool
from app.db import healthcheck as db_healthcheck
from app.logging import configure_logging, get_logger
from app.queue import close_queue, init_queue
from app.queue import healthcheck as queue_healthcheck


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger("main")
    log.info("startup_begin", llm_provider=settings.llm_provider)
    await init_pool()
    await init_queue()
    log.info("startup_complete")
    try:
        yield
    finally:
        log.info("shutdown_begin")
        await close_queue()
        await close_pool()
        log.info("shutdown_complete")


app = FastAPI(
    title="GLS Webhook Ingestion",
    version="0.1.0",
    description="AI-powered webhook ingestion and normalization for vendor logistics/invoice events.",
    lifespan=lifespan,
)

app.include_router(receiver_router)


@app.get("/healthz", tags=["health"])
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz", tags=["health"])
async def readyz() -> dict[str, object]:
    db = await db_healthcheck()
    queue = await queue_healthcheck()
    ready = bool(db.get("ok") and queue.get("ok"))
    return {"ready": ready, "db": db, "queue": queue}


@app.get("/metrics", tags=["health"], include_in_schema=False)
async def metrics() -> PlainTextResponse:
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
