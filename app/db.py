"""Asyncpg connection pool + jsonb codec setup.

We register a JSONB codec that uses orjson for both encode and decode so that
payloads round-trip without needing an extra serialization step in callers.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import orjson

from app.config import get_settings

_pool: asyncpg.Pool | None = None


async def _init_codecs(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: orjson.dumps(v).decode("utf-8"),
        decoder=lambda v: orjson.loads(v),
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=lambda v: orjson.dumps(v).decode("utf-8"),
        decoder=lambda v: orjson.loads(v),
        schema="pg_catalog",
    )


async def init_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool
    settings = get_settings()
    _pool = await asyncpg.create_pool(
        dsn=settings.asyncpg_dsn,
        min_size=settings.db_min_pool_size,
        max_size=settings.db_max_pool_size,
        init=_init_codecs,
        command_timeout=10,
    )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized; call init_pool() first.")
    return _pool


async def healthcheck() -> dict[str, Any]:
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            value = await conn.fetchval("SELECT 1")
        return {"ok": value == 1}
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__}
