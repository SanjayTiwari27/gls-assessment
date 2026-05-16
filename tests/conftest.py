"""Pytest fixtures.

Three test tiers:

  - Pure unit tests: no infra, run unconditionally.
  - DB e2e tests: need a reachable Postgres. ``TEST_DATABASE_URL`` overrides;
    otherwise we use ``DATABASE_URL`` and finally fall back to the
    docker-compose default. Tests that need this fixture are SKIPPED if the
    DB is unreachable so that ``pytest`` exits cleanly on any laptop.
  - Queue e2e tests: need Redis. Same skip semantics.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import orjson
import pytest
import pytest_asyncio

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = PROJECT_ROOT / "app" / "storage" / "migrations"
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures" / "payloads"


def _resolve_db_url() -> str:
    return (
        os.environ.get("TEST_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://gls:gls@localhost:5432/gls"
    )


def _resolve_redis_url() -> str:
    return (
        os.environ.get("TEST_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or "redis://localhost:6379/0"
    )


async def _can_connect(dsn: str) -> bool:
    try:
        conn = await asyncpg.connect(dsn=dsn, timeout=2)
        await conn.close()
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    """Mark DB-using tests as e2e so they can be filtered with -m."""
    for item in items:
        if "e2e" in item.keywords:
            continue
        fixtures = set(getattr(item, "fixturenames", ()) or ())
        if {"db_pool", "clean_db"} & fixtures:
            item.add_marker(pytest.mark.e2e)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def db_pool() -> AsyncIterator[asyncpg.Pool]:
    dsn = _resolve_db_url()
    if not await _can_connect(dsn):
        pytest.skip(f"Postgres not reachable at {dsn}; skipping DB e2e tests")
    # Apply migrations idempotently.
    conn = await asyncpg.connect(dsn=dsn)
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                name TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        already = {r["name"] for r in await conn.fetch("SELECT name FROM schema_migrations")}
        for f in sorted(MIGRATIONS_DIR.glob("*.sql")):
            if f.name in already:
                continue
            sql = f.read_text(encoding="utf-8")
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO schema_migrations (name) VALUES ($1) ON CONFLICT DO NOTHING",
                    f.name,
                )
    finally:
        await conn.close()

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=4,
        init=_init_codecs,
    )
    yield pool
    await pool.close()


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


@pytest_asyncio.fixture(loop_scope="session")
async def clean_db(db_pool):
    """Truncate all business tables before each DB test for isolation."""

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                requires_human_review,
                outbox,
                stale_event_log,
                applied_events,
                shipments,
                invoices,
                entities,
                llm_audit,
                llm_cache,
                raw_events
            CASCADE
            """
        )
    yield db_pool


@pytest.fixture
def fixture_payloads() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for f in sorted(FIXTURES_DIR.glob("*.json")):
        out[f.stem] = orjson.loads(f.read_bytes())
    return out
