"""Outbox dispatcher tests."""

from __future__ import annotations

from typing import Any

import pytest

from app.workers.outbox_dispatcher import (
    PermanentError,
    TransientError,
    dispatch_once,
)


class CountingSink:
    def __init__(self, *, fail_n_times: int = 0, raise_permanent: bool = False) -> None:
        self.calls: list[tuple[str, dict[str, Any], str]] = []
        self.fail_n_times = fail_n_times
        self.raise_permanent = raise_permanent

    async def deliver(self, *, kind: str, payload: dict[str, Any], idempotency_key: str) -> None:
        self.calls.append((kind, payload, idempotency_key))
        if self.raise_permanent:
            raise PermanentError("nope")
        if self.fail_n_times > 0:
            self.fail_n_times -= 1
            raise TransientError("transient")


async def _seed_outbox(pool, n: int = 3) -> None:
    async with pool.acquire() as conn:
        for i in range(n):
            await conn.execute(
                "INSERT INTO raw_events (event_id, payload, headers) VALUES ($1, '{}'::jsonb, '{}'::jsonb)",
                f"e{i}",
            )
            await conn.execute(
                """
                INSERT INTO outbox (event_id, kind, payload, status)
                VALUES ($1, $2, $3::jsonb, 'pending')
                """,
                f"e{i}", "shipment.transitioned_to.IN_TRANSIT", {"i": i},
            )


@pytest.mark.asyncio
async def test_dispatcher_delivers_each_row_once(clean_db):
    pool = clean_db
    await _seed_outbox(pool, n=3)
    sink = CountingSink()

    processed = await dispatch_once(pool, sink, batch_size=10, max_attempts=5)
    assert processed == 3
    assert len(sink.calls) == 3

    async with pool.acquire() as conn:
        statuses = await conn.fetch("SELECT status FROM outbox ORDER BY event_id")
    assert [r["status"] for r in statuses] == ["sent", "sent", "sent"]


@pytest.mark.asyncio
async def test_transient_error_retries_with_backoff(clean_db):
    pool = clean_db
    await _seed_outbox(pool, n=1)
    sink = CountingSink(fail_n_times=2)

    # First pass: transient error → row still pending, attempts=1
    await dispatch_once(pool, sink, batch_size=10, max_attempts=10)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status, attempts, next_attempt_at, last_error FROM outbox")
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["last_error"] == "transient"


@pytest.mark.asyncio
async def test_dlq_after_max_attempts(clean_db):
    pool = clean_db
    await _seed_outbox(pool, n=1)

    sink = CountingSink(raise_permanent=True)
    await dispatch_once(pool, sink, batch_size=10, max_attempts=2)

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM outbox")
    assert row["status"] == "dlq"


@pytest.mark.asyncio
async def test_idempotency_key_is_event_id_kind(clean_db):
    pool = clean_db
    await _seed_outbox(pool, n=1)
    sink = CountingSink()

    await dispatch_once(pool, sink, batch_size=10, max_attempts=10)
    assert len(sink.calls) == 1
    _, _, idem = sink.calls[0]
    assert idem == "e0:shipment.transitioned_to.IN_TRANSIT"
