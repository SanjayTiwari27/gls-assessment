"""Outbox dispatcher.

Polls `outbox` for pending rows, claims them with `FOR UPDATE SKIP LOCKED`
so that horizontal scale-out is free, and delivers each side-effect to a
downstream sink. Claims are persisted as `status='sending'` before network
I/O so delivery calls do not hold a long-running DB transaction. The sink
here is a logging stub — production would inject a real HTTP/SQS/whatever
client behind the same interface.

End-to-end idempotency rests on:

  - The composite PK `(event_id, kind)` on `outbox`: at most one row per
    decision per side-effect.
  - The `idempotency_key` parameter passed to the downstream sink: receivers
    that respect it dedupe on their side.
"""

from __future__ import annotations

import asyncio
import signal
from typing import Any, Protocol

import asyncpg

from app.config import get_settings
from app.db import close_pool, init_pool
from app.logging import configure_logging, get_logger
from app.metrics import OUTBOX_DISPATCH_TOTAL

log = get_logger("outbox.dispatcher")


class TransientError(Exception):
    """Sink raises this to ask for a retry with backoff."""


class PermanentError(Exception):
    """Sink raises this to send the row straight to DLQ."""


class DownstreamSink(Protocol):
    async def deliver(self, *, kind: str, payload: dict[str, Any], idempotency_key: str) -> None: ...


class LoggingSink:
    """Default sink: writes a structured log line. Honours idempotency_key."""

    async def deliver(self, *, kind: str, payload: dict[str, Any], idempotency_key: str) -> None:
        log.info("outbox_delivered", kind=kind, idempotency_key=idempotency_key, payload=payload)


async def _claim_batch(conn: asyncpg.Connection, *, batch_size: int) -> list[asyncpg.Record]:
    rows = await conn.fetch(
        """
        SELECT event_id, kind, payload, attempts
          FROM outbox
         WHERE
               (status = 'pending' AND next_attempt_at <= now())
            OR (status = 'sending' AND updated_at <= now() - interval '5 minutes')
         ORDER BY created_at ASC
         LIMIT $1
         FOR UPDATE SKIP LOCKED
        """,
        batch_size,
    )
    if not rows:
        return []

    claimed: list[asyncpg.Record] = []
    for row in rows:
        updated = await conn.fetchval(
            """
            UPDATE outbox
               SET status='sending',
                   updated_at=now()
             WHERE event_id=$1 AND kind=$2
               AND status IN ('pending', 'sending')
            RETURNING 1
            """,
            row["event_id"],
            row["kind"],
        )
        if updated is not None:
            claimed.append(row)
    return claimed


async def dispatch_once(
    pool: asyncpg.Pool,
    sink: DownstreamSink,
    *,
    batch_size: int,
    max_attempts: int,
) -> int:
    """Process up to ``batch_size`` outbox rows. Returns the number processed."""

    processed = 0
    async with pool.acquire() as conn, conn.transaction():
        rows = await _claim_batch(conn, batch_size=batch_size)

    for r in rows:
        event_id = r["event_id"]
        kind = r["kind"]
        payload = r["payload"]
        attempts = int(r["attempts"])
        idempotency_key = f"{event_id}:{kind}"

        try:
            await sink.deliver(kind=kind, payload=payload, idempotency_key=idempotency_key)
        except PermanentError as exc:
            OUTBOX_DISPATCH_TOTAL.labels(kind=kind, outcome="dlq").inc()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE outbox
                       SET status='dlq',
                           attempts=attempts+1,
                           last_error=$3,
                           updated_at=now()
                     WHERE event_id=$1 AND kind=$2 AND status='sending'
                    """,
                    event_id,
                    kind,
                    str(exc),
                )
        except TransientError as exc:
            new_attempts = attempts + 1
            async with pool.acquire() as conn:
                if new_attempts >= max_attempts:
                    OUTBOX_DISPATCH_TOTAL.labels(kind=kind, outcome="dlq").inc()
                    await conn.execute(
                        """
                        UPDATE outbox
                           SET status='dlq',
                               attempts=$3,
                               last_error=$4,
                               updated_at=now()
                         WHERE event_id=$1 AND kind=$2 AND status='sending'
                        """,
                        event_id,
                        kind,
                        new_attempts,
                        str(exc),
                    )
                else:
                    OUTBOX_DISPATCH_TOTAL.labels(kind=kind, outcome="retry").inc()
                    backoff_s = min(2**new_attempts, 600)
                    await conn.execute(
                        """
                        UPDATE outbox
                           SET status='pending',
                               attempts=$3,
                               next_attempt_at=now() + ($4 * interval '1 second'),
                               last_error=$5,
                               updated_at=now()
                         WHERE event_id=$1 AND kind=$2 AND status='sending'
                        """,
                        event_id,
                        kind,
                        new_attempts,
                        backoff_s,
                        str(exc),
                    )
        else:
            OUTBOX_DISPATCH_TOTAL.labels(kind=kind, outcome="delivered").inc()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE outbox
                       SET status='sent',
                           attempts=attempts+1,
                           last_error=NULL,
                           updated_at=now()
                     WHERE event_id=$1 AND kind=$2 AND status='sending'
                    """,
                    event_id,
                    kind,
                )
        processed += 1

    return processed


async def run(sink: DownstreamSink | None = None) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    pool = await init_pool()
    sink = sink or LoggingSink()

    stop = asyncio.Event()

    def _stop(*_: Any) -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    log.info("outbox_dispatcher_started")
    try:
        while not stop.is_set():
            try:
                processed = await dispatch_once(
                    pool,
                    sink,
                    batch_size=settings.outbox_batch_size,
                    max_attempts=settings.outbox_max_attempts,
                )
            except Exception as exc:  # noqa: BLE001
                log.exception("outbox_dispatch_loop_error", error=str(exc))
                processed = 0

            if processed == 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval_s)
                except TimeoutError:
                    pass
    finally:
        await close_pool()
        log.info("outbox_dispatcher_stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
