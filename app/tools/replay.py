"""Replay CLI.

Selects a slice of `raw_events` (by vendor / time range / entity) and runs the
normal worker pipeline against each. There is no special "replay mode" in the
worker — that's deliberate: a pipeline change must work for live and replay
traffic identically, or the design is broken.

Two execution modes:

  - ``--mode enqueue`` (default): push event_ids onto the arq queue. The
    running worker picks them up. Best in production (back-pressure-aware).
  - ``--mode inline``: process each event_id in-process. Best for tests and
    local rebuilds; no queue or worker needed.

Optional ``--truncate-projections`` wipes shipments/invoices/applied_events
*before* replaying — useful for full-rebuild integration tests.
"""

from __future__ import annotations

import asyncio
from datetime import datetime

import typer

from app.config import get_settings
from app.db import close_pool, init_pool
from app.logging import configure_logging, get_logger
from app.queue import close_queue, enqueue_process, init_queue
from app.workers.pipeline import process_event, reset_pipeline_singletons

cli = typer.Typer(add_completion=False, help="Replay events from raw_events through the worker pipeline.")
log = get_logger("replay")


@cli.command()
def replay(
    vendor: str | None = typer.Option(None, "--vendor", help="Filter by vendor_id."),
    since: datetime | None = typer.Option(None, "--since", help="Replay events received_at >= since."),
    until: datetime | None = typer.Option(None, "--until", help="Replay events received_at < until."),
    external_id: str | None = typer.Option(None, "--external-id", help="Filter by an entity external id."),
    truncate_projections: bool = typer.Option(
        False,
        "--truncate-projections",
        help="Wipe shipments/invoices/applied_events/etc before replay. Use only for full rebuilds.",
    ),
    mode: str = typer.Option("enqueue", "--mode", help="enqueue (push to queue) or inline (process here)."),
    limit: int | None = typer.Option(None, "--limit", help="Replay at most N events."),
) -> None:
    """Replay raw_events through the canonical processing pipeline."""

    asyncio.run(_run(vendor, since, until, external_id, truncate_projections, mode, limit))


async def _run(
    vendor: str | None,
    since: datetime | None,
    until: datetime | None,
    external_id: str | None,
    truncate_projections: bool,
    mode: str,
    limit: int | None,
) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    pool = await init_pool()
    if mode == "enqueue":
        await init_queue()
    reset_pipeline_singletons()

    try:
        if truncate_projections:
            await truncate(pool)

        async with pool.acquire() as conn:
            sql = """
                SELECT event_id FROM raw_events
                WHERE ($1::text IS NULL OR vendor_id = $1)
                  AND ($2::timestamptz IS NULL OR received_at >= $2)
                  AND ($3::timestamptz IS NULL OR received_at <  $3)
                  AND (
                        $4::text IS NULL
                        OR payload->'transport_doc'->>'number' = $4
                        OR payload->>'container'      = $4
                        OR payload->>'house_bl'       = $4
                        OR payload->>'master_bl'      = $4
                        OR payload->>'doc_ref'        = $4
                        OR payload->>'tracking_number'= $4
                      )
                ORDER BY received_at ASC, event_id ASC
            """
            if limit is not None:
                sql += f" LIMIT {int(limit)}"
            rows = await conn.fetch(sql, vendor, since, until, external_id)

        log.info("replay_planned", count=len(rows), mode=mode, vendor=vendor)

        for r in rows:
            event_id = r["event_id"]
            if mode == "inline":
                outcome = await process_event(pool, event_id)
                log.info("replay_inline", event_id=event_id, outcome=outcome)
            else:
                await enqueue_process(event_id)
                log.info("replay_enqueued", event_id=event_id)

    finally:
        if mode == "enqueue":
            await close_queue()
        await close_pool()


async def truncate(pool) -> None:
    """Wipe projections so a replay rebuilds them from raw_events.

    Order matters: applied_events references entities, shipments/invoices
    reference entities; we truncate them in dependency order.
    """
    log.warning("truncating_projections")
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            """
                TRUNCATE TABLE
                    requires_human_review,
                    outbox,
                    stale_event_log,
                    applied_events,
                    shipments,
                    invoices,
                    entities
                CASCADE
                """
        )
        await conn.execute(
            """
                UPDATE raw_events
                   SET processing_status='queued',
                       processing_error=NULL,
                       processed_at=NULL
                """
        )


def main() -> None:
    cli()


if __name__ == "__main__":
    cli()
