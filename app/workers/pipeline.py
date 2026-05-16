"""End-to-end processing for a single event_id.

Lifted out of the arq task so that the same code path is exercised by:
  - the live worker (arq)
  - the replay CLI
  - tests (no queue, no FastAPI)

The function re-reads the raw payload from `raw_events`. The queue message is
just a pointer; the source of truth is the append-only log.
"""

from __future__ import annotations

import time
from typing import Any

import asyncpg
import orjson

from app.adapters.base import AdapterResult
from app.adapters.llm_universal import LLMUniversalAdapter
from app.adapters.registry import AdapterRegistry
from app.adapters.registry import registry as default_registry
from app.domain.canonical import CanonicalEvent
from app.domain.state_machine import apply_event
from app.llm.fallback import LLMFallback
from app.llm.provider import build_default_provider
from app.logging import get_logger, set_trace_id
from app.metrics import WORKER_LATENCY, WORKER_PROCESS_TOTAL

log = get_logger("worker.pipeline")


_fallback_singleton: LLMFallback | None = None
_universal_singleton: LLMUniversalAdapter | None = None


def get_fallback(pool: asyncpg.Pool) -> LLMFallback:
    global _fallback_singleton
    if _fallback_singleton is None:
        _fallback_singleton = LLMFallback(provider=build_default_provider(), pool=pool)
    return _fallback_singleton


def get_universal_adapter(pool: asyncpg.Pool) -> LLMUniversalAdapter:
    global _universal_singleton
    if _universal_singleton is None:
        _universal_singleton = LLMUniversalAdapter(get_fallback(pool))
    return _universal_singleton


def reset_pipeline_singletons() -> None:
    """For tests: drop cached LLM provider/orchestrator."""
    global _fallback_singleton, _universal_singleton
    _fallback_singleton = None
    _universal_singleton = None


async def process_event(
    pool: asyncpg.Pool,
    event_id: str,
    *,
    trace_id: str | None = None,
    registry: AdapterRegistry = default_registry,
) -> str:
    """Process a single event by id. Returns the apply_event outcome."""

    if trace_id:
        set_trace_id(trace_id)
    started = time.perf_counter()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT vendor_id, payload, headers FROM raw_events WHERE event_id = $1",
            event_id,
        )

    if row is None:
        log.warning("event_not_found", event_id=event_id)
        WORKER_PROCESS_TOTAL.labels(vendor="unknown", classification="missing", outcome="not_found").inc()
        return "not_found"

    payload: dict[str, Any] = row["payload"]
    headers: dict[str, Any] = row["headers"] or {}

    adapter = registry.resolve(payload, headers)
    vendor_for_metric = adapter.vendor_id if adapter else (row["vendor_id"] or "unknown")

    canonical = None
    detail: dict[str, Any] = {}
    if adapter is not None:
        result: AdapterResult = adapter.normalize(payload, headers, event_id)
        if result.status == "ok" and result.canonical_event is not None:
            canonical = result.canonical_event
            detail = {"adapter": adapter.vendor_id, "source": "deterministic"}
        else:
            log.info(
                "adapter_falls_through_to_llm",
                event_id=event_id,
                adapter=adapter.vendor_id,
                status=result.status,
                missing=result.missing_fields,
            )

    if canonical is None:
        universal = get_universal_adapter(pool)
        vendor_hint = adapter.vendor_id if adapter else None
        result = await universal.normalize(payload, headers, event_id, vendor_hint=vendor_hint)
        if result.status == "ok" and result.canonical_event is not None:
            canonical = result.canonical_event
            detail = {"adapter": "llm_universal", **result.detail}
        elif result.status in {"deferred", "needs_llm"}:
            reason = result.detail.get("reason", "llm_deferred")
            await _mark_pending_llm(pool, event_id, reason=reason, detail=result.detail)
            WORKER_PROCESS_TOTAL.labels(
                vendor=vendor_for_metric, classification="unknown", outcome="pending_llm"
            ).inc()
            WORKER_LATENCY.observe(time.perf_counter() - started)
            return "pending_llm"
        else:
            await _mark_review(
                pool, event_id, reason=result.detail.get("reason", "unsupported"), detail=result.detail
            )
            WORKER_PROCESS_TOTAL.labels(
                vendor=vendor_for_metric, classification="unknown", outcome="review"
            ).inc()
            WORKER_LATENCY.observe(time.perf_counter() - started)
            return "requires_review"

    classification = canonical.classification.value
    vendor_for_metric = canonical.vendor_id

    async with pool.acquire() as conn, conn.transaction():
        await _upsert_canonical_event(conn, canonical)
        outcome = await apply_event(conn, canonical)
        await conn.execute(
            """
            UPDATE raw_events
               SET vendor_id = COALESCE(vendor_id, $2),
                   processing_status = 'processed',
                   processing_error = NULL,
                   processed_at = now()
             WHERE event_id = $1
            """,
            event_id,
            canonical.vendor_id,
        )

    log.info(
        "event_processed",
        event_id=event_id,
        vendor_id=canonical.vendor_id,
        classification=classification,
        outcome=outcome,
        **detail,
    )
    WORKER_PROCESS_TOTAL.labels(
        vendor=vendor_for_metric, classification=classification, outcome=outcome
    ).inc()
    WORKER_LATENCY.observe(time.perf_counter() - started)
    return outcome


async def _upsert_canonical_event(conn: asyncpg.Connection, canonical: CanonicalEvent) -> None:
    canonical_payload = orjson.loads(orjson.dumps(canonical.model_dump(mode="json"), default=str))
    await conn.execute(
        """
        INSERT INTO canonical_events (
            event_id, classification, vendor_id, schema_version, source, confidence, canonical
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        ON CONFLICT (event_id) DO UPDATE
              SET classification = EXCLUDED.classification,
                  vendor_id = EXCLUDED.vendor_id,
                  schema_version = EXCLUDED.schema_version,
                  source = EXCLUDED.source,
                  confidence = EXCLUDED.confidence,
                  canonical = EXCLUDED.canonical,
                  normalized_at = now()
        """,
        canonical.event_id,
        canonical.classification.value,
        canonical.vendor_id,
        canonical.schema_version,
        canonical.source.value,
        canonical.confidence,
        canonical_payload,
    )


async def _mark_review(
    pool: asyncpg.Pool,
    event_id: str,
    *,
    reason: str,
    detail: dict[str, Any] | None,
) -> None:
    safe_detail = orjson.loads(orjson.dumps(detail or {}, default=str))
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO requires_human_review (event_id, reason, detail)
            VALUES ($1, $2, $3::jsonb)
            ON CONFLICT (event_id) DO NOTHING
            """,
            event_id,
            reason,
            safe_detail,
        )
        await conn.execute(
            """
            UPDATE raw_events
               SET processing_status = 'review',
                   processing_error = $2,
                   processed_at = now()
             WHERE event_id = $1
            """,
            event_id,
            reason,
        )


async def _mark_pending_llm(
    pool: asyncpg.Pool,
    event_id: str,
    *,
    reason: str,
    detail: dict[str, Any] | None,
) -> None:
    _ = detail  # reserved for future diagnostics persistence
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE raw_events
               SET processing_status = 'pending_llm',
                   processing_error = $2,
                   processed_at = now()
             WHERE event_id = $1
            """,
            event_id,
            reason,
        )
