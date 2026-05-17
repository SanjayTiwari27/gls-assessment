"""End-to-end processing for a single event_id.

Two-path architecture:
  Path A — known vendor+shape: DB schema lookup → deterministic extraction → state machine.
  Path B — unknown shape: LLM fallback for discovery (learns once, then Path A forever).

The function re-reads the raw payload from `raw_events`. The queue message is
just a pointer; the source of truth is the append-only log.
"""

from __future__ import annotations

import time
from typing import Any

import asyncpg
import orjson

from app.adapters.llm_universal import LLMUniversalAdapter
from app.adapters.registry import SchemaRegistry
from app.adapters.registry import registry as default_registry
from app.adapters.schema_driven import SchemaDrivenAdapter
from app.domain.canonical import CanonicalEvent
from app.domain.state_machine import apply_event
from app.llm.fallback import LLMFallback
from app.llm.provider import build_default_provider
from app.logging import get_logger, set_trace_id
from app.metrics import WORKER_LATENCY, WORKER_PROCESS_TOTAL

log = get_logger("worker.pipeline")


_fallback_singleton: LLMFallback | None = None
_universal_singleton: LLMUniversalAdapter | None = None
_schema_adapter = SchemaDrivenAdapter()


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
    registry: SchemaRegistry = default_registry,
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
    vendor_id = row["vendor_id"] or _infer_vendor_id(payload, headers)

    # ----- Path A: known vendor + known shape ----- #
    schema_match = await registry.lookup(pool, vendor_id=vendor_id, payload=payload)

    if schema_match is not None:
        result = await _process_via_schema(
            pool=pool,
            event_id=event_id,
            vendor_id=vendor_id,
            payload=payload,
            headers=headers,
            schema_match=schema_match,
            registry=registry,
        )
        if result is not None:
            canonical, detail = result
            return await _finalize(
                pool=pool,
                event_id=event_id,
                vendor_id=vendor_id,
                canonical=canonical,
                detail=detail,
                started=started,
                schema_id=schema_match.schema_id,
            )
        # Schema extraction failed — fall through to Path B
        await registry.increment_failure(pool, schema_match.schema_id)

    # ----- Path B: unknown shape — LLM fallback ----- #
    vendor_for_metric = vendor_id or "unknown"

    universal = get_universal_adapter(pool)
    llm_result = await universal.normalize(payload, headers, event_id, vendor_hint=vendor_id)

    if llm_result.status == "ok" and llm_result.canonical_event is not None:
        canonical = llm_result.canonical_event
        detail = {"adapter": "llm_universal", **llm_result.detail}
        return await _finalize(
            pool=pool,
            event_id=event_id,
            vendor_id=vendor_id,
            canonical=canonical,
            detail=detail,
            started=started,
            schema_id=None,
        )

    if llm_result.status == "deferred":
        reason = llm_result.detail.get("reason", "llm_deferred")
        await _mark_pending_llm(pool, event_id, reason=reason, detail=llm_result.detail)
        WORKER_PROCESS_TOTAL.labels(
            vendor=vendor_for_metric, classification="unknown", outcome="pending_llm"
        ).inc()
        WORKER_LATENCY.observe(time.perf_counter() - started)
        return "pending_llm"

    # Unsupported / failed
    await _mark_review(
        pool, event_id, reason=llm_result.detail.get("reason", "unsupported"), detail=llm_result.detail
    )
    WORKER_PROCESS_TOTAL.labels(
        vendor=vendor_for_metric, classification="unknown", outcome="review"
    ).inc()
    WORKER_LATENCY.observe(time.perf_counter() - started)
    return "requires_review"


async def _process_via_schema(
    *,
    pool: asyncpg.Pool,
    event_id: str,
    vendor_id: str,
    payload: dict[str, Any],
    headers: dict[str, Any],
    schema_match: Any,
    registry: SchemaRegistry,
) -> tuple[CanonicalEvent, dict[str, Any]] | None:
    """Try Path A extraction. Returns (canonical, detail) or None on failure."""

    schema_doc = schema_match.schema_doc

    # First pass: extract raw_event_type and try to resolve canonical_state
    extraction = _schema_adapter.extract(
        payload=payload,
        headers=headers,
        event_id=event_id,
        vendor_id=vendor_id,
        schema_doc=schema_doc,
        canonical_state=None,  # first pass to get raw_event_type
    )

    raw_event_type = extraction.raw_event_type
    classification = schema_doc.get("classification")

    # For unclassified, no state resolution needed
    if classification == "unclassified" and extraction.success:
        await registry.increment_success(pool, schema_match.schema_id)
        return extraction.canonical_event, {
            "adapter": "schema_driven",
            "schema_id": schema_match.schema_id,
            "source": "deterministic",
        }

    if not raw_event_type:
        log.info("schema_no_raw_event_type", event_id=event_id, schema_id=schema_match.schema_id)
        return None

    # Resolve canonical_state from event_type_map
    mapping = await registry.lookup_event_type(pool, vendor_id=vendor_id, raw_event_type=raw_event_type)

    if mapping is None:
        # Event type not yet mapped — classify via LLM and persist
        mapping = await _classify_and_persist(
            pool=pool,
            vendor_id=vendor_id,
            raw_event_type=raw_event_type,
            registry=registry,
        )

    if mapping is None:
        log.info("event_type_unresolvable", event_id=event_id, raw_event_type=raw_event_type)
        return None

    resolved_classification, canonical_state = mapping

    # Second pass: full extraction with resolved state
    extraction = _schema_adapter.extract(
        payload=payload,
        headers=headers,
        event_id=event_id,
        vendor_id=vendor_id,
        schema_doc=schema_doc,
        canonical_state=canonical_state,
        classification=resolved_classification,
    )

    if not extraction.success:
        log.info(
            "schema_extraction_failed",
            event_id=event_id,
            schema_id=schema_match.schema_id,
            error=extraction.error,
        )
        return None

    await registry.increment_success(pool, schema_match.schema_id)
    return extraction.canonical_event, {
        "adapter": "schema_driven",
        "schema_id": schema_match.schema_id,
        "source": "deterministic",
    }


async def _classify_and_persist(
    *,
    pool: asyncpg.Pool,
    vendor_id: str,
    raw_event_type: str,
    registry: SchemaRegistry,
) -> tuple[str, str] | None:
    """Use the LLM stub/provider to classify a raw_event_type and persist the mapping.

    This is the "Prompt B" equivalent — a lightweight classification call.
    For now, uses the existing LLM fallback's classification capability via a
    keyword-based heuristic (matching what the stub does).
    """
    from app.adapters.parsers import _TZ_ALIASES  # noqa: F401 — import check

    # Keyword-based classification matching the stub's logic
    classification, canonical_state = _heuristic_classify(raw_event_type)

    if canonical_state is None:
        return None

    await registry.persist_event_type(
        pool,
        vendor_id=vendor_id,
        raw_event_type=raw_event_type,
        classification=classification,
        canonical_state=canonical_state,
        confidence=0.85,
        source="heuristic",
    )

    return (classification, canonical_state)


_SHIPMENT_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    (
        "shipment.delivered",
        ("released to consignee", "delivered", "package handed", "empty container returned", "delivery completed"),
    ),
    ("shipment.out_for_delivery", ("out for delivery", "delivery scheduled", "on hand for delivery")),
    (
        "shipment.in_transit",
        ("loaded onboard", "sailed", "vessel arrived", "discharged", "transhipment", "in transit", "vessel departed"),
    ),
    (
        "shipment.picked_up",
        ("empty container released to shipper", "received at origin", "container received", "gate-in", "gate in", "picked up"),
    ),
    ("shipment.exception", ("exception", "delay", "rolled", "blocked", "hold")),
    ("shipment.cancelled", ("cancelled", "canceled", "voided shipment")),
]

_INVOICE_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("invoice.refunded", ("refund", "reversed", "reversal", "credit note")),
    ("invoice.voided", ("voided", "cancelled", "canceled", "annulled")),
    (
        "invoice.paid",
        ("settled in full", "settled", "paid in full", "payment received", "remitted", "payment confirmed"),
    ),
    (
        "invoice.issued",
        ("freight invoice raised", "invoice raised", "invoice issued", "issued", "billed", "raised"),
    ),
]


def _heuristic_classify(raw_event_type: str) -> tuple[str, str | None]:
    """Keyword-based classification of a raw event type string."""
    haystack = raw_event_type.lower()

    for state, keywords in _SHIPMENT_KEYWORDS:
        if any(kw in haystack for kw in keywords):
            return ("shipment", state)

    for state, keywords in _INVOICE_KEYWORDS:
        if any(kw in haystack for kw in keywords):
            return ("invoice", state)

    return ("unclassified", None)


def _infer_vendor_id(payload: dict[str, Any], headers: dict[str, Any]) -> str:
    """Content-based vendor inference when no explicit vendor_id is set."""
    if str(payload.get("carrier_scac", "")).upper() == "MAEU":
        return "maersk"
    if str(payload.get("carrier_scac", "")).upper() == "ONEY":
        return "ocean_network_express"
    carrier = str(payload.get("carrier", "")).lower()
    if "ocean network express" in carrier:
        return "ocean_network_express"
    if str(payload.get("source", "")).lower() == "globalfreightpay.api":
        return "globalfreightpay"
    if str(payload.get("issuer", "")).lower() == "marine-traffic-advisory":
        return "marine_traffic_advisory"
    return "unknown"


async def _finalize(
    *,
    pool: asyncpg.Pool,
    event_id: str,
    vendor_id: str,
    canonical: CanonicalEvent,
    detail: dict[str, Any],
    started: float,
    schema_id: int | None,
) -> str:
    """Apply canonical event to state machine and update raw_events status."""
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
    async with pool.acquire() as conn, conn.transaction():
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
    _ = detail
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
