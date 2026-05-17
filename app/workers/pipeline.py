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
from app.adapters.schema_discoverer import SchemaDiscoverer
from app.adapters.schema_driven import SchemaDrivenAdapter
from app.domain.canonical import CanonicalEvent
from app.domain.normalizer import NormalizationError, normalize
from app.domain.state_machine import apply_event
from app.llm.fallback import LLMFallback
from app.llm.provider import build_default_provider
from app.logging import get_logger, set_trace_id
from app.metrics import WORKER_LATENCY, WORKER_PROCESS_TOTAL

log = get_logger("worker.pipeline")


_fallback_singleton: LLMFallback | None = None
_universal_singleton: LLMUniversalAdapter | None = None
_schema_adapter = SchemaDrivenAdapter()
_schema_discoverer = SchemaDiscoverer()


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
        llm_output = llm_result.detail.get("llm_output")
        detail = {"adapter": "llm_universal", **{k: v for k, v in llm_result.detail.items() if k != "llm_output"}}

        # Schema Discovery: infer and persist schema_doc so future events use Path A
        schema_id = None
        if llm_output and llm_result.detail.get("llm_source") != "llm_cache":
            try:
                schema_id = await _schema_discoverer.discover_and_persist(
                    pool=pool,
                    vendor_id=vendor_id,
                    payload=payload,
                    llm_output=llm_output,
                    event_id=event_id,
                )
                if schema_id:
                    detail["schema_discovered"] = True
                    detail["discovered_schema_id"] = schema_id
            except Exception:
                log.warning("schema_discovery_failed", event_id=event_id, vendor_id=vendor_id, exc_info=True)

        return await _finalize(
            pool=pool,
            event_id=event_id,
            vendor_id=vendor_id,
            canonical=canonical,
            detail=detail,
            started=started,
            schema_id=schema_id,
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
    """Classify a raw_event_type via LLM and persist the mapping permanently.

    Uses a lightweight LLM prompt (~40 tokens in, ~20 out) to map the vendor's
    raw event type string to a canonical (classification, state). The result is
    persisted to vendor_event_type_map so the LLM is never called again for this
    (vendor_id, raw_event_type) combination.
    """
    fallback = get_fallback(pool)
    result = await fallback.classify_event_type(
        vendor_id=vendor_id,
        raw_event_type=raw_event_type,
    )

    if result is None:
        return None

    classification, canonical_state, confidence = result

    await registry.persist_event_type(
        pool,
        vendor_id=vendor_id,
        raw_event_type=raw_event_type,
        classification=classification,
        canonical_state=canonical_state,
        confidence=confidence,
        source="llm",
    )

    return (classification, canonical_state)


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
    """Normalize canonical event and apply to state machine."""

    # --- Vendor identity override ---
    # The pipeline-level vendor_id (from DB row or _infer_vendor_id) is more
    # reliable than what the LLM returns. Override the canonical event's vendor_id
    # to ensure entity identity stability across Path A and Path B extractions.
    if vendor_id and vendor_id != "unknown" and canonical.vendor_id != vendor_id:
        canonical = canonical.model_copy(update={"vendor_id": vendor_id})

    # --- Normalization step: standardize all values for projection ---
    # Timestamps → UTC+00:00, money → integer minor units, confidence → [0,1]
    try:
        canonical = normalize(canonical)
    except NormalizationError as exc:
        log.warning("normalization_failed", event_id=event_id, error=str(exc))
        await _mark_review(pool, event_id, reason="normalization_failed", detail={"error": str(exc)})
        WORKER_PROCESS_TOTAL.labels(
            vendor=vendor_id or "unknown", classification="unknown", outcome="review"
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
