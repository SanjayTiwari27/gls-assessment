"""Webhook receiver — the hot path.

This handler runs synchronously in the request lifecycle and must complete in
well under a second. It does only what's necessary to durably accept the
delivery:

  1. parse JSON
  2. compute the content-addressed event_id
  3. INSERT raw_events ON CONFLICT DO NOTHING (idempotency)
  4. enqueue webhook.process job (only if the row was actually inserted)
  5. return 202

Forbidden here: vendor detection, classification, normalization, LLM, business
joins, multi-statement business writes, retries to other services. All that
moves to the worker.
"""

from __future__ import annotations

import time
from typing import Any

import orjson
from fastapi import APIRouter, Header, HTTPException, Request

from app.config import get_settings
from app.db import get_pool
from app.hashing import compute_event_id
from app.logging import get_logger, new_trace_id, set_trace_id
from app.metrics import INGEST_LATENCY, INGEST_TOTAL
from app.queue import enqueue_process

router = APIRouter(prefix="/webhooks", tags=["ingestion"])
log = get_logger("receiver")


@router.post("", status_code=202)
async def receive_any(
    request: Request,
    x_event_id: str | None = Header(default=None, alias="X-Event-Id"),
    x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
) -> dict[str, Any]:
    """Accept any JSON payload. The vendor is intentionally not part of the URL —
    the assessment spec says "any arbitrary JSON". Vendor is detected in the
    worker by the adapter registry."""

    return await _ingest(request, vendor_hint=None, x_event_id=x_event_id, x_request_id=x_request_id)


@router.post("/{vendor_id}", status_code=202)
async def receive_vendor(
    vendor_id: str,
    request: Request,
    x_event_id: str | None = Header(default=None, alias="X-Event-Id"),
    x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
) -> dict[str, Any]:
    """Optional vendor-scoped path. Production deployments would prefer this
    so that signature verification can happen here per vendor secret."""

    return await _ingest(request, vendor_hint=vendor_id, x_event_id=x_event_id, x_request_id=x_request_id)


async def _ingest(
    request: Request,
    *,
    vendor_hint: str | None,
    x_event_id: str | None,
    x_request_id: str | None,
) -> dict[str, Any]:
    settings = get_settings()
    started = time.perf_counter()

    trace_id = x_request_id or new_trace_id()
    set_trace_id(trace_id)

    raw: bytes = await request.body()
    if not raw:
        INGEST_TOTAL.labels(outcome="bad_request").inc()
        raise HTTPException(status_code=400, detail="empty body")
    if len(raw) > settings.receiver_max_payload_bytes:
        INGEST_TOTAL.labels(outcome="bad_request").inc()
        raise HTTPException(status_code=413, detail="payload too large")

    try:
        payload: Any = orjson.loads(raw)
    except orjson.JSONDecodeError:
        INGEST_TOTAL.labels(outcome="bad_request").inc()
        raise HTTPException(status_code=400, detail="invalid json") from None

    if not isinstance(payload, dict):
        INGEST_TOTAL.labels(outcome="bad_request").inc()
        raise HTTPException(status_code=400, detail="json object required")

    vendor_event_id = (
        x_event_id
        or _safe_str(payload.get("event_msg_id"))
        or _safe_str(payload.get("event_id"))
        or _safe_str(payload.get("doc_ref"))
        or _safe_str(payload.get("advisory_id"))
    )

    event_id = compute_event_id(payload, vendor_event_id)

    headers = {
        k.lower(): v
        for k, v in request.headers.items()
        if k.lower() in {"content-type", "user-agent", "x-event-id", "x-request-id", "x-forwarded-for"}
    }

    pool = get_pool()
    async with pool.acquire() as conn:
        inserted = await conn.fetchval(
            """
            INSERT INTO raw_events (event_id, vendor_id, payload, headers, received_at, processing_status)
            VALUES ($1, $2, $3::jsonb, $4::jsonb, now(), 'queued')
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            event_id,
            vendor_hint,
            payload,
            headers,
        )

    deduplicated = inserted is None
    if not deduplicated:
        await enqueue_process(event_id, trace_id=trace_id)
        INGEST_TOTAL.labels(outcome="accepted").inc()
        log.info("ingest_accepted", event_id=event_id, vendor_hint=vendor_hint)
    else:
        INGEST_TOTAL.labels(outcome="duplicated").inc()
        log.info("ingest_duplicate", event_id=event_id, vendor_hint=vendor_hint)

    INGEST_LATENCY.observe(time.perf_counter() - started)

    return {
        "event_id": event_id,
        "deduplicated": deduplicated,
        "trace_id": trace_id,
    }


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (str, int)):
        return str(value)
    return None
