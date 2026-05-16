"""LLM fallback orchestration tests.

We use a controllable mock provider that returns whatever we tell it. The
tests assert the orchestration contract:

  - Cache hit short-circuits the call AND records audit row.
  - Cache miss: call provider, cache result, record audit row.
  - Invalid JSON: retry once with the validation error appended; succeed.
  - Always-invalid: raise LLMValidationError; no llm_cache row written.
  - Identical payload twice: provider invoked exactly once.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from app.llm.fallback import LLMFallback, LLMValidationError, load_target_schema
from app.llm.provider import LLMResult

pytestmark = pytest.mark.e2e


class MockLLM:
    name = "mock"
    model = "mock-1"

    def __init__(self, responses: list[str | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def complete(self, *, prompt: str, schema: dict, temperature: float = 0.0) -> LLMResult:
        self.calls.append({"prompt": prompt, "temperature": temperature})
        if not self.responses:
            raise RuntimeError("MockLLM out of canned responses")
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return LLMResult(
            text=nxt,
            model=self.model,
            tokens_in=10,
            tokens_out=10,
            latency_ms=1,
            cost_estimate=0.0001,
        )


VALID_SHIPMENT_OUTPUT = json.dumps({
    "classification": "shipment",
    "vendor_id": "unknown_vendor",
    "confidence": 0.7,
    "entity_external_id": "BL123:CONT1",
    "event_type": "shipment.in_transit",
    "event_timestamp": "2026-04-21T22:47:00+00:00",
    "amount": None,
    "due_at": None,
    "reference_ids": {"bl": "BL123"},
    "linked_references": None,
    "location": None,
    "raw_milestone": "in transit",
    "raw_kind": None,
    "summary": None,
    "reason": None,
})


INVALID_OUTPUT = '{"classification": "shipment", "missing_required_fields": true}'


@pytest.mark.asyncio
async def test_first_call_caches_and_audits(clean_db):
    pool = clean_db
    mock = MockLLM([VALID_SHIPMENT_OUTPUT])
    fallback = LLMFallback(provider=mock, pool=pool)
    payload = {"random": "shape", "n": 1}

    # raw_events row needed for the FK on llm_audit
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO raw_events (event_id, payload) VALUES ($1, $2::jsonb)",
            "ev-1", payload,
        )

    outcome = await fallback.classify_extract(event_id="ev-1", vendor_hint="unknown", payload=payload)
    assert outcome.source == "llm"
    assert outcome.data["classification"] == "shipment"
    assert len(mock.calls) == 1

    async with pool.acquire() as conn:
        cache_count = await conn.fetchval("SELECT count(*) FROM llm_cache")
        audit_count = await conn.fetchval("SELECT count(*) FROM llm_audit WHERE event_id=$1", "ev-1")
    assert cache_count == 1
    assert audit_count == 1


@pytest.mark.asyncio
async def test_second_identical_call_hits_cache(clean_db):
    pool = clean_db
    mock = MockLLM([VALID_SHIPMENT_OUTPUT])
    fallback = LLMFallback(provider=mock, pool=pool)
    payload = {"random": "shape", "n": 2}

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO raw_events (event_id, payload) VALUES ($1, $2::jsonb), ($3, $2::jsonb)",
            "ev-a", payload, "ev-b",
        )

    a = await fallback.classify_extract(event_id="ev-a", vendor_hint="unknown", payload=payload)
    b = await fallback.classify_extract(event_id="ev-b", vendor_hint="unknown", payload=payload)

    assert a.source == "llm"
    assert b.source == "llm_cache"
    assert len(mock.calls) == 1   # provider hit only once

    async with pool.acquire() as conn:
        audits = await conn.fetch("SELECT event_id, source FROM llm_audit ORDER BY event_id")
    by_event = {r["event_id"]: r["source"] for r in audits}
    assert by_event == {"ev-a": "llm", "ev-b": "llm_cache"}


@pytest.mark.asyncio
async def test_invalid_then_valid_one_retry(clean_db):
    pool = clean_db
    mock = MockLLM([INVALID_OUTPUT, VALID_SHIPMENT_OUTPUT])
    fallback = LLMFallback(provider=mock, pool=pool)
    payload = {"k": 1}

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO raw_events (event_id, payload) VALUES ($1, $2::jsonb)",
            "ev-3", payload,
        )

    outcome = await fallback.classify_extract(event_id="ev-3", vendor_hint="unknown", payload=payload)
    assert outcome.data["classification"] == "shipment"
    assert len(mock.calls) == 2
    # Retry prompt should mention the validation error.
    assert "rejected by the schema" in mock.calls[1]["prompt"]


@pytest.mark.asyncio
async def test_always_invalid_raises_and_writes_no_cache(clean_db):
    pool = clean_db
    mock = MockLLM([INVALID_OUTPUT, INVALID_OUTPUT])
    fallback = LLMFallback(provider=mock, pool=pool)
    payload = {"k": 2}

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO raw_events (event_id, payload) VALUES ($1, $2::jsonb)",
            "ev-4", payload,
        )

    with pytest.raises(LLMValidationError):
        await fallback.classify_extract(event_id="ev-4", vendor_hint="unknown", payload=payload)

    assert len(mock.calls) == 2

    async with pool.acquire() as conn:
        cache_count = await conn.fetchval("SELECT count(*) FROM llm_cache")
        audit_count = await conn.fetchval("SELECT count(*) FROM llm_audit")
    # No partial cache write, no audit row.
    assert cache_count == 0
    assert audit_count == 0


@pytest.mark.asyncio
async def test_target_schema_loads_cleanly():
    schema = load_target_schema()
    assert schema["title"] == "WebhookExtractionV1"
    assert "classification" in schema["required"]
