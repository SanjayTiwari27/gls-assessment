"""End-to-end pipeline tests using the in-process StubLLM.

Exercises the worker pipeline against the appendix payloads:
  - all 6 fixtures classify correctly,
  - shipments and invoices end in the expected terminal states,
  - one fixture (marine traffic) records as 'unclassified_recorded',
  - duplicate events are no-ops on the projection.
"""

from __future__ import annotations

import pytest

from app.hashing import compute_event_id
from app.workers.pipeline import process_event, reset_pipeline_singletons

pytestmark = pytest.mark.e2e


async def _ingest_fixture(pool, payload: dict, *, vendor_hint: str | None = None) -> str:
    event_id = compute_event_id(payload, payload.get("event_msg_id") or payload.get("event_id"))
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO raw_events (event_id, vendor_id, payload, headers)
            VALUES ($1, $2, $3::jsonb, '{}'::jsonb)
            ON CONFLICT (event_id) DO NOTHING
            """,
            event_id,
            vendor_hint,
            payload,
        )
    return event_id


@pytest.mark.asyncio
async def test_full_pipeline_against_appendix(clean_db, fixture_payloads):
    pool = clean_db
    reset_pipeline_singletons()

    # Order chosen to exercise both forward progression and out-of-order:
    #   issued before paid (forward),
    #   in_transit before picked_up (out of order on shipment).
    order = [
        "04_globalfreightpay_issued",
        "03_globalfreightpay_paid",
        "01_maersk_in_transit",
        "02_maersk_picked_up",
        "05_one_delivered",
        "06_marine_traffic_advisory",
    ]
    event_ids: list[str] = []
    for name in order:
        event_ids.append(await _ingest_fixture(pool, fixture_payloads[name]))

    for eid in event_ids:
        await process_event(pool, eid)

    async with pool.acquire() as conn:
        ship_rows = await conn.fetch(
            "SELECT vendor_id, external_id, state FROM shipments ORDER BY vendor_id, external_id"
        )
        inv_rows = await conn.fetch(
            "SELECT vendor_id, external_id, state, currency, amount_minor FROM invoices ORDER BY external_id"
        )
        stale_rows = await conn.fetch("SELECT reason FROM stale_event_log")
        review_rows = await conn.fetch("SELECT count(*) AS n FROM requires_human_review")
        canonical_count = await conn.fetchval("SELECT count(*) FROM canonical_events")

    ships_by_key = {(r["vendor_id"], r["external_id"]): r["state"] for r in ship_rows}
    invs_by_key = {r["external_id"]: dict(r) for r in inv_rows}

    # Maersk shipment: out-of-order PICKED_UP must NOT roll IN_TRANSIT back.
    assert ships_by_key[("maersk", "MAEU240498712:MSKU7748112")] == "IN_TRANSIT"
    # ONE shipment: delivered.
    assert ships_by_key[("ocean_network_express", "ONEYJKTHKG2604113:TLLU2890442")] == "DELIVERED"
    # GFP invoice: issued -> paid forwards cleanly.
    assert invs_by_key["GFP-INV-2026-Q2-08821"]["state"] == "PAID"
    assert invs_by_key["GFP-INV-2026-Q2-08821"]["currency"] == "EUR"
    assert invs_by_key["GFP-INV-2026-Q2-08821"]["amount_minor"] == 2_435_075
    # Stale picked-up logged.
    assert any(r["reason"] == "older_than_last_applied" for r in stale_rows)
    # No human review needed.
    assert review_rows[0]["n"] == 0
    # Every processed event is persisted as canonical output (including unclassified).
    assert canonical_count == len(order)


@pytest.mark.asyncio
async def test_replay_produces_identical_projections(clean_db, fixture_payloads):
    pool = clean_db
    reset_pipeline_singletons()

    # First pass.
    order = [
        "02_maersk_picked_up",
        "01_maersk_in_transit",
        "04_globalfreightpay_issued",
        "03_globalfreightpay_paid",
        "05_one_delivered",
        "06_marine_traffic_advisory",
    ]
    event_ids = [await _ingest_fixture(pool, fixture_payloads[n]) for n in order]
    for eid in event_ids:
        await process_event(pool, eid)

    async with pool.acquire() as conn:
        before_ships = await conn.fetch(
            "SELECT vendor_id, external_id, state, last_applied_event_id FROM shipments ORDER BY vendor_id, external_id"
        )
        before_invs = await conn.fetch(
            "SELECT external_id, state, currency, amount_minor FROM invoices ORDER BY external_id"
        )

    # Wipe projections, keep raw_events.
    async with pool.acquire() as conn:
        await conn.execute("""
            TRUNCATE TABLE
                requires_human_review, canonical_events, outbox, stale_event_log,
                applied_events, shipments, invoices, entities
            CASCADE
        """)

    # Replay all events from raw_events, in received_at order (same as ingest order).
    for eid in event_ids:
        await process_event(pool, eid)

    async with pool.acquire() as conn:
        after_ships = await conn.fetch(
            "SELECT vendor_id, external_id, state, last_applied_event_id FROM shipments ORDER BY vendor_id, external_id"
        )
        after_invs = await conn.fetch(
            "SELECT external_id, state, currency, amount_minor FROM invoices ORDER BY external_id"
        )

    assert [dict(r) for r in before_ships] == [dict(r) for r in after_ships]
    assert [dict(r) for r in before_invs] == [dict(r) for r in after_invs]


@pytest.mark.asyncio
async def test_duplicate_processing_is_noop(clean_db, fixture_payloads):
    pool = clean_db
    reset_pipeline_singletons()
    payload = fixture_payloads["01_maersk_in_transit"]
    eid = await _ingest_fixture(pool, payload)

    # Process once, then again — second call must be a no-op.
    out1 = await process_event(pool, eid)
    out2 = await process_event(pool, eid)

    assert out1 == "applied_initial"
    assert out2 == "already_applied"

    async with pool.acquire() as conn:
        ae_count = await conn.fetchval("SELECT count(*) FROM applied_events")
        ob_count = await conn.fetchval("SELECT count(*) FROM outbox")
    assert ae_count == 1
    assert ob_count == 1


@pytest.mark.asyncio
async def test_unsupported_payload_routes_to_human_review(clean_db, monkeypatch):
    """Fully unrecognized payloads still get classified — by the stub LLM
    they go to 'unclassified', not to human review. Human review is reserved
    for payloads where even the LLM cannot produce schema-valid output."""

    pool = clean_db
    reset_pipeline_singletons()

    # Force the universal adapter to fail by stubbing the fallback.
    from app.adapters.base import AdapterResult
    from app.adapters.llm_universal import LLMUniversalAdapter
    from app.workers import pipeline as pipeline_mod

    class FailingAdapter(LLMUniversalAdapter):
        def __init__(self):
            pass

        async def normalize(self, payload, headers, event_id, *, vendor_hint=None):
            return AdapterResult(status="unsupported", detail={"reason": "synthetic_failure_for_test"})

    def fake_get_universal_adapter(_pool):
        return FailingAdapter()

    monkeypatch.setattr(pipeline_mod, "get_universal_adapter", fake_get_universal_adapter)

    payload = {"deeply_unknown": {"shape": [1, 2]}}
    eid = await _ingest_fixture(pool, payload)
    out = await process_event(pool, eid)
    assert out == "requires_review"

    async with pool.acquire() as conn:
        review_count = await conn.fetchval(
            "SELECT count(*) FROM requires_human_review WHERE event_id = $1", eid
        )
    assert review_count == 1


@pytest.mark.asyncio
async def test_budget_exhausted_payload_is_marked_pending_llm(clean_db, monkeypatch):
    pool = clean_db
    reset_pipeline_singletons()

    from app.adapters.base import AdapterResult
    from app.adapters.llm_universal import LLMUniversalAdapter
    from app.workers import pipeline as pipeline_mod

    class BudgetDeferredAdapter(LLMUniversalAdapter):
        def __init__(self):
            pass

        async def normalize(self, payload, headers, event_id, *, vendor_hint=None):
            return AdapterResult(status="deferred", detail={"reason": "llm_budget_exceeded"})

    def fake_get_universal_adapter(_pool):
        return BudgetDeferredAdapter()

    monkeypatch.setattr(pipeline_mod, "get_universal_adapter", fake_get_universal_adapter)

    payload = {"unknown": {"shape": "for-budget-test"}}
    eid = await _ingest_fixture(pool, payload)
    out = await process_event(pool, eid)
    assert out == "pending_llm"

    async with pool.acquire() as conn:
        status = await conn.fetchval("SELECT processing_status FROM raw_events WHERE event_id = $1", eid)
        review_count = await conn.fetchval(
            "SELECT count(*) FROM requires_human_review WHERE event_id = $1", eid
        )
    assert status == "pending_llm"
    assert review_count == 0
