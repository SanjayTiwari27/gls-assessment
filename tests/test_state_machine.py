"""DB-backed tests for apply_event.

These tests need a live Postgres (TEST_DATABASE_URL or DATABASE_URL). They are
skipped automatically if no DB is reachable.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.domain.canonical import (
    CanonicalInvoiceEvent,
    CanonicalShipmentEvent,
    InvoiceEventType,
    Money,
    ShipmentEventType,
    Source,
)
from app.domain.state_machine import apply_event

pytestmark = pytest.mark.e2e


def _ship(event_id: str, event_type: ShipmentEventType, ts: datetime, *, ext_id: str = "MAEU1:MSKU1") -> CanonicalShipmentEvent:
    return CanonicalShipmentEvent(
        event_id=event_id,
        vendor_id="maersk",
        entity_external_id=ext_id,
        event_type=event_type,
        event_timestamp=ts,
        reference_ids={"mbl_number": "MAEU1", "container": "MSKU1"},
        source=Source.DETERMINISTIC,
        confidence=0.95,
    )


def _inv(event_id: str, event_type: InvoiceEventType, ts: datetime, *, ext_id: str = "DOC-1") -> CanonicalInvoiceEvent:
    return CanonicalInvoiceEvent(
        event_id=event_id,
        vendor_id="globalfreightpay",
        entity_external_id=ext_id,
        event_type=event_type,
        event_timestamp=ts,
        amount=Money(currency="EUR", amount_minor=2_435_075),
        source=Source.DETERMINISTIC,
        confidence=0.95,
    )


async def _seed_raw_event(pool, event_id: str) -> None:
    """Insert the minimum raw_events row required by FK constraints."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO raw_events (event_id, vendor_id, payload, headers)
            VALUES ($1, $2, '{}'::jsonb, '{}'::jsonb)
            ON CONFLICT (event_id) DO NOTHING
            """,
            event_id, "test",
        )


@pytest.mark.asyncio
async def test_first_shipment_event_creates_projection(clean_db):
    pool = clean_db
    await _seed_raw_event(pool, "ship-1")
    ev = _ship("ship-1", ShipmentEventType.PICKED_UP, datetime(2026, 4, 19, 3, 15, tzinfo=UTC))

    async with pool.acquire() as conn:
        outcome = await apply_event(conn, ev)
    assert outcome == "applied_initial"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, version FROM shipments WHERE vendor_id=$1 AND external_id=$2",
            "maersk", "MAEU1:MSKU1",
        )
        assert row["state"] == "PICKED_UP"
        assert row["version"] == 1


@pytest.mark.asyncio
async def test_shipment_progresses_through_lifecycle(clean_db):
    pool = clean_db
    base = datetime(2026, 4, 19, 0, 0, tzinfo=UTC)
    seq = [
        ("e1", ShipmentEventType.PICKED_UP, base),
        ("e2", ShipmentEventType.IN_TRANSIT, base + timedelta(hours=4)),
        ("e3", ShipmentEventType.OUT_FOR_DELIVERY, base + timedelta(days=8)),
        ("e4", ShipmentEventType.DELIVERED, base + timedelta(days=8, hours=4)),
    ]

    for eid, _, _ in seq:
        await _seed_raw_event(pool, eid)
    for eid, et, ts in seq:
        async with pool.acquire() as conn:
            await apply_event(conn, _ship(eid, et, ts))

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, last_applied_event_id FROM shipments WHERE vendor_id=$1 AND external_id=$2",
            "maersk", "MAEU1:MSKU1",
        )
    assert row["state"] == "DELIVERED"
    assert row["last_applied_event_id"] == "e4"


@pytest.mark.asyncio
async def test_idempotent_replay_of_same_event(clean_db):
    pool = clean_db
    await _seed_raw_event(pool, "ship-1")
    ev = _ship("ship-1", ShipmentEventType.IN_TRANSIT, datetime(2026, 4, 21, 14, 47, tzinfo=UTC))

    async with pool.acquire() as conn:
        outcomes = []
        for _ in range(5):
            outcomes.append(await apply_event(conn, ev))

    # First call applies, all subsequent are no-ops.
    assert outcomes[0] == "applied_initial"
    assert all(o == "already_applied" for o in outcomes[1:])

    async with pool.acquire() as conn:
        ae = await conn.fetchval(
            "SELECT count(*) FROM applied_events WHERE event_id = $1", "ship-1"
        )
        ob = await conn.fetchval(
            "SELECT count(*) FROM outbox WHERE event_id = $1", "ship-1"
        )
    assert ae == 1
    assert ob == 1


@pytest.mark.asyncio
async def test_out_of_order_arrival_does_not_move_state_backward(clean_db):
    """Maersk fixture #1 (IN_TRANSIT, 2026-04-21) arrives BEFORE fixture #2
    (PICKED_UP, 2026-04-19). The IN_TRANSIT must end as the entity state,
    and the PICKED_UP must be recorded in stale_event_log."""

    pool = clean_db
    await _seed_raw_event(pool, "in_transit")
    await _seed_raw_event(pool, "picked_up")

    in_transit = _ship("in_transit", ShipmentEventType.IN_TRANSIT,
                       datetime(2026, 4, 21, 14, 47, tzinfo=UTC))
    picked_up = _ship("picked_up", ShipmentEventType.PICKED_UP,
                      datetime(2026, 4, 19, 3, 15, tzinfo=UTC))

    async with pool.acquire() as conn:
        a = await apply_event(conn, in_transit)
        b = await apply_event(conn, picked_up)

    assert a == "applied_initial"
    assert b == "stale_skipped"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state FROM shipments WHERE vendor_id=$1 AND external_id=$2",
            "maersk", "MAEU1:MSKU1",
        )
        stale = await conn.fetchrow(
            "SELECT reason FROM stale_event_log WHERE event_id=$1", "picked_up"
        )
    assert row["state"] == "IN_TRANSIT"
    assert stale and stale["reason"] == "older_than_last_applied"


@pytest.mark.asyncio
async def test_disallowed_transition_rejected(clean_db):
    pool = clean_db
    await _seed_raw_event(pool, "delivered")
    await _seed_raw_event(pool, "picked_up_after")

    delivered = _ship("delivered", ShipmentEventType.DELIVERED,
                      datetime(2026, 4, 19, 0, 0, tzinfo=UTC))
    later_picked_up = _ship("picked_up_after", ShipmentEventType.PICKED_UP,
                            datetime(2026, 4, 20, 0, 0, tzinfo=UTC))

    async with pool.acquire() as conn:
        await apply_event(conn, delivered)
        outcome = await apply_event(conn, later_picked_up)

    assert outcome == "transition_rejected"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state FROM shipments WHERE vendor_id=$1 AND external_id=$2",
            "maersk", "MAEU1:MSKU1",
        )
        stale = await conn.fetchrow(
            "SELECT reason FROM stale_event_log WHERE event_id=$1", "picked_up_after"
        )
    assert row["state"] == "DELIVERED"
    assert stale and stale["reason"].startswith("disallowed_transition:DELIVERED->PICKED_UP")


@pytest.mark.asyncio
async def test_invoice_lifecycle_issued_to_paid(clean_db):
    pool = clean_db
    await _seed_raw_event(pool, "issued")
    await _seed_raw_event(pool, "paid")

    issued = _inv("issued", InvoiceEventType.ISSUED,
                  datetime(2026, 4, 15, 7, 0, tzinfo=UTC))
    paid = _inv("paid", InvoiceEventType.PAID,
                datetime(2026, 4, 22, 16, 47, 11, tzinfo=UTC))

    async with pool.acquire() as conn:
        await apply_event(conn, issued)
        await apply_event(conn, paid)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, currency, amount_minor FROM invoices WHERE vendor_id=$1 AND external_id=$2",
            "globalfreightpay", "DOC-1",
        )
    assert row["state"] == "PAID"
    assert row["currency"] == "EUR"
    assert row["amount_minor"] == 2_435_075


@pytest.mark.asyncio
async def test_invoice_paid_to_voided_rejected(clean_db):
    """Once an invoice is PAID, VOIDED is no longer a valid transition."""
    pool = clean_db
    await _seed_raw_event(pool, "paid")
    await _seed_raw_event(pool, "voided_late")

    paid = _inv("paid", InvoiceEventType.PAID,
                datetime(2026, 4, 22, 16, 47, tzinfo=UTC))
    voided_late = _inv("voided_late", InvoiceEventType.VOIDED,
                       datetime(2026, 4, 23, 0, 0, tzinfo=UTC))

    async with pool.acquire() as conn:
        await apply_event(conn, paid)
        outcome = await apply_event(conn, voided_late)

    assert outcome == "transition_rejected"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state FROM invoices WHERE vendor_id=$1 AND external_id=$2",
            "globalfreightpay", "DOC-1",
        )
    assert row["state"] == "PAID"


@pytest.mark.asyncio
async def test_invoice_paid_to_refunded_allowed(clean_db):
    pool = clean_db
    await _seed_raw_event(pool, "paid")
    await _seed_raw_event(pool, "refunded")

    paid = _inv("paid", InvoiceEventType.PAID,
                datetime(2026, 4, 22, 16, 47, tzinfo=UTC))
    refunded = _inv("refunded", InvoiceEventType.REFUNDED,
                    datetime(2026, 4, 28, 0, 0, tzinfo=UTC))

    async with pool.acquire() as conn:
        await apply_event(conn, paid)
        outcome = await apply_event(conn, refunded)

    assert outcome == "applied"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state FROM invoices WHERE vendor_id=$1 AND external_id=$2",
            "globalfreightpay", "DOC-1",
        )
    assert row["state"] == "REFUNDED"
