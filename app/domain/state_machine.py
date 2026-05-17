"""Entity state machines for shipments and invoices.

States and allowed transitions are encoded as **data**, not scattered ``if``s.
This file is the single source of truth for what shipments and invoices can
do; tests assert it.

Apply semantics:

  - Idempotent. ``applied_events(entity_id, event_id)`` is the hard guarantee.
  - Time-aware. Events older than ``last_applied_ts`` are recorded but never
    move state.
  - Transactional. Lock + check + write happen in one DB transaction.
  - Replay-safe. Running the same sequence twice produces the same projection.
"""

from __future__ import annotations

from typing import Literal

import asyncpg

from app.domain.canonical import (
    INVOICE_EVENT_TO_STATE,
    SHIPMENT_EVENT_TO_STATE,
    CanonicalEvent,
    CanonicalInvoiceEvent,
    CanonicalShipmentEvent,
    CanonicalUnclassifiedEvent,
    InvoiceState,
    ShipmentState,
)
from app.logging import get_logger
from app.metrics import STATE_TRANSITION_TOTAL

log = get_logger("state_machine")

ApplyOutcome = Literal[
    "applied",
    "applied_initial",
    "already_applied",
    "stale_skipped",
    "transition_rejected",
    "unclassified_recorded",
]


# Initial-state (None) is permissive: any non-terminal state can be the
# first observed state. This handles out-of-order arrivals where we see
# IN_TRANSIT before PICKED_UP, or where the vendor backfills mid-life.
SHIPMENT_TRANSITIONS: dict[ShipmentState | None, set[ShipmentState]] = {
    None: {
        ShipmentState.PICKED_UP,
        ShipmentState.IN_TRANSIT,
        ShipmentState.OUT_FOR_DELIVERY,
        ShipmentState.DELIVERED,
        ShipmentState.EXCEPTION,
        ShipmentState.CANCELLED,
    },
    ShipmentState.PICKED_UP: {
        ShipmentState.IN_TRANSIT,
        ShipmentState.OUT_FOR_DELIVERY,
        ShipmentState.DELIVERED,
        ShipmentState.EXCEPTION,
        ShipmentState.CANCELLED,
    },
    ShipmentState.IN_TRANSIT: {
        ShipmentState.OUT_FOR_DELIVERY,
        ShipmentState.DELIVERED,
        ShipmentState.EXCEPTION,
        ShipmentState.CANCELLED,
    },
    ShipmentState.OUT_FOR_DELIVERY: {
        ShipmentState.DELIVERED,
        ShipmentState.EXCEPTION,
    },
    ShipmentState.EXCEPTION: {
        ShipmentState.IN_TRANSIT,
        ShipmentState.OUT_FOR_DELIVERY,
        ShipmentState.DELIVERED,
        ShipmentState.CANCELLED,
    },
    ShipmentState.DELIVERED: set(),
    ShipmentState.CANCELLED: set(),
}


INVOICE_TRANSITIONS: dict[InvoiceState | None, set[InvoiceState]] = {
    None: {InvoiceState.ISSUED, InvoiceState.PAID, InvoiceState.VOIDED, InvoiceState.REFUNDED},
    InvoiceState.ISSUED: {InvoiceState.PAID, InvoiceState.VOIDED},
    InvoiceState.PAID: {InvoiceState.REFUNDED},
    InvoiceState.VOIDED: set(),
    InvoiceState.REFUNDED: set(),
}


# ---------------------------------------------------------------------------- #
# apply_event
# ---------------------------------------------------------------------------- #


async def apply_event(conn: asyncpg.Connection, event: CanonicalEvent) -> ApplyOutcome:
    """Apply a canonical event to its projection.

    The whole call runs in one DB transaction. The caller (worker) does not
    need to start one.
    """

    if isinstance(event, CanonicalShipmentEvent):
        outcome = await _apply_shipment(conn, event)
    elif isinstance(event, CanonicalInvoiceEvent):
        outcome = await _apply_invoice(conn, event)
    elif isinstance(event, CanonicalUnclassifiedEvent):
        outcome = "unclassified_recorded"
    else:  # pragma: no cover - exhaustive
        raise TypeError(f"unsupported canonical event type: {type(event).__name__}")

    entity_type = event.classification.value
    STATE_TRANSITION_TOTAL.labels(entity_type=entity_type, outcome=outcome).inc()
    return outcome


async def _apply_shipment(conn: asyncpg.Connection, ev: CanonicalShipmentEvent) -> ApplyOutcome:
    target_state = SHIPMENT_EVENT_TO_STATE[ev.event_type]

    async with conn.transaction():
        entity_id = await _get_or_create_entity(
            conn, vendor_id=ev.vendor_id, entity_type="shipment", external_id=ev.entity_external_id
        )

        # Idempotency guard: this event is observed for this entity exactly once.
        applied = await conn.fetchval(
            """
            INSERT INTO applied_events (entity_id, event_id, target_state)
            VALUES ($1, $2, $3)
            ON CONFLICT (entity_id, event_id) DO NOTHING
            RETURNING entity_id
            """,
            entity_id,
            ev.event_id,
            target_state.value,
        )
        if applied is None:
            return "already_applied"

        # Race-tolerant first-event path. We try to insert the projection in
        # one statement; if RETURNING gives us a row, this is the genuine
        # initial event and we are done. If not, another worker won the race
        # while we were not holding the row lock; we fall through to the
        # existing-row branch which acquires FOR UPDATE and re-evaluates.
        inserted = await conn.fetchval(
            """
            INSERT INTO shipments (entity_id, vendor_id, external_id, state,
                                   last_applied_event_id, last_applied_ts,
                                   version, reference_ids, location)
            VALUES ($1, $2, $3, $4, $5, $6, 1, $7::jsonb, $8::jsonb)
            ON CONFLICT (entity_id) DO NOTHING
            RETURNING entity_id
            """,
            entity_id,
            ev.vendor_id,
            ev.entity_external_id,
            target_state.value,
            ev.event_id,
            ev.event_timestamp,
            ev.reference_ids or {},
            ev.location.model_dump() if ev.location else None,
        )
        if inserted is not None:
            return "applied_initial"

        existing = await conn.fetchrow(
            """
            SELECT state, last_applied_ts, version
            FROM shipments
            WHERE entity_id = $1
            FOR UPDATE
            """,
            entity_id,
        )
        assert existing is not None  # we lost the insert race, so the row exists
        current_state = ShipmentState(existing["state"])

        if ev.event_timestamp < existing["last_applied_ts"]:
            await _log_stale(
                conn,
                entity_id,
                ev.event_id,
                "older_than_last_applied",
                {
                    "current_state": current_state.value,
                    "current_last_applied_ts": existing["last_applied_ts"].isoformat(),
                    "event_timestamp": ev.event_timestamp.isoformat(),
                },
            )
            return "stale_skipped"

        allowed = SHIPMENT_TRANSITIONS.get(current_state, set())
        if target_state not in allowed:
            await _log_stale(
                conn,
                entity_id,
                ev.event_id,
                f"disallowed_transition:{current_state.value}->{target_state.value}",
                None,
            )
            return "transition_rejected"

        # No-op transition (target_state == current_state): record timestamp / refs only.
        if target_state == current_state:
            await conn.execute(
                """
                UPDATE shipments
                   SET last_applied_event_id = $1,
                       last_applied_ts = $2,
                       reference_ids = reference_ids || $3::jsonb,
                       updated_at = now(),
                       version = version + 1
                 WHERE entity_id = $4 AND version = $5
                """,
                ev.event_id,
                ev.event_timestamp,
                ev.reference_ids or {},
                entity_id,
                existing["version"],
            )
            return "applied"

        await conn.execute(
            """
            UPDATE shipments
               SET state = $1,
                   last_applied_event_id = $2,
                   last_applied_ts = $3,
                   version = version + 1,
                   reference_ids = reference_ids || $4::jsonb,
                   location = COALESCE($5::jsonb, location),
                   updated_at = now()
             WHERE entity_id = $6 AND version = $7
            """,
            target_state.value,
            ev.event_id,
            ev.event_timestamp,
            ev.reference_ids or {},
            ev.location.model_dump() if ev.location else None,
            entity_id,
            existing["version"],
        )
        return "applied"


async def _apply_invoice(conn: asyncpg.Connection, ev: CanonicalInvoiceEvent) -> ApplyOutcome:
    target_state = INVOICE_EVENT_TO_STATE[ev.event_type]

    async with conn.transaction():
        entity_id = await _get_or_create_entity(
            conn, vendor_id=ev.vendor_id, entity_type="invoice", external_id=ev.entity_external_id
        )

        applied = await conn.fetchval(
            """
            INSERT INTO applied_events (entity_id, event_id, target_state)
            VALUES ($1, $2, $3)
            ON CONFLICT (entity_id, event_id) DO NOTHING
            RETURNING entity_id
            """,
            entity_id,
            ev.event_id,
            target_state.value,
        )
        if applied is None:
            return "already_applied"

        currency = ev.amount.currency if ev.amount else None
        amount_minor = ev.amount.amount_minor if ev.amount else None

        # Race-tolerant first-event path; see _apply_shipment for the rationale.
        inserted = await conn.fetchval(
            """
            INSERT INTO invoices (entity_id, vendor_id, external_id, state,
                                  last_applied_event_id, last_applied_ts, version,
                                  currency, amount_minor, due_at, linked_references)
            VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8, $9, $10::jsonb)
            ON CONFLICT (entity_id) DO NOTHING
            RETURNING entity_id
            """,
            entity_id,
            ev.vendor_id,
            ev.entity_external_id,
            target_state.value,
            ev.event_id,
            ev.event_timestamp,
            currency,
            amount_minor,
            ev.due_at,
            ev.linked_references or {},
        )
        if inserted is not None:
            return "applied_initial"

        existing = await conn.fetchrow(
            """
            SELECT state, last_applied_ts, version, currency, amount_minor
            FROM invoices
            WHERE entity_id = $1
            FOR UPDATE
            """,
            entity_id,
        )
        assert existing is not None
        current_state = InvoiceState(existing["state"])

        if ev.event_timestamp < existing["last_applied_ts"]:
            await _log_stale(
                conn,
                entity_id,
                ev.event_id,
                "older_than_last_applied",
                {
                    "current_state": current_state.value,
                    "current_last_applied_ts": existing["last_applied_ts"].isoformat(),
                    "event_timestamp": ev.event_timestamp.isoformat(),
                },
            )
            return "stale_skipped"

        allowed = INVOICE_TRANSITIONS.get(current_state, set())
        if target_state not in allowed and target_state != current_state:
            await _log_stale(
                conn,
                entity_id,
                ev.event_id,
                f"disallowed_transition:{current_state.value}->{target_state.value}",
                None,
            )
            return "transition_rejected"

        if target_state == current_state:
            await conn.execute(
                """
                UPDATE invoices
                   SET last_applied_event_id = $1,
                       last_applied_ts = $2,
                       linked_references = linked_references || $3::jsonb,
                       updated_at = now(),
                       version = version + 1
                 WHERE entity_id = $4 AND version = $5
                """,
                ev.event_id,
                ev.event_timestamp,
                ev.linked_references or {},
                entity_id,
                existing["version"],
            )
            return "applied"

        await conn.execute(
            """
            UPDATE invoices
               SET state = $1,
                   last_applied_event_id = $2,
                   last_applied_ts = $3,
                   version = version + 1,
                   currency = COALESCE($4, currency),
                   amount_minor = COALESCE($5, amount_minor),
                   due_at = COALESCE($6, due_at),
                   linked_references = linked_references || $7::jsonb,
                   updated_at = now()
             WHERE entity_id = $8 AND version = $9
            """,
            target_state.value,
            ev.event_id,
            ev.event_timestamp,
            currency,
            amount_minor,
            ev.due_at,
            ev.linked_references or {},
            entity_id,
            existing["version"],
        )
        return "applied"


# ---------------------------------------------------------------------------- #
# helpers
# ---------------------------------------------------------------------------- #


async def _get_or_create_entity(
    conn: asyncpg.Connection,
    *,
    vendor_id: str,
    entity_type: str,
    external_id: str,
) -> int:
    row = await conn.fetchrow(
        "SELECT id FROM entities WHERE vendor_id=$1 AND entity_type=$2 AND external_id=$3",
        vendor_id,
        entity_type,
        external_id,
    )
    if row is not None:
        return int(row["id"])

    entity_id = await conn.fetchval(
        """
        INSERT INTO entities (vendor_id, entity_type, external_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (vendor_id, entity_type, external_id) DO NOTHING
        RETURNING id
        """,
        vendor_id,
        entity_type,
        external_id,
    )
    if entity_id is not None:
        return int(entity_id)

    # Concurrent insert: read again.
    row = await conn.fetchrow(
        "SELECT id FROM entities WHERE vendor_id=$1 AND entity_type=$2 AND external_id=$3",
        vendor_id,
        entity_type,
        external_id,
    )
    assert row is not None  # invariant: just lost the race; the row exists
    return int(row["id"])


async def _log_stale(
    conn: asyncpg.Connection,
    entity_id: int,
    event_id: str,
    reason: str,
    detail: dict | None,
) -> None:
    await conn.execute(
        """
        INSERT INTO stale_event_log (entity_id, event_id, reason, detail)
        VALUES ($1, $2, $3, $4::jsonb)
        """,
        entity_id,
        event_id,
        reason,
        detail or {},
    )
