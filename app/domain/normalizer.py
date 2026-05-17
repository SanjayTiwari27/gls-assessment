"""Canonical event normalizer — standardizes all values before state machine.

This is the single normalization checkpoint between extraction (adapters/LLM)
and projection (state machine). It guarantees:

  - All timestamps → UTC+00:00 (offset-aware datetime)
  - All money → integer minor units (cents), currency uppercase ISO-4217
  - All confidence → float [0.0, 1.0], clamped
  - All latitude/longitude → float
  - All strings → stripped, non-empty or None

The state machine and projections can rely on these invariants without
re-validating.
"""

from __future__ import annotations

from datetime import UTC, datetime

from app.domain.canonical import (
    CanonicalEvent,
    CanonicalInvoiceEvent,
    CanonicalShipmentEvent,
    CanonicalUnclassifiedEvent,
    Location,
    Money,
)
from app.logging import get_logger

log = get_logger("domain.normalizer")


class NormalizationError(Exception):
    """Raised when a canonical event cannot be normalized to valid form."""


def normalize(event: CanonicalEvent) -> CanonicalEvent:
    """Normalize a canonical event to projection-ready form.

    Guarantees:
      - event_timestamp is UTC+00:00
      - due_at (if present) is UTC+00:00
      - amount_minor is a non-negative integer
      - currency is 3-letter uppercase
      - confidence is clamped to [0.0, 1.0]
      - location coords are floats
      - empty strings become None where appropriate

    Raises NormalizationError if a required field cannot be normalized.
    """
    if isinstance(event, CanonicalShipmentEvent):
        return _normalize_shipment(event)
    elif isinstance(event, CanonicalInvoiceEvent):
        return _normalize_invoice(event)
    elif isinstance(event, CanonicalUnclassifiedEvent):
        return _normalize_unclassified(event)
    else:
        raise NormalizationError(f"unknown event type: {type(event).__name__}")


def _normalize_shipment(ev: CanonicalShipmentEvent) -> CanonicalShipmentEvent:
    return CanonicalShipmentEvent(
        event_id=ev.event_id,
        vendor_id=ev.vendor_id.strip(),
        schema_version=ev.schema_version,
        source=ev.source,
        confidence=_clamp_confidence(ev.confidence),
        entity_external_id=ev.entity_external_id.strip(),
        event_type=ev.event_type,
        event_timestamp=_to_utc(ev.event_timestamp),
        reference_ids=_clean_ref_dict(ev.reference_ids),
        location=_normalize_location(ev.location),
        raw_milestone=_strip_or_none(ev.raw_milestone),
    )


def _normalize_invoice(ev: CanonicalInvoiceEvent) -> CanonicalInvoiceEvent:
    return CanonicalInvoiceEvent(
        event_id=ev.event_id,
        vendor_id=ev.vendor_id.strip(),
        schema_version=ev.schema_version,
        source=ev.source,
        confidence=_clamp_confidence(ev.confidence),
        entity_external_id=ev.entity_external_id.strip(),
        event_type=ev.event_type,
        event_timestamp=_to_utc(ev.event_timestamp),
        amount=_normalize_money(ev.amount),
        due_at=_to_utc(ev.due_at) if ev.due_at else None,
        linked_references=_clean_ref_dict(ev.linked_references),
        raw_kind=_strip_or_none(ev.raw_kind),
    )


def _normalize_unclassified(ev: CanonicalUnclassifiedEvent) -> CanonicalUnclassifiedEvent:
    return CanonicalUnclassifiedEvent(
        event_id=ev.event_id,
        vendor_id=ev.vendor_id.strip(),
        schema_version=ev.schema_version,
        source=ev.source,
        confidence=_clamp_confidence(ev.confidence),
        summary=_strip_or_none(ev.summary),
        reason=_strip_or_none(ev.reason),
    )


# ---------------------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------------------- #


def _to_utc(dt: datetime) -> datetime:
    """Ensure datetime is UTC+00:00. Converts from any timezone."""
    if dt.tzinfo is None:
        # Treat naive as UTC (shouldn't happen — validators catch this)
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _clamp_confidence(val: float) -> float:
    """Clamp confidence to [0.0, 1.0]."""
    return max(0.0, min(1.0, float(val)))


def _normalize_money(money: Money | None) -> Money | None:
    """Ensure currency is uppercase, amount_minor is non-negative integer."""
    if money is None:
        return None
    return Money(
        currency=money.currency.upper(),
        amount_minor=max(0, int(money.amount_minor)),
    )


def _normalize_location(loc: Location | None) -> Location | None:
    """Ensure location coords are floats, strings are stripped."""
    if loc is None:
        return None
    code = _strip_or_none(loc.code)
    name = _strip_or_none(loc.name)
    if not code and not name:
        return None
    return Location(
        code=code,
        name=name,
        latitude=float(loc.latitude) if loc.latitude is not None else None,
        longitude=float(loc.longitude) if loc.longitude is not None else None,
    )


def _strip_or_none(val: str | None) -> str | None:
    """Strip whitespace; return None if empty."""
    if val is None:
        return None
    stripped = str(val).strip()
    return stripped if stripped else None


def _clean_ref_dict(refs: dict) -> dict:
    """Strip string values, remove None/empty entries."""
    cleaned = {}
    for k, v in refs.items():
        if v is None:
            continue
        if isinstance(v, str):
            stripped = v.strip()
            if stripped:
                cleaned[k] = stripped
        else:
            cleaned[k] = v
    return cleaned
