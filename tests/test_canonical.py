from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.domain.canonical import (
    CanonicalInvoiceEvent,
    CanonicalShipmentEvent,
    CanonicalUnclassifiedEvent,
    InvoiceEventType,
    Money,
    ShipmentEventType,
    Source,
)


def _shipment(**overrides):
    base = dict(
        event_id="e1",
        vendor_id="maersk",
        entity_external_id="MAEU1:MSKU1",
        event_type=ShipmentEventType.IN_TRANSIT,
        event_timestamp=datetime(2026, 4, 21, 14, 47, 0, tzinfo=UTC),
        source=Source.DETERMINISTIC,
        confidence=0.95,
    )
    base.update(overrides)
    return CanonicalShipmentEvent(**base)


def test_shipment_requires_tz_aware_timestamp():
    with pytest.raises(ValidationError):
        _shipment(event_timestamp=datetime(2026, 4, 21, 14, 47, 0))  # naive


def test_invoice_amount_must_be_3letter_currency():
    with pytest.raises(ValidationError):
        Money(currency="EU", amount_minor=100)


def test_invoice_amount_minor_must_be_non_negative():
    with pytest.raises(ValidationError):
        Money(currency="EUR", amount_minor=-1)


def test_invoice_event_classification_pinned():
    inv = CanonicalInvoiceEvent(
        event_id="e1",
        vendor_id="globalfreightpay",
        entity_external_id="DOC-1",
        event_type=InvoiceEventType.ISSUED,
        event_timestamp=datetime(2026, 4, 15, 7, 0, 0, tzinfo=UTC),
        amount=Money(currency="EUR", amount_minor=100),
        source=Source.DETERMINISTIC,
        confidence=1.0,
    )
    assert inv.classification.value == "invoice"


def test_unclassified_does_not_require_event_type():
    ev = CanonicalUnclassifiedEvent(
        event_id="e1",
        vendor_id="random",
        source=Source.DETERMINISTIC,
        confidence=0.6,
        summary="weather advisory",
    )
    assert ev.classification.value == "unclassified"


def test_extra_fields_rejected():
    with pytest.raises(ValidationError):
        _shipment(unexpected="field")
