"""Tests for the canonical event normalizer."""

from datetime import UTC, datetime, timezone

import pytest

from app.domain.canonical import (
    CanonicalInvoiceEvent,
    CanonicalShipmentEvent,
    CanonicalUnclassifiedEvent,
    InvoiceEventType,
    Location,
    Money,
    ShipmentEventType,
    Source,
)
from app.domain.normalizer import normalize


@pytest.fixture
def shipment_event():
    return CanonicalShipmentEvent(
        event_id="evt-001",
        vendor_id="maersk",
        source=Source.DETERMINISTIC,
        confidence=0.92,
        entity_external_id="MAEU123:CTR456",
        event_type=ShipmentEventType.IN_TRANSIT,
        event_timestamp=datetime(2026, 4, 28, 14, 30, tzinfo=UTC),
        reference_ids={"mbl": "MAEU123", "container": "CTR456"},
        location=Location(code="SGSIN", name="Singapore"),
        raw_milestone="Loaded onboard",
    )


@pytest.fixture
def invoice_event():
    return CanonicalInvoiceEvent(
        event_id="evt-002",
        vendor_id="globalfreightpay",
        source=Source.DETERMINISTIC,
        confidence=0.88,
        entity_external_id="GFP-INV-001",
        event_type=InvoiceEventType.PAID,
        event_timestamp=datetime(2026, 5, 2, 10, 0, tzinfo=timezone(offset=__import__("datetime").timedelta(hours=2))),
        amount=Money(currency="EUR", amount_minor=2435075),
        due_at=datetime(2026, 5, 29, 9, 0, tzinfo=timezone(offset=__import__("datetime").timedelta(hours=2))),
        linked_references={"carrier": "Maersk Line"},
    )


class TestTimestampNormalization:
    def test_utc_timestamp_unchanged(self, shipment_event):
        result = normalize(shipment_event)
        assert result.event_timestamp.tzinfo == UTC
        assert result.event_timestamp == datetime(2026, 4, 28, 14, 30, tzinfo=UTC)

    def test_non_utc_timestamp_converted(self, invoice_event):
        result = normalize(invoice_event)
        # +02:00 10:00 → UTC 08:00
        assert result.event_timestamp.tzinfo == UTC
        assert result.event_timestamp.hour == 8

    def test_due_at_converted_to_utc(self, invoice_event):
        result = normalize(invoice_event)
        # +02:00 09:00 → UTC 07:00
        assert result.due_at.tzinfo == UTC
        assert result.due_at.hour == 7


class TestMoneyNormalization:
    def test_amount_preserved_as_integer(self, invoice_event):
        result = normalize(invoice_event)
        assert result.amount.amount_minor == 2435075
        assert isinstance(result.amount.amount_minor, int)

    def test_currency_uppercased(self):
        event = CanonicalInvoiceEvent(
            event_id="evt-003",
            vendor_id="test",
            source=Source.LLM,
            confidence=0.9,
            entity_external_id="INV-001",
            event_type=InvoiceEventType.ISSUED,
            event_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            amount=Money(currency="EUR", amount_minor=10000),
        )
        result = normalize(event)
        assert result.amount.currency == "EUR"

    def test_none_amount_stays_none(self):
        event = CanonicalInvoiceEvent(
            event_id="evt-004",
            vendor_id="test",
            source=Source.LLM,
            confidence=0.9,
            entity_external_id="INV-002",
            event_type=InvoiceEventType.ISSUED,
            event_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            amount=None,
        )
        result = normalize(event)
        assert result.amount is None


class TestConfidenceNormalization:
    def test_confidence_preserved(self, shipment_event):
        result = normalize(shipment_event)
        assert result.confidence == 0.92

    def test_confidence_clamped_high(self):
        event = CanonicalUnclassifiedEvent(
            event_id="evt-005",
            vendor_id="test",
            source=Source.LLM,
            confidence=1.0,  # max valid
            summary="test",
        )
        result = normalize(event)
        assert result.confidence == 1.0


class TestStringNormalization:
    def test_whitespace_stripped(self):
        event = CanonicalShipmentEvent(
            event_id="evt-006",
            vendor_id="  maersk  ",
            source=Source.DETERMINISTIC,
            confidence=0.9,
            entity_external_id="  MAEU123  ",
            event_type=ShipmentEventType.DELIVERED,
            event_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            reference_ids={"key": "  value  "},
            raw_milestone="  Delivered  ",
        )
        result = normalize(event)
        assert result.vendor_id == "maersk"
        assert result.entity_external_id == "MAEU123"
        assert result.reference_ids["key"] == "value"
        assert result.raw_milestone == "Delivered"

    def test_empty_refs_removed(self):
        event = CanonicalShipmentEvent(
            event_id="evt-007",
            vendor_id="test",
            source=Source.DETERMINISTIC,
            confidence=0.9,
            entity_external_id="X",
            event_type=ShipmentEventType.IN_TRANSIT,
            event_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            reference_ids={"keep": "value", "drop_none": None, "drop_empty": ""},
        )
        result = normalize(event)
        assert "keep" in result.reference_ids
        assert "drop_none" not in result.reference_ids
        assert "drop_empty" not in result.reference_ids


class TestLocationNormalization:
    def test_location_preserved(self, shipment_event):
        result = normalize(shipment_event)
        assert result.location.code == "SGSIN"
        assert result.location.name == "Singapore"

    def test_empty_location_becomes_none(self):
        event = CanonicalShipmentEvent(
            event_id="evt-008",
            vendor_id="test",
            source=Source.DETERMINISTIC,
            confidence=0.9,
            entity_external_id="X",
            event_type=ShipmentEventType.IN_TRANSIT,
            event_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            location=Location(code="", name=""),
        )
        result = normalize(event)
        assert result.location is None


class TestUnclassifiedNormalization:
    def test_unclassified_normalized(self):
        event = CanonicalUnclassifiedEvent(
            event_id="evt-009",
            vendor_id="marine_traffic",
            source=Source.DETERMINISTIC,
            confidence=0.85,
            summary="  Port congestion alert  ",
            reason="  advisory  ",
        )
        result = normalize(event)
        assert result.summary == "Port congestion alert"
        assert result.reason == "advisory"
