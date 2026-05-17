"""Unit tests for the schema-driven adapter using appendix payload fixtures.

These tests exercise the same extractions the old hardcoded adapters did, but
via schema_doc declarations + SchemaDrivenAdapter rather than per-vendor Python
classes. Tests are pure (no DB required) — schema_docs are imported directly.
"""

from datetime import UTC, datetime

from app.adapters.schema_driven import SchemaDrivenAdapter
from app.domain.canonical import (
    Classification,
    InvoiceEventType,
    ShipmentEventType,
    Source,
)
from tests.vendor_schemas import (
    GLOBALFREIGHTPAY_SCHEMA_DOC,
    MAERSK_SCHEMA_DOC,
    MARINE_TRAFFIC_SCHEMA_DOC,
    ONE_SCHEMA_DOC,
)

EVENT_ID = "test-event-id"
adapter = SchemaDrivenAdapter()


def test_maersk_in_transit(fixture_payloads):
    payload = fixture_payloads["01_maersk_in_transit"]
    result = adapter.extract(
        payload=payload,
        headers={},
        event_id=EVENT_ID,
        vendor_id="maersk",
        schema_doc=MAERSK_SCHEMA_DOC,
        canonical_state="shipment.in_transit",
    )
    assert result.success
    ev = result.canonical_event
    assert ev.classification == Classification.SHIPMENT
    assert ev.event_type == ShipmentEventType.IN_TRANSIT
    assert ev.entity_external_id == "MAEU240498712:MSKU7748112"
    assert ev.event_timestamp == datetime(2026, 4, 21, 14, 47, 0, tzinfo=UTC)
    assert ev.source == Source.DETERMINISTIC
    assert ev.location is not None
    assert ev.location.code == "CNSHA"
    assert ev.reference_ids["mbl_number"] == "MAEU240498712"
    assert ev.reference_ids["container"] == "MSKU7748112"


def test_maersk_picked_up(fixture_payloads):
    payload = fixture_payloads["02_maersk_picked_up"]
    result = adapter.extract(
        payload=payload,
        headers={},
        event_id=EVENT_ID,
        vendor_id="maersk",
        schema_doc=MAERSK_SCHEMA_DOC,
        canonical_state="shipment.picked_up",
    )
    assert result.success
    ev = result.canonical_event
    assert ev.event_type == ShipmentEventType.PICKED_UP
    assert ev.entity_external_id == "MAEU240498712:MSKU7748112"
    assert ev.event_timestamp < datetime(2026, 4, 21, tzinfo=UTC)


def test_maersk_extracts_raw_event_type(fixture_payloads):
    payload = fixture_payloads["01_maersk_in_transit"]
    result = adapter.extract(
        payload=payload,
        headers={},
        event_id=EVENT_ID,
        vendor_id="maersk",
        schema_doc=MAERSK_SCHEMA_DOC,
        canonical_state="shipment.in_transit",
    )
    assert result.raw_event_type == "Loaded onboard and sailed"


def test_one_delivered(fixture_payloads):
    payload = fixture_payloads["05_one_delivered"]
    result = adapter.extract(
        payload=payload,
        headers={},
        event_id=EVENT_ID,
        vendor_id="ocean_network_express",
        schema_doc=ONE_SCHEMA_DOC,
        canonical_state="shipment.delivered",
    )
    assert result.success
    ev = result.canonical_event
    assert ev.event_type == ShipmentEventType.DELIVERED
    assert ev.entity_external_id == "ONEYJKTHKG2604113:TLLU2890442"
    assert ev.event_timestamp == datetime(2026, 4, 28, 2, 42, 0, tzinfo=UTC)
    assert ev.location is not None and ev.location.code == "IDJKT"


def test_globalfreightpay_paid(fixture_payloads):
    payload = fixture_payloads["03_globalfreightpay_paid"]
    result = adapter.extract(
        payload=payload,
        headers={},
        event_id=EVENT_ID,
        vendor_id="globalfreightpay",
        schema_doc=GLOBALFREIGHTPAY_SCHEMA_DOC,
        canonical_state="invoice.paid",
    )
    assert result.success
    ev = result.canonical_event
    assert ev.classification == Classification.INVOICE
    assert ev.event_type == InvoiceEventType.PAID
    assert ev.entity_external_id == "GFP-INV-2026-Q2-08821"
    assert ev.amount is not None
    assert ev.amount.currency == "EUR"
    assert ev.amount.amount_minor == 2_435_075
    assert ev.event_timestamp == datetime(2026, 4, 22, 16, 47, 11, tzinfo=UTC)


def test_globalfreightpay_issued(fixture_payloads):
    payload = fixture_payloads["04_globalfreightpay_issued"]
    result = adapter.extract(
        payload=payload,
        headers={},
        event_id=EVENT_ID,
        vendor_id="globalfreightpay",
        schema_doc=GLOBALFREIGHTPAY_SCHEMA_DOC,
        canonical_state="invoice.issued",
    )
    assert result.success
    ev = result.canonical_event
    assert ev.event_type == InvoiceEventType.ISSUED
    assert ev.entity_external_id == "GFP-INV-2026-Q2-08821"
    assert ev.amount.amount_minor == 2_435_075
    assert ev.due_at == datetime(2026, 5, 14, 22, 0, 0, tzinfo=UTC)


def test_marine_traffic_advisory_classifies_unclassified(fixture_payloads):
    payload = fixture_payloads["06_marine_traffic_advisory"]
    result = adapter.extract(
        payload=payload,
        headers={},
        event_id=EVENT_ID,
        vendor_id="marine_traffic_advisory",
        schema_doc=MARINE_TRAFFIC_SCHEMA_DOC,
        canonical_state=None,
    )
    assert result.success
    ev = result.canonical_event
    assert ev.classification == Classification.UNCLASSIFIED
    assert ev.summary and "Antwerp" in ev.summary


def test_schema_extraction_fails_gracefully_on_missing_fields():
    """When required paths don't resolve, extraction fails cleanly with details."""
    payload = {
        "carrier_scac": "MAEU",
        "transport_doc": {"number": "MAEU1"},
        "container": "MSKU1",
        # milestone_at is missing
    }
    result = adapter.extract(
        payload=payload,
        headers={},
        event_id=EVENT_ID,
        vendor_id="maersk",
        schema_doc=MAERSK_SCHEMA_DOC,
        canonical_state="shipment.in_transit",
    )
    assert not result.success
    assert "event_timestamp" in result.missing_fields


def test_schema_returns_raw_event_type_even_on_failure(fixture_payloads):
    """When canonical_state is None, extraction fails but returns raw_event_type for resolution."""
    payload = fixture_payloads["01_maersk_in_transit"]
    result = adapter.extract(
        payload=payload,
        headers={},
        event_id=EVENT_ID,
        vendor_id="maersk",
        schema_doc=MAERSK_SCHEMA_DOC,
        canonical_state=None,
    )
    assert not result.success
    assert result.raw_event_type == "Loaded onboard and sailed"
