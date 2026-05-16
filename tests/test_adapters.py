"""Pure unit tests for vendor adapters using the appendix payload fixtures."""

from datetime import UTC, datetime

from app.adapters.globalfreightpay_v1 import GlobalFreightPayV1Adapter
from app.adapters.maersk_v1 import MaerskV1Adapter
from app.adapters.marine_traffic_v1 import MarineTrafficV1Adapter
from app.adapters.one_v1 import OneV1Adapter
from app.adapters.registry import AdapterRegistry
from app.adapters.registry import registry as default_registry
from app.domain.canonical import (
    Classification,
    InvoiceEventType,
    ShipmentEventType,
    Source,
)

EVENT_ID = "test-event-id"


def test_maersk_in_transit(fixture_payloads):
    payload = fixture_payloads["01_maersk_in_transit"]
    adapter = MaerskV1Adapter()
    assert adapter.matches(payload, {})

    result = adapter.normalize(payload, {}, EVENT_ID)
    assert result.status == "ok"
    ev = result.canonical_event
    assert ev is not None
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
    result = MaerskV1Adapter().normalize(payload, {}, EVENT_ID)
    assert result.status == "ok"
    ev = result.canonical_event
    assert ev.event_type == ShipmentEventType.PICKED_UP
    assert ev.entity_external_id == "MAEU240498712:MSKU7748112"
    # Both Maersk fixtures land on the same shipment entity.
    assert ev.event_timestamp < datetime(2026, 4, 21, tzinfo=UTC)


def test_maersk_does_not_match_other_carriers():
    assert not MaerskV1Adapter().matches({"carrier_scac": "ONEY"}, {})
    assert not MaerskV1Adapter().matches({}, {})


def test_one_delivered(fixture_payloads):
    payload = fixture_payloads["05_one_delivered"]
    adapter = OneV1Adapter()
    assert adapter.matches(payload, {})

    result = adapter.normalize(payload, {}, EVENT_ID)
    assert result.status == "ok"
    ev = result.canonical_event
    assert ev.event_type == ShipmentEventType.DELIVERED
    assert ev.entity_external_id == "ONEYJKTHKG2604113:TLLU2890442"
    assert ev.event_timestamp == datetime(2026, 4, 28, 2, 42, 0, tzinfo=UTC)
    assert ev.location is not None and ev.location.code == "IDJKT"


def test_globalfreightpay_paid(fixture_payloads):
    payload = fixture_payloads["03_globalfreightpay_paid"]
    adapter = GlobalFreightPayV1Adapter()
    assert adapter.matches(payload, {})

    result = adapter.normalize(payload, {}, EVENT_ID)
    assert result.status == "ok"
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
    result = GlobalFreightPayV1Adapter().normalize(payload, {}, EVENT_ID)
    assert result.status == "ok"
    ev = result.canonical_event
    assert ev.event_type == InvoiceEventType.ISSUED
    assert ev.entity_external_id == "GFP-INV-2026-Q2-08821"
    assert ev.amount.amount_minor == 2_435_075
    assert ev.due_at == datetime(2026, 5, 14, 22, 0, 0, tzinfo=UTC)


def test_marine_traffic_advisory_classifies_unclassified(fixture_payloads):
    payload = fixture_payloads["06_marine_traffic_advisory"]
    adapter = MarineTrafficV1Adapter()
    assert adapter.matches(payload, {})

    result = adapter.normalize(payload, {}, EVENT_ID)
    assert result.status == "ok"
    ev = result.canonical_event
    assert ev.classification == Classification.UNCLASSIFIED
    assert ev.summary and "Antwerp" in ev.summary


def test_registry_resolves_each_appendix_payload(fixture_payloads):
    expected = {
        "01_maersk_in_transit":          "maersk",
        "02_maersk_picked_up":           "maersk",
        "03_globalfreightpay_paid":      "globalfreightpay",
        "04_globalfreightpay_issued":    "globalfreightpay",
        "05_one_delivered":              "ocean_network_express",
        "06_marine_traffic_advisory":    "marine_traffic_advisory",
    }
    registry = AdapterRegistry()
    for name, vendor in expected.items():
        adapter = registry.resolve(fixture_payloads[name], {})
        assert adapter is not None, f"no adapter matched {name}"
        assert adapter.vendor_id == vendor


def test_registry_returns_none_for_unknown_payload():
    assert default_registry.resolve({"random": "shape"}, {}) is None


def test_adapter_falls_through_to_llm_when_milestone_missing():
    payload = {
        "carrier_scac": "MAEU",
        "transport_doc": {"number": "MAEU1"},
        "container": "MSKU1",
        "milestone_at": "2026-04-21T22:47:00+08:00",
        # milestone is missing
    }
    result = MaerskV1Adapter().normalize(payload, {}, EVENT_ID)
    assert result.status == "needs_llm"
    assert "milestone" in result.missing_fields


def test_adapter_falls_through_to_llm_when_milestone_unrecognized():
    payload = {
        "carrier_scac": "MAEU",
        "transport_doc": {"number": "MAEU1"},
        "container": "MSKU1",
        "milestone": "Some weird internal vendor jargon nobody mapped",
        "milestone_at": "2026-04-21T22:47:00+08:00",
    }
    result = MaerskV1Adapter().normalize(payload, {}, EVENT_ID)
    assert result.status == "needs_llm"
    assert "event_type" in result.missing_fields
