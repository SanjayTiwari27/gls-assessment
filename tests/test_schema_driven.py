"""Tests for the schema-driven adapter (reverse-mapping format)."""

from datetime import UTC, datetime

import pytest

from app.adapters.schema_driven import SchemaDrivenAdapter


@pytest.fixture
def adapter():
    return SchemaDrivenAdapter()


@pytest.fixture
def maersk_schema_doc():
    return {
        "classification": "shipment",
        "fields": {
            "entity_external_id": {"template": "{transport_doc.number}:{container}"},
            "event_type": "$.milestone",
            "event_timestamp": "$.milestone_at",
            "raw_milestone": "$.milestone",
            "reference_ids": {
                "mbl_number": "$.transport_doc.number",
                "container": "$.container",
                "carrier_scac": "$.carrier_scac",
            },
            "location": {
                "code": "$.port.code",
                "name": "$.port.name",
            },
        },
    }


@pytest.fixture
def maersk_payload():
    return {
        "carrier_scac": "MAEU",
        "transport_doc": {"number": "MAEU240498712"},
        "container": "MSKU7748112",
        "milestone": "Loaded onboard",
        "milestone_at": "2026-04-28T14:30:00Z",
        "port": {"code": "SGSIN", "name": "Singapore"},
        "vessel": {"name": "Maersk Seletar", "imo": "9876543", "voyage": "426E"},
        "event_msg_id": "MSG-SG-20260428-0042",
    }


@pytest.fixture
def invoice_schema_doc():
    return {
        "classification": "invoice",
        "fields": {
            "entity_external_id": "$.doc_ref",
            "event_type": "$.transaction.kind",
            "event_timestamp": "$.transaction.settled_at",
            "raw_kind": "$.transaction.kind",
            "amount": "$.transaction.amount",
            "due_at": "$.transaction.due_at",
            "linked_references": {
                "carrier": "$.carrier",
                "linked_bl": "$.linked_bl",
            },
        },
    }


@pytest.fixture
def invoice_payload():
    return {
        "source": "globalfreightpay.api",
        "doc_ref": "GFP-INV-2026-Q2-08821",
        "carrier": "Maersk Line",
        "linked_bl": "MAEU240498712",
        "transaction": {
            "kind": "Freight invoice raised",
            "issued_at": "2026-04-29T09:00:00+02:00",
            "settled_at": "2026-05-02T10:00:00+02:00",
            "amount": "EUR 24.350,75",
            "due_at": "2026-05-29T09:00:00+02:00",
        },
    }


class TestSchemaDrivenShipment:
    def test_extract_shipment_success(self, adapter, maersk_schema_doc, maersk_payload):
        result = adapter.extract(
            payload=maersk_payload,
            headers={},
            event_id="test-event-001",
            vendor_id="maersk",
            schema_doc=maersk_schema_doc,
            canonical_state="shipment.in_transit",
        )
        assert result.success
        ev = result.canonical_event
        assert ev.vendor_id == "maersk"
        assert ev.entity_external_id == "MAEU240498712:MSKU7748112"
        assert ev.event_type.value == "shipment.in_transit"
        assert ev.event_timestamp == datetime(2026, 4, 28, 14, 30, tzinfo=UTC)
        assert ev.location.code == "SGSIN"
        assert ev.location.name == "Singapore"
        assert ev.reference_ids["mbl_number"] == "MAEU240498712"
        assert ev.reference_ids["container"] == "MSKU7748112"
        assert ev.raw_milestone == "Loaded onboard"

    def test_extract_shipment_missing_timestamp(self, adapter, maersk_schema_doc):
        payload = {
            "carrier_scac": "MAEU",
            "transport_doc": {"number": "MAEU240498712"},
            "container": "MSKU7748112",
            "milestone": "Loaded",
        }
        result = adapter.extract(
            payload=payload,
            headers={},
            event_id="test-event-002",
            vendor_id="maersk",
            schema_doc=maersk_schema_doc,
            canonical_state="shipment.in_transit",
        )
        assert not result.success
        assert "event_timestamp" in result.missing_fields

    def test_extract_returns_raw_event_type(self, adapter, maersk_schema_doc, maersk_payload):
        result = adapter.extract(
            payload=maersk_payload,
            headers={},
            event_id="test-event-003",
            vendor_id="maersk",
            schema_doc=maersk_schema_doc,
            canonical_state="shipment.in_transit",
        )
        assert result.raw_event_type == "Loaded onboard"

    def test_extract_without_canonical_state_returns_raw_event_type(
        self, adapter, maersk_schema_doc, maersk_payload
    ):
        result = adapter.extract(
            payload=maersk_payload,
            headers={},
            event_id="test-event-004",
            vendor_id="maersk",
            schema_doc=maersk_schema_doc,
            canonical_state=None,
        )
        assert not result.success
        assert result.raw_event_type == "Loaded onboard"
        assert "canonical_state not resolved" in result.error


class TestSchemaDrivenInvoice:
    def test_extract_invoice_success(self, adapter, invoice_schema_doc, invoice_payload):
        result = adapter.extract(
            payload=invoice_payload,
            headers={},
            event_id="test-event-010",
            vendor_id="globalfreightpay",
            schema_doc=invoice_schema_doc,
            canonical_state="invoice.paid",
        )
        assert result.success
        ev = result.canonical_event
        assert ev.vendor_id == "globalfreightpay"
        assert ev.entity_external_id == "GFP-INV-2026-Q2-08821"
        assert ev.event_type.value == "invoice.paid"
        assert ev.amount.currency == "EUR"
        assert ev.amount.amount_minor == 2_435_075
        assert ev.linked_references["carrier"] == "Maersk Line"
        assert ev.linked_references["linked_bl"] == "MAEU240498712"

    def test_extract_invoice_missing_entity_id(self, adapter, invoice_schema_doc):
        payload = {
            "source": "globalfreightpay.api",
            "transaction": {
                "kind": "Freight invoice raised",
                "settled_at": "2026-05-02T10:00:00+02:00",
                "amount": "EUR 100,00",
            },
        }
        result = adapter.extract(
            payload=payload,
            headers={},
            event_id="test-event-011",
            vendor_id="globalfreightpay",
            schema_doc=invoice_schema_doc,
            canonical_state="invoice.issued",
        )
        assert not result.success
        assert "entity_external_id" in result.missing_fields

    def test_extract_invoice_fallback_timestamp(self, adapter, invoice_payload):
        """Test fallback list for event_timestamp."""
        schema_doc = {
            "classification": "invoice",
            "fields": {
                "entity_external_id": "$.doc_ref",
                "event_type": "$.transaction.kind",
                "event_timestamp": [
                    "$.transaction.settled_at",
                    "$.transaction.issued_at",
                ],
                "amount": "$.transaction.amount",
            },
        }
        result = adapter.extract(
            payload=invoice_payload,
            headers={},
            event_id="test-event-012",
            vendor_id="globalfreightpay",
            schema_doc=schema_doc,
            canonical_state="invoice.paid",
        )
        assert result.success
        # settled_at is first in list and present → used
        assert result.canonical_event.event_timestamp.day == 2

    def test_extract_invoice_fallback_skips_null(self, adapter):
        """When first path is null, falls to second."""
        schema_doc = {
            "classification": "invoice",
            "fields": {
                "entity_external_id": "$.doc_ref",
                "event_type": "$.transaction.kind",
                "event_timestamp": [
                    "$.transaction.settled_at",
                    "$.transaction.issued_at",
                ],
                "amount": "$.transaction.amount",
            },
        }
        payload = {
            "doc_ref": "INV-001",
            "transaction": {
                "kind": "issued",
                "issued_at": "2026-04-29T09:00:00+02:00",
                "amount": "USD 100.00",
            },
        }
        result = adapter.extract(
            payload=payload,
            headers={},
            event_id="test-event-013",
            vendor_id="test",
            schema_doc=schema_doc,
            canonical_state="invoice.issued",
        )
        assert result.success
        assert result.canonical_event.event_timestamp.day == 29


class TestSchemaDrivenUnclassified:
    def test_extract_unclassified(self, adapter):
        schema_doc = {
            "classification": "unclassified",
            "fields": {
                "summary": "$.subject",
                "reason": "$.advisory_type",
            },
        }
        payload = {
            "issuer": "marine-traffic-advisory",
            "subject": "Port congestion alert",
            "advisory_type": "operational_notice",
        }
        result = adapter.extract(
            payload=payload,
            headers={},
            event_id="test-event-020",
            vendor_id="marine_traffic_advisory",
            schema_doc=schema_doc,
            canonical_state=None,
        )
        assert result.success
        assert result.canonical_event.summary == "Port congestion alert"
        assert result.canonical_event.reason == "operational_notice"
        assert result.canonical_event.vendor_id == "marine_traffic_advisory"

    def test_extract_unclassified_default_reason(self, adapter):
        schema_doc = {
            "classification": "unclassified",
            "fields": {
                "summary": "$.subject",
            },
        }
        payload = {"subject": "Weather advisory"}
        result = adapter.extract(
            payload=payload,
            headers={},
            event_id="test-event-021",
            vendor_id="test",
            schema_doc=schema_doc,
            canonical_state=None,
        )
        assert result.success
        assert result.canonical_event.reason == "schema_driven_unclassified"


class TestFieldResolution:
    def test_nested_path(self, adapter):
        payload = {"a": {"b": {"c": "deep"}}}
        assert adapter._resolve_path(payload, "$.a.b.c") == "deep"

    def test_array_index(self, adapter):
        payload = {"events": [{"code": "LO"}, {"code": "DI"}]}
        assert adapter._resolve_path(payload, "$.events[0].code") == "LO"
        assert adapter._resolve_path(payload, "$.events[1].code") == "DI"

    def test_array_bare(self, adapter):
        payload = {"items": [{"name": "first"}, {"name": "second"}]}
        assert adapter._resolve_path(payload, "$.items[].name") == "first"

    def test_missing_path_returns_none(self, adapter):
        payload = {"a": 1}
        assert adapter._resolve_path(payload, "$.b.c") is None

    def test_none_path_returns_none(self, adapter):
        assert adapter._resolve_path({"a": 1}, None) is None

    def test_entity_id_template(self, adapter):
        payload = {"transport_doc": {"number": "MBL123"}, "container": "CTR456"}
        fields = {"entity_external_id": {"template": "{transport_doc.number}:{container}"}}
        result = adapter._resolve_entity_id(payload, fields)
        assert result == "MBL123:CTR456"

    def test_entity_id_template_partial(self, adapter):
        payload = {"transport_doc": {"number": "MBL123"}}
        fields = {"entity_external_id": {"template": "{transport_doc.number}:{container}"}}
        result = adapter._resolve_entity_id(payload, fields)
        assert result == "MBL123"

    def test_entity_id_direct_path(self, adapter):
        payload = {"doc_ref": "INV-001"}
        fields = {"entity_external_id": "$.doc_ref"}
        result = adapter._resolve_entity_id(payload, fields)
        assert result == "INV-001"

    def test_fallback_list_resolution(self, adapter):
        payload = {"b": "second"}
        assert adapter._resolve_field(payload, ["$.a", "$.b"]) == "second"

    def test_fallback_list_first_wins(self, adapter):
        payload = {"a": "first", "b": "second"}
        assert adapter._resolve_field(payload, ["$.a", "$.b"]) == "first"
