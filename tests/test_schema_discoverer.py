"""Tests for schema discovery — reverse-mapping LLM output to schema_doc."""

import pytest

from app.adapters.schema_discoverer import SchemaDiscoverer


@pytest.fixture
def discoverer():
    return SchemaDiscoverer()


@pytest.fixture
def maersk_payload():
    return {
        "carrier_scac": "MAEU",
        "transport_doc": {"number": "MAEU240498712"},
        "container": "MSKU7748112",
        "milestone": "Loaded onboard and sailed",
        "milestone_at": "2026-04-28T14:30:00Z",
        "port": {"code": "SGSIN", "name": "Singapore"},
        "vessel": {"name": "Maersk Seletar"},
        "event_msg_id": "MSG-001",
    }


@pytest.fixture
def maersk_llm_output():
    return {
        "classification": "shipment",
        "vendor_id": "maersk",
        "confidence": 0.95,
        "entity_external_id": "MAEU240498712",
        "event_type": "shipment.in_transit",
        "event_timestamp": "2026-04-28T14:30:00Z",
        "raw_milestone": "Loaded onboard and sailed",
        "reference_ids": {
            "mbl_number": "MAEU240498712",
            "container": "MSKU7748112",
            "carrier_scac": "MAEU",
        },
        "location": {"code": "SGSIN", "name": "Singapore"},
    }


@pytest.fixture
def invoice_payload():
    return {
        "source": "globalfreightpay.api",
        "doc_ref": "GFP-INV-2026-Q2-08821",
        "carrier": "Maersk Line",
        "linked_bl": "MAEU240498712",
        "transaction": {
            "kind": "settled in full",
            "settled_at": "2026-05-02T10:00:00+02:00",
            "amount": "EUR 24.350,75",
            "due_at": "2026-05-29T09:00:00+02:00",
        },
    }


@pytest.fixture
def invoice_llm_output():
    return {
        "classification": "invoice",
        "vendor_id": "globalfreightpay",
        "confidence": 0.92,
        "entity_external_id": "GFP-INV-2026-Q2-08821",
        "event_type": "invoice.paid",
        "event_timestamp": "2026-05-02T10:00:00+02:00",
        "raw_kind": "settled in full",
        "amount": {"currency": "EUR", "amount_minor": 2435075},
        "due_at": "2026-05-29T09:00:00+02:00",
        "linked_references": {
            "carrier": "Maersk Line",
            "linked_bl": "MAEU240498712",
        },
    }


class TestSchemaInference:
    def test_infer_shipment_schema(self, discoverer, maersk_payload, maersk_llm_output):
        schema_doc = discoverer._infer_schema_doc(maersk_payload, maersk_llm_output, "shipment")

        assert schema_doc is not None
        assert schema_doc["classification"] == "shipment"
        fields = schema_doc["fields"]

        assert fields["entity_external_id"] == "$.transport_doc.number"
        assert fields["event_timestamp"] == "$.milestone_at"
        assert fields["event_type"] == "$.milestone"
        assert fields["raw_milestone"] == "$.milestone"
        assert fields["location"]["code"] == "$.port.code"
        assert fields["location"]["name"] == "$.port.name"
        assert fields["reference_ids"]["mbl_number"] == "$.transport_doc.number"
        assert fields["reference_ids"]["container"] == "$.container"
        assert fields["reference_ids"]["carrier_scac"] == "$.carrier_scac"

    def test_infer_invoice_schema(self, discoverer, invoice_payload, invoice_llm_output):
        schema_doc = discoverer._infer_schema_doc(invoice_payload, invoice_llm_output, "invoice")

        assert schema_doc is not None
        assert schema_doc["classification"] == "invoice"
        fields = schema_doc["fields"]

        assert fields["entity_external_id"] == "$.doc_ref"
        assert fields["event_timestamp"] == "$.transaction.settled_at"
        assert fields["event_type"] == "$.transaction.kind"
        assert fields["raw_kind"] == "$.transaction.kind"
        assert fields["due_at"] == "$.transaction.due_at"
        assert fields["amount"] == "$.transaction.amount"
        assert fields["linked_references"]["carrier"] == "$.carrier"
        assert fields["linked_references"]["linked_bl"] == "$.linked_bl"

    def test_infer_unclassified_schema(self, discoverer):
        payload = {
            "issuer": "marine-traffic-advisory",
            "subject": "Port congestion alert Singapore",
            "advisory_type": "operational",
        }
        llm_output = {
            "classification": "unclassified",
            "vendor_id": "marine_traffic",
            "confidence": 0.88,
            "summary": "Port congestion alert Singapore",
            "reason": "operational",
        }
        schema_doc = discoverer._infer_schema_doc(payload, llm_output, "unclassified")

        assert schema_doc is not None
        assert schema_doc["classification"] == "unclassified"
        assert schema_doc["fields"]["summary"] == "$.subject"
        assert schema_doc["fields"]["reason"] == "$.advisory_type"

    def test_infer_fails_without_required_fields(self, discoverer):
        payload = {"random": "data"}
        llm_output = {
            "classification": "shipment",
            "vendor_id": "test",
            "confidence": 0.5,
            "entity_external_id": "NOT_FOUND_IN_PAYLOAD",
            "event_type": "shipment.in_transit",
            "event_timestamp": "2026-01-01T00:00:00Z",
        }
        schema_doc = discoverer._infer_schema_doc(payload, llm_output, "shipment")
        # Can't find entity_external_id or event_timestamp in payload
        assert schema_doc is None


class TestSchemaValidation:
    def test_validate_shipment_schema(self, discoverer, maersk_payload, maersk_llm_output):
        schema_doc = discoverer._infer_schema_doc(maersk_payload, maersk_llm_output, "shipment")
        assert schema_doc is not None

        valid = discoverer._validate_schema(
            maersk_payload, "test-event", "maersk", schema_doc, maersk_llm_output
        )
        assert valid

    def test_validate_invoice_schema(self, discoverer, invoice_payload, invoice_llm_output):
        schema_doc = discoverer._infer_schema_doc(invoice_payload, invoice_llm_output, "invoice")
        assert schema_doc is not None

        valid = discoverer._validate_schema(
            invoice_payload, "test-event", "globalfreightpay", schema_doc, invoice_llm_output
        )
        assert valid


class TestValuePathFinding:
    def test_find_nested_value(self, discoverer):
        payload = {"a": {"b": {"c": "target"}}}
        path = discoverer._find_value_path(payload, "target")
        assert path == "$.a.b.c"

    def test_find_top_level_value(self, discoverer):
        payload = {"name": "hello", "nested": {"also": "hello"}}
        path = discoverer._find_value_path(payload, "hello")
        # Should prefer shorter path
        assert path == "$.name"

    def test_find_returns_none_for_missing(self, discoverer):
        payload = {"a": 1, "b": 2}
        path = discoverer._find_value_path(payload, "not_here")
        assert path is None

    def test_find_numeric_value(self, discoverer):
        payload = {"data": {"count": 42}}
        path = discoverer._find_value_path(payload, 42)
        assert path == "$.data.count"
