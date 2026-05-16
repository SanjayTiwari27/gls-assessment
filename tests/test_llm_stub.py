"""StubLLM is the offline default. It must classify the appendix payloads
the same way the deterministic adapters would, so the system is fully
demonstrable end-to-end without an LLM key.
"""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import orjson
import pytest

from app.hashing import canonical_json
from app.llm.stub import StubLLM

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[1] / "app" / "llm" / "schemas" / "v1_target_schema.json").read_text()
)


def _make_prompt(payload: dict) -> str:
    return f"Vendor payload:\n{canonical_json(payload).decode('utf-8')}"


async def _run(payload: dict) -> dict:
    res = await StubLLM().complete(prompt=_make_prompt(payload), schema=SCHEMA)
    return orjson.loads(res.text)


@pytest.mark.asyncio
async def test_stub_output_validates_against_schema(fixture_payloads):
    for payload in fixture_payloads.values():
        out = await _run(payload)
        jsonschema.validate(out, SCHEMA)


@pytest.mark.asyncio
async def test_stub_classifies_maersk_in_transit(fixture_payloads):
    out = await _run(fixture_payloads["01_maersk_in_transit"])
    assert out["classification"] == "shipment"
    assert out["event_type"] == "shipment.in_transit"
    assert out["entity_external_id"] == "MAEU240498712:MSKU7748112"
    assert out["event_timestamp"].endswith("+00:00")


@pytest.mark.asyncio
async def test_stub_classifies_maersk_picked_up(fixture_payloads):
    out = await _run(fixture_payloads["02_maersk_picked_up"])
    assert out["classification"] == "shipment"
    assert out["event_type"] == "shipment.picked_up"


@pytest.mark.asyncio
async def test_stub_classifies_one_delivered(fixture_payloads):
    out = await _run(fixture_payloads["05_one_delivered"])
    assert out["classification"] == "shipment"
    assert out["event_type"] == "shipment.delivered"
    assert out["entity_external_id"] == "ONEYJKTHKG2604113:TLLU2890442"


@pytest.mark.asyncio
async def test_stub_classifies_invoice_paid(fixture_payloads):
    out = await _run(fixture_payloads["03_globalfreightpay_paid"])
    assert out["classification"] == "invoice"
    assert out["event_type"] == "invoice.paid"
    assert out["amount"] == {"currency": "EUR", "amount_minor": 2_435_075}


@pytest.mark.asyncio
async def test_stub_classifies_invoice_issued(fixture_payloads):
    out = await _run(fixture_payloads["04_globalfreightpay_issued"])
    assert out["classification"] == "invoice"
    assert out["event_type"] == "invoice.issued"


@pytest.mark.asyncio
async def test_stub_classifies_marine_traffic_unclassified(fixture_payloads):
    out = await _run(fixture_payloads["06_marine_traffic_advisory"])
    assert out["classification"] == "unclassified"
    assert "Antwerp" in (out.get("summary") or "")


@pytest.mark.asyncio
async def test_stub_returns_unclassified_for_random_payload():
    out = await _run({"foo": "bar", "baz": [1, 2, 3]})
    assert out["classification"] == "unclassified"
    assert out["confidence"] <= 0.7
