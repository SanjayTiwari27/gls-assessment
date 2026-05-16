"""StubLLM — a deterministic, key-free LLM provider for offline runs.

This is the default provider so reviewers can run the full system end-to-end
without needing to plumb an OpenAI key. It does not invent semantics: it
recognizes the same fingerprints the deterministic adapters do, plus a few
broader signals, and emits the canonical extraction directly.

For unknown payloads it returns ``classification=unclassified`` with a low
confidence — this is what a real LLM would (and should) do when the input is
ambiguous.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

import orjson

from app.adapters.parsers import parse_money, parse_timestamp
from app.llm.provider import LLMResult


class StubLLM:
    name = "stub"
    model = "stub-deterministic-v1"

    async def complete(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        temperature: float = 0.0,
    ) -> LLMResult:
        started = time.perf_counter()
        payload = _extract_payload_from_prompt(prompt)
        result = _classify_extract(payload)
        text = orjson.dumps(result).decode("utf-8")
        latency_ms = int((time.perf_counter() - started) * 1000)
        return LLMResult(
            text=text,
            model=self.model,
            tokens_in=len(prompt) // 4,   # rough heuristic, audit-only
            tokens_out=len(text) // 4,
            latency_ms=latency_ms,
            cost_estimate=0.0,
        )


def _extract_payload_from_prompt(prompt: str) -> dict[str, Any]:
    # Our prompt template ends with "Vendor payload:\n{{PAYLOAD}}", so the
    # payload is everything after the marker. The orchestrator interpolates
    # the JSON in place of {{PAYLOAD}}.
    marker = "Vendor payload:"
    idx = prompt.rfind(marker)
    if idx == -1:
        return {}
    blob = prompt[idx + len(marker) :].strip()
    try:
        parsed = orjson.loads(blob)
    except orjson.JSONDecodeError:
        # Some payloads might have trailing whitespace or partial trims.
        try:
            parsed = json.loads(blob)
        except json.JSONDecodeError:
            return {}
    return parsed if isinstance(parsed, dict) else {}


def _classify_extract(payload: dict[str, Any]) -> dict[str, Any]:
    """Tiny rule engine that produces the canonical extraction schema.

    Order: invoice signals → shipment signals → unclassified. The shape of the
    return matches v1_target_schema.json exactly so that the orchestrator can
    validate it without surprises.
    """

    if _looks_like_invoice(payload):
        return _extract_invoice(payload)

    if _looks_like_shipment(payload):
        return _extract_shipment(payload)

    return _unclassified(payload)


# --------------------------------------------------------------------------- #
# Heuristic fingerprints
# --------------------------------------------------------------------------- #

def _looks_like_invoice(p: dict[str, Any]) -> bool:
    if str(p.get("source", "")).lower() == "globalfreightpay.api":
        return True
    if "transaction" in p and isinstance(p["transaction"], dict):
        kind = str(p["transaction"].get("kind", "")).lower()
        if any(kw in kind for kw in ("invoice", "settle", "paid", "raised", "refund", "voided")):
            return True
    return any(k in p for k in ("invoice_number", "invoice_id", "doc_ref", "amount_due"))


def _looks_like_shipment(p: dict[str, Any]) -> bool:
    if "carrier_scac" in p or "carrier_code" in p:
        return True
    carrier = str(p.get("carrier", "")).lower()
    if "ocean network express" in carrier or "maersk" in carrier or "hapag" in carrier:
        return True
    if any(k in p for k in ("container", "container_no", "tracking_number")):
        return True
    if "milestone" in p or "milestone_text" in p or "status" in p:
        return True
    return False


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #

_SHIP_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("shipment.delivered", ("released to consignee", "delivered", "package handed", "empty container returned")),
    ("shipment.out_for_delivery", ("out for delivery", "delivery scheduled")),
    ("shipment.in_transit", ("loaded onboard", "sailed", "vessel arrived", "discharged", "transhipment", "in transit")),
    ("shipment.picked_up", ("empty container released to shipper", "received at origin", "container received", "gate-in", "gate in", "picked up")),
    ("shipment.exception", ("exception", "delay", "rolled", "blocked", "hold")),
    ("shipment.cancelled", ("cancelled", "canceled", "voided shipment")),
]

_INV_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("invoice.refunded", ("refund", "reversed", "reversal", "credit note")),
    ("invoice.voided", ("voided", "cancelled", "canceled", "annulled")),
    ("invoice.paid", ("settled in full", "settled", "paid in full", "payment received", "remitted", "payment confirmed")),
    ("invoice.issued", ("freight invoice raised", "invoice raised", "invoice issued", "issued", "billed", "raised")),
]


def _classify_text(text: str, table: list[tuple[str, tuple[str, ...]]]) -> str | None:
    haystack = (text or "").lower()
    for label, kws in table:
        if any(kw in haystack for kw in kws):
            return label
    return None


def _extract_shipment(p: dict[str, Any]) -> dict[str, Any]:
    vendor_id = _shipment_vendor_id(p)
    transport = p.get("transport_doc") or {}
    mbl = transport.get("number") if isinstance(transport, dict) else None
    house_bl = p.get("house_bl")
    master_bl = p.get("master_bl")
    container = p.get("container") or p.get("container_no")
    tracking = p.get("tracking_number")

    primary = mbl or house_bl or master_bl or tracking
    if primary and container:
        ext_id = f"{primary}:{container}"
    else:
        ext_id = primary or container

    milestone = p.get("milestone") or p.get("milestone_text") or p.get("status") or ""
    event_type = _classify_text(str(milestone), _SHIP_KEYWORDS)

    ts_raw = (
        p.get("milestone_at")
        or p.get("milestone_local_time")
        or p.get("event_time")
        or p.get("timestamp")
    )
    event_ts: str | None = None
    if ts_raw:
        try:
            event_ts = parse_timestamp(str(ts_raw)).isoformat()
        except Exception:  # noqa: BLE001
            event_ts = None

    location: dict[str, Any] | None = None
    port = p.get("port") if isinstance(p.get("port"), dict) else None
    if port:
        location = {"code": port.get("code"), "name": port.get("name"), "latitude": None, "longitude": None}
    elif p.get("port_of_discharge") or p.get("port_of_loading"):
        location = {
            "code": p.get("port_of_discharge") or p.get("port_of_loading"),
            "name": None, "latitude": None, "longitude": None,
        }

    reference_ids = {
        k: v for k, v in {
            "mbl_number": mbl,
            "house_bl": house_bl,
            "master_bl": master_bl,
            "container": container,
            "tracking_number": tracking,
            "carrier_scac": p.get("carrier_scac"),
            "vendor_event_id": p.get("event_msg_id") or p.get("event_id"),
            "vessel": p.get("vessel"),
            "shipper_ref": p.get("shipper_ref"),
            "consignee": p.get("consignee"),
            "delivery_order_no": p.get("delivery_order_no"),
        }.items() if v
    }

    confidence = 0.85 if (ext_id and event_type and event_ts) else 0.4

    return {
        "classification": "shipment",
        "vendor_id": vendor_id,
        "confidence": confidence,
        "entity_external_id": ext_id,
        "event_type": event_type,
        "event_timestamp": event_ts,
        "amount": None,
        "due_at": None,
        "reference_ids": reference_ids,
        "linked_references": None,
        "location": location,
        "raw_milestone": str(milestone) if milestone else None,
        "raw_kind": None,
        "summary": None,
        "reason": None,
    }


def _extract_invoice(p: dict[str, Any]) -> dict[str, Any]:
    vendor_id = _invoice_vendor_id(p)
    doc_ref = p.get("doc_ref") or p.get("invoice_id") or p.get("invoice_number")
    transaction = p.get("transaction") if isinstance(p.get("transaction"), dict) else {}
    kind = transaction.get("kind") or p.get("kind") or ""
    event_type = _classify_text(str(kind), _INV_KEYWORDS)

    ts_raw = (
        transaction.get("settled_at")
        or transaction.get("issued_at")
        or transaction.get("voided_at")
        or transaction.get("refunded_at")
        or p.get("issued_at")
    )
    event_ts: str | None = None
    if ts_raw:
        try:
            event_ts = parse_timestamp(str(ts_raw)).isoformat()
        except Exception:  # noqa: BLE001
            event_ts = None

    amount: dict[str, Any] | None = None
    amount_raw = transaction.get("amount") or p.get("amount")
    if amount_raw:
        try:
            ccy, minor = parse_money(str(amount_raw))
            amount = {"currency": ccy, "amount_minor": minor}
        except ValueError:
            amount = None

    due_at: str | None = None
    due_raw = transaction.get("due_at") or p.get("due_at")
    if due_raw:
        try:
            due_at = parse_timestamp(str(due_raw)).isoformat()
        except Exception:  # noqa: BLE001
            due_at = None

    line_items = transaction.get("line_items") if isinstance(transaction.get("line_items"), list) else None

    linked_references = {
        k: v for k, v in {
            "carrier": p.get("carrier"),
            "linked_bl": p.get("linked_bl"),
            "channel": p.get("channel"),
            "remitter": transaction.get("remitter"),
            "memo": transaction.get("memo"),
            "line_items": line_items,
        }.items() if v
    }

    confidence = 0.85 if (doc_ref and event_type and event_ts) else 0.4

    return {
        "classification": "invoice",
        "vendor_id": vendor_id,
        "confidence": confidence,
        "entity_external_id": str(doc_ref) if doc_ref else None,
        "event_type": event_type,
        "event_timestamp": event_ts,
        "amount": amount,
        "due_at": due_at,
        "reference_ids": None,
        "linked_references": linked_references or None,
        "location": None,
        "raw_milestone": None,
        "raw_kind": str(kind) if kind else None,
        "summary": None,
        "reason": None,
    }


def _unclassified(p: dict[str, Any]) -> dict[str, Any]:
    summary = p.get("subject") or p.get("body") or p.get("description")
    if isinstance(summary, str):
        summary = re.sub(r"\s+", " ", summary).strip()[:500] or None
    else:
        summary = None
    return {
        "classification": "unclassified",
        "vendor_id": _generic_vendor_id(p),
        "confidence": 0.6,
        "entity_external_id": None,
        "event_type": None,
        "event_timestamp": None,
        "amount": None,
        "due_at": None,
        "reference_ids": None,
        "linked_references": None,
        "location": None,
        "raw_milestone": None,
        "raw_kind": None,
        "summary": summary,
        "reason": "no_shipment_or_invoice_signals_detected",
    }


def _shipment_vendor_id(p: dict[str, Any]) -> str:
    if str(p.get("carrier_scac", "")).upper() == "MAEU":
        return "maersk"
    if str(p.get("carrier_scac", "")).upper() == "ONEY":
        return "ocean_network_express"
    carrier = str(p.get("carrier", "")).strip()
    if carrier:
        return _slug(carrier)
    return "unknown_shipment_vendor"


def _invoice_vendor_id(p: dict[str, Any]) -> str:
    src = str(p.get("source", "")).lower()
    if "globalfreightpay" in src:
        return "globalfreightpay"
    return _slug(str(p.get("source") or p.get("issuer") or "unknown_invoice_vendor"))


def _generic_vendor_id(p: dict[str, Any]) -> str:
    issuer = p.get("issuer") or p.get("source") or "unknown"
    return _slug(str(issuer))


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "unknown"
