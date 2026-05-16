"""GlobalFreightPay (carrier billing) v1 adapter."""

from __future__ import annotations

from typing import Any

from app.adapters.base import AdapterResult
from app.adapters.parsers import parse_money, parse_timestamp
from app.domain.canonical import (
    CanonicalInvoiceEvent,
    InvoiceEventType,
    Money,
    Source,
)

_KIND_KEYWORDS: list[tuple[InvoiceEventType, tuple[str, ...]]] = [
    (
        InvoiceEventType.REFUNDED,
        ("refund", "reversed", "reversal", "credit note"),
    ),
    (
        InvoiceEventType.VOIDED,
        ("voided", "cancelled", "canceled", "annulled"),
    ),
    (
        InvoiceEventType.PAID,
        (
            "settled in full",
            "settled",
            "paid in full",
            "payment received",
            "remitted",
            "payment confirmed",
        ),
    ),
    (
        InvoiceEventType.ISSUED,
        (
            "freight invoice raised",
            "invoice raised",
            "invoice issued",
            "issued",
            "billed",
            "raised",
        ),
    ),
]


def _classify(text: str) -> InvoiceEventType | None:
    if not text:
        return None
    haystack = text.lower()
    for event_type, keywords in _KIND_KEYWORDS:
        if any(kw in haystack for kw in keywords):
            return event_type
    return None


class GlobalFreightPayV1Adapter:
    vendor_id = "globalfreightpay"
    schema_version = "globalfreightpay_v1"

    def matches(self, payload: dict[str, Any], headers: dict[str, Any]) -> bool:
        return str(payload.get("source", "")).lower() == "globalfreightpay.api"

    def normalize(
        self,
        payload: dict[str, Any],
        headers: dict[str, Any],
        event_id: str,
    ) -> AdapterResult:
        doc_ref = payload.get("doc_ref")
        transaction = payload.get("transaction") or {}
        if not isinstance(transaction, dict):
            return AdapterResult(
                status="needs_llm",
                missing_fields=["transaction"],
                schema_version=self.schema_version,
            )

        kind = transaction.get("kind")
        ts_raw = (
            transaction.get("settled_at")
            or transaction.get("issued_at")
            or transaction.get("voided_at")
            or transaction.get("refunded_at")
            or transaction.get("event_at")
        )
        amount_raw = transaction.get("amount")

        missing: list[str] = []
        if not doc_ref:
            missing.append("doc_ref")
        if not kind:
            missing.append("transaction.kind")
        if not ts_raw:
            missing.append("transaction.timestamp")
        if missing:
            return AdapterResult(
                status="needs_llm",
                missing_fields=missing,
                schema_version=self.schema_version,
            )

        event_type = _classify(str(kind))
        if event_type is None:
            return AdapterResult(
                status="needs_llm",
                confidence=0.4,
                missing_fields=["event_type"],
                schema_version=self.schema_version,
                detail={"raw_kind": kind},
            )

        try:
            ts = parse_timestamp(str(ts_raw))
        except (ValueError, TypeError):
            return AdapterResult(
                status="needs_llm",
                missing_fields=["event_timestamp"],
                schema_version=self.schema_version,
            )

        amount: Money | None = None
        if amount_raw:
            try:
                currency, amount_minor = parse_money(str(amount_raw))
                amount = Money(currency=currency, amount_minor=amount_minor)
            except ValueError:
                # Amount unparseable — leave as None and let the LLM fill it
                # in later if the field becomes critical for downstream.
                pass

        due_at = None
        due_raw = transaction.get("due_at")
        if due_raw:
            try:
                due_at = parse_timestamp(str(due_raw))
            except (ValueError, TypeError):
                due_at = None

        line_items = (
            transaction.get("line_items") if isinstance(transaction.get("line_items"), list) else None
        )

        linked_references = {
            "carrier": payload.get("carrier"),
            "linked_bl": payload.get("linked_bl"),
            "channel": payload.get("channel"),
            "remitter": transaction.get("remitter"),
            "memo": transaction.get("memo"),
            "line_items": line_items,
        }
        linked_references = {k: v for k, v in linked_references.items() if v}

        canonical = CanonicalInvoiceEvent(
            event_id=event_id,
            vendor_id=self.vendor_id,
            entity_external_id=str(doc_ref),
            event_type=event_type,
            event_timestamp=ts,
            amount=amount,
            due_at=due_at,
            linked_references=linked_references,
            raw_kind=str(kind),
            schema_version=self.schema_version,
            source=Source.DETERMINISTIC,
            confidence=0.95,
        )

        return AdapterResult(
            status="ok",
            canonical_event=canonical,
            confidence=0.95,
            schema_version=self.schema_version,
        )
