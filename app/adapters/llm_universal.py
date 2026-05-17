"""Universal LLM-backed adapter.

Used when no deterministic adapter recognizes the payload. The adapter
itself is *not* a pure function — it talks to ``LLMFallback`` — but it keeps
the same interface so the worker pipeline does not branch on adapter kind.

The orchestration responsibility (cache/budget/validate) lives in
``LLMFallback``. This class only:
  - calls the orchestrator,
  - shapes the validated dict into a strict canonical pydantic model,
  - returns ``unsupported`` if validation still fails.
"""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from app.adapters.base import AdapterResult
from app.adapters.parsers import parse_timestamp
from app.domain.canonical import (
    CanonicalEvent,
    CanonicalInvoiceEvent,
    CanonicalShipmentEvent,
    CanonicalUnclassifiedEvent,
    InvoiceEventType,
    Location,
    Money,
    ShipmentEventType,
    Source,
)
from app.llm.fallback import BudgetExceeded, LLMFallback, LLMValidationError


class LLMUniversalAdapter:
    vendor_id = "llm_universal"
    schema_version = "v1"

    def __init__(self, fallback: LLMFallback) -> None:
        self._fallback = fallback

    def matches(self, payload: dict[str, Any], headers: dict[str, Any]) -> bool:
        # The registry calls deterministic adapters first; this one is a
        # fall-through, never matched directly.
        return True

    async def normalize(
        self,
        payload: dict[str, Any],
        headers: dict[str, Any],
        event_id: str,
        *,
        vendor_hint: str | None = None,
    ) -> AdapterResult:
        try:
            outcome = await self._fallback.classify_extract(
                event_id=event_id,
                vendor_hint=vendor_hint or "unknown",
                payload=payload,
            )
        except BudgetExceeded:
            return AdapterResult(
                status="deferred",
                missing_fields=["budget_exceeded"],
                schema_version=self.schema_version,
                detail={"reason": "llm_budget_exceeded"},
            )
        except LLMValidationError as exc:
            return AdapterResult(
                status="unsupported",
                schema_version=self.schema_version,
                detail={"reason": "llm_invalid_after_retry", "error": str(exc)},
            )

        try:
            canonical = self._build_canonical(event_id=event_id, data=outcome.data, source=outcome.source)
        except (ValidationError, ValueError) as exc:
            return AdapterResult(
                status="unsupported",
                schema_version=self.schema_version,
                detail={"reason": "canonical_build_failed", "error": str(exc), "llm_data": outcome.data},
            )

        return AdapterResult(
            status="ok",
            canonical_event=canonical,
            confidence=float(outcome.data.get("confidence") or 0.0),
            schema_version=self.schema_version,
            detail={"llm_source": outcome.source, "llm_model": outcome.model, "llm_output": outcome.data},
        )

    @staticmethod
    def _build_canonical(*, event_id: str, data: dict[str, Any], source: str) -> CanonicalEvent:
        classification = data["classification"]
        vendor_id = data["vendor_id"]
        confidence = float(data.get("confidence") or 0.0)
        source_enum = Source.LLM_CACHE if source == "llm_cache" else Source.LLM

        if classification == "shipment":
            event_type_str = data.get("event_type")
            ext_id = data.get("entity_external_id")
            ts_str = data.get("event_timestamp")
            if not (event_type_str and ext_id and ts_str):
                raise ValueError("shipment classification missing required fields")
            location = data.get("location")
            return CanonicalShipmentEvent(
                event_id=event_id,
                vendor_id=vendor_id,
                entity_external_id=ext_id,
                event_type=ShipmentEventType(event_type_str),
                event_timestamp=parse_timestamp(ts_str),
                reference_ids=data.get("reference_ids") or {},
                location=Location(**location) if isinstance(location, dict) else None,
                raw_milestone=data.get("raw_milestone"),
                source=source_enum,
                confidence=confidence,
            )

        if classification == "invoice":
            event_type_str = data.get("event_type")
            ext_id = data.get("entity_external_id")
            ts_str = data.get("event_timestamp")
            if not (event_type_str and ext_id and ts_str):
                raise ValueError("invoice classification missing required fields")
            amount_dict = data.get("amount")
            amount = Money(**amount_dict) if isinstance(amount_dict, dict) else None
            due_at = parse_timestamp(data["due_at"]) if data.get("due_at") else None
            return CanonicalInvoiceEvent(
                event_id=event_id,
                vendor_id=vendor_id,
                entity_external_id=ext_id,
                event_type=InvoiceEventType(event_type_str),
                event_timestamp=parse_timestamp(ts_str),
                amount=amount,
                due_at=due_at,
                linked_references=data.get("linked_references") or {},
                raw_kind=data.get("raw_kind"),
                source=source_enum,
                confidence=confidence,
            )

        return CanonicalUnclassifiedEvent(
            event_id=event_id,
            vendor_id=vendor_id,
            summary=data.get("summary"),
            reason=data.get("reason"),
            source=source_enum,
            confidence=confidence,
        )
