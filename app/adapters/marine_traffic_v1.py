"""Marine Traffic Advisory v1 adapter.

These payloads are operational advisories (e.g. port congestion notices) and
have no shipment- or invoice-level identity. They classify as ``unclassified``
deterministically — there is no need to call the LLM.
"""

from __future__ import annotations

from typing import Any

from app.adapters.base import AdapterResult
from app.domain.canonical import CanonicalUnclassifiedEvent, Source


class MarineTrafficV1Adapter:
    vendor_id = "marine_traffic_advisory"
    schema_version = "marine_traffic_v1"

    def matches(self, payload: dict[str, Any], headers: dict[str, Any]) -> bool:
        return str(payload.get("issuer", "")).lower() == "marine-traffic-advisory"

    def normalize(
        self,
        payload: dict[str, Any],
        headers: dict[str, Any],
        event_id: str,
    ) -> AdapterResult:
        canonical = CanonicalUnclassifiedEvent(
            event_id=event_id,
            vendor_id=self.vendor_id,
            schema_version=self.schema_version,
            source=Source.DETERMINISTIC,
            confidence=1.0,
            summary=str(payload.get("subject") or "")[:500] or None,
            reason="marine_traffic_advisory_is_operational_not_shipment_or_invoice",
        )
        return AdapterResult(
            status="ok",
            canonical_event=canonical,
            confidence=1.0,
            schema_version=self.schema_version,
        )
