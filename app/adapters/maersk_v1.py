"""Maersk Line (carrier_scac=MAEU) v1 adapter."""

from __future__ import annotations

from typing import Any

from app.adapters.base import AdapterResult
from app.adapters.parsers import parse_timestamp
from app.domain.canonical import (
    CanonicalShipmentEvent,
    Location,
    ShipmentEventType,
    Source,
)

_MILESTONE_KEYWORDS: list[tuple[ShipmentEventType, tuple[str, ...]]] = [
    (
        ShipmentEventType.DELIVERED,
        (
            "released to consignee",
            "delivered to consignee",
            "empty container returned",
            "delivery completed",
        ),
    ),
    (
        ShipmentEventType.OUT_FOR_DELIVERY,
        (
            "out for delivery",
            "on hand for delivery",
            "delivery scheduled",
        ),
    ),
    (
        ShipmentEventType.IN_TRANSIT,
        (
            "loaded onboard",
            "vessel departed",
            "sailed",
            "vessel arrived",
            "transhipment",
            "discharged",
            "in transit",
        ),
    ),
    (
        ShipmentEventType.PICKED_UP,
        (
            "empty container released to shipper",
            "received at origin terminal",
            "gate-in",
            "gate in",
            "picked up",
            "container received",
        ),
    ),
    (
        ShipmentEventType.EXCEPTION,
        ("exception", "delay", "rolled", "blocked", "hold"),
    ),
    (
        ShipmentEventType.CANCELLED,
        ("cancelled", "canceled", "voided shipment"),
    ),
]


def _classify_milestone(text: str) -> ShipmentEventType | None:
    """Lowercased keyword match. Order matters: more specific buckets first."""

    if not text:
        return None
    haystack = text.lower()
    for event_type, keywords in _MILESTONE_KEYWORDS:
        if any(kw in haystack for kw in keywords):
            return event_type
    return None


class MaerskV1Adapter:
    vendor_id = "maersk"
    schema_version = "maersk_v1"

    def matches(self, payload: dict[str, Any], headers: dict[str, Any]) -> bool:
        return str(payload.get("carrier_scac", "")).upper() == "MAEU"

    def normalize(
        self,
        payload: dict[str, Any],
        headers: dict[str, Any],
        event_id: str,
    ) -> AdapterResult:
        transport = payload.get("transport_doc") or {}
        mbl = transport.get("number") if isinstance(transport, dict) else None
        container = payload.get("container")
        milestone = payload.get("milestone")
        milestone_at = payload.get("milestone_at")

        missing: list[str] = []
        if not mbl and not container:
            missing.append("entity_external_id")
        if not milestone:
            missing.append("milestone")
        if not milestone_at:
            missing.append("milestone_at")

        if missing:
            return AdapterResult(
                status="needs_llm",
                missing_fields=missing,
                schema_version=self.schema_version,
            )

        event_type = _classify_milestone(str(milestone))
        if event_type is None:
            return AdapterResult(
                status="needs_llm",
                confidence=0.4,
                missing_fields=["event_type"],
                schema_version=self.schema_version,
                detail={"raw_milestone": milestone},
            )

        external_id = self._build_external_id(mbl, container)
        try:
            ts = parse_timestamp(str(milestone_at))
        except (ValueError, TypeError):
            return AdapterResult(
                status="needs_llm",
                missing_fields=["event_timestamp"],
                schema_version=self.schema_version,
            )

        port = payload.get("port") or {}
        location = None
        if isinstance(port, dict) and (port.get("code") or port.get("name")):
            location = Location(code=port.get("code"), name=port.get("name"))

        vessel = payload.get("vessel") or {}
        reference_ids = {
            "mbl_number": mbl,
            "container": container,
            "carrier_scac": payload.get("carrier_scac"),
            "vendor_event_id": payload.get("event_msg_id"),
            "shipper_ref": payload.get("shipper_ref"),
        }
        if isinstance(vessel, dict) and vessel:
            reference_ids["vessel"] = {
                "name": vessel.get("name"),
                "imo": vessel.get("imo"),
                "voyage": vessel.get("voyage"),
            }
        reference_ids = {k: v for k, v in reference_ids.items() if v}

        canonical = CanonicalShipmentEvent(
            event_id=event_id,
            vendor_id=self.vendor_id,
            entity_external_id=external_id,
            event_type=event_type,
            event_timestamp=ts,
            reference_ids=reference_ids,
            location=location,
            raw_milestone=str(milestone),
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

    @staticmethod
    def _build_external_id(mbl: str | None, container: str | None) -> str:
        # Maersk events about the same parcel should collapse to one entity.
        # MBL identifies the bill of lading; container identifies the unit
        # within it. We key on both so that splits/consolidations (which would
        # show different MBL+container pairs) are kept distinct.
        if mbl and container:
            return f"{mbl}:{container}"
        return mbl or container or ""
