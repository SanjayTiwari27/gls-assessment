"""Ocean Network Express (carrier_scac=ONEY) v1 adapter."""

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

_ONE_KEYWORDS: list[tuple[ShipmentEventType, tuple[str, ...]]] = [
    (
        ShipmentEventType.DELIVERED,
        (
            "released to consignee",
            "delivered",
            "package handed",
            "empty container returned",
        ),
    ),
    (
        ShipmentEventType.OUT_FOR_DELIVERY,
        ("out for delivery", "delivery scheduled"),
    ),
    (
        ShipmentEventType.IN_TRANSIT,
        (
            "discharged",
            "loaded onboard",
            "vessel departed",
            "sailed",
            "transhipment",
            "in transit",
            "vessel arrived",
        ),
    ),
    (
        ShipmentEventType.PICKED_UP,
        (
            "received at origin",
            "container received",
            "gate-in",
            "gate in",
            "picked up",
        ),
    ),
    (ShipmentEventType.EXCEPTION, ("exception", "delay", "hold")),
    (ShipmentEventType.CANCELLED, ("cancelled", "canceled")),
]


def _classify(text: str) -> ShipmentEventType | None:
    if not text:
        return None
    haystack = text.lower()
    for event_type, keywords in _ONE_KEYWORDS:
        if any(kw in haystack for kw in keywords):
            return event_type
    return None


class OneV1Adapter:
    vendor_id = "ocean_network_express"
    schema_version = "one_v1"

    def matches(self, payload: dict[str, Any], headers: dict[str, Any]) -> bool:
        if str(payload.get("carrier_scac", "")).upper() == "ONEY":
            return True
        carrier = str(payload.get("carrier", "")).lower()
        return "ocean network express" in carrier

    def normalize(
        self,
        payload: dict[str, Any],
        headers: dict[str, Any],
        event_id: str,
    ) -> AdapterResult:
        house_bl = payload.get("house_bl")
        master_bl = payload.get("master_bl")
        container = payload.get("container_no")
        milestone = payload.get("milestone_text") or payload.get("milestone")
        ts_raw = payload.get("milestone_local_time") or payload.get("milestone_at")

        missing: list[str] = []
        if not (house_bl or master_bl or container):
            missing.append("entity_external_id")
        if not milestone:
            missing.append("milestone_text")
        if not ts_raw:
            missing.append("milestone_local_time")
        if missing:
            return AdapterResult(
                status="needs_llm",
                missing_fields=missing,
                schema_version=self.schema_version,
            )

        event_type = _classify(str(milestone))
        if event_type is None:
            return AdapterResult(
                status="needs_llm",
                confidence=0.4,
                missing_fields=["event_type"],
                schema_version=self.schema_version,
                detail={"raw_milestone": milestone},
            )

        try:
            ts = parse_timestamp(str(ts_raw))
        except (ValueError, TypeError):
            return AdapterResult(
                status="needs_llm",
                missing_fields=["event_timestamp"],
                schema_version=self.schema_version,
            )

        external_id = self._build_external_id(house_bl, master_bl, container)

        location = None
        port_code = payload.get("port_of_discharge") or payload.get("port_of_loading")
        if port_code:
            location = Location(code=str(port_code))

        reference_ids = {
            "house_bl": house_bl,
            "master_bl": master_bl,
            "container": container,
            "consignee": payload.get("consignee"),
            "delivery_order_no": payload.get("delivery_order_no"),
            "carrier_scac": payload.get("carrier_scac"),
            "vendor_event_id": payload.get("event_id"),
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
            confidence=0.93,
        )

        return AdapterResult(
            status="ok",
            canonical_event=canonical,
            confidence=0.93,
            schema_version=self.schema_version,
        )

    @staticmethod
    def _build_external_id(house: str | None, master: str | None, container: str | None) -> str:
        # Prefer house BL (most specific to one shipment) + container.
        primary = house or master
        if primary and container:
            return f"{primary}:{container}"
        return primary or container or ""
