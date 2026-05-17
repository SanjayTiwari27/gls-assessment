"""Canonical event types — the single internal representation downstream of
every adapter.

Adapters and the LLM both emit one of these. The state machine and the storage
layer only ever consume canonical events. Vendor-shaped fields live inside
``reference_ids`` / ``linked_references`` for auditability but never drive
business logic.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Classification(str, Enum):
    SHIPMENT = "shipment"
    INVOICE = "invoice"
    UNCLASSIFIED = "unclassified"


class ShipmentEventType(str, Enum):
    PICKED_UP = "shipment.picked_up"
    IN_TRANSIT = "shipment.in_transit"
    OUT_FOR_DELIVERY = "shipment.out_for_delivery"
    DELIVERED = "shipment.delivered"
    EXCEPTION = "shipment.exception"
    CANCELLED = "shipment.cancelled"


class InvoiceEventType(str, Enum):
    ISSUED = "invoice.issued"
    PAID = "invoice.paid"
    VOIDED = "invoice.voided"
    REFUNDED = "invoice.refunded"


class ShipmentState(str, Enum):
    PICKED_UP = "PICKED_UP"
    IN_TRANSIT = "IN_TRANSIT"
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    DELIVERED = "DELIVERED"
    EXCEPTION = "EXCEPTION"
    CANCELLED = "CANCELLED"


class InvoiceState(str, Enum):
    ISSUED = "ISSUED"
    PAID = "PAID"
    VOIDED = "VOIDED"
    REFUNDED = "REFUNDED"


class Source(str, Enum):
    DETERMINISTIC = "deterministic"
    LLM = "llm"
    LLM_CACHE = "llm_cache"


class Money(BaseModel):
    """Currency-aware money. Stored as integer minor units to avoid float drift."""

    model_config = ConfigDict(extra="forbid")
    currency: Annotated[str, Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")]
    amount_minor: Annotated[int, Field(ge=0)]


class Location(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: str | None = None
    name: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class _CanonicalBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    vendor_id: str
    schema_version: str = "v1"
    source: Source
    confidence: Annotated[float, Field(ge=0.0, le=1.0)]

    @field_validator("event_id", "vendor_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must be non-empty")
        return v


class CanonicalShipmentEvent(_CanonicalBase):
    classification: Literal[Classification.SHIPMENT] = Classification.SHIPMENT
    entity_external_id: str
    event_type: ShipmentEventType
    event_timestamp: datetime
    reference_ids: dict[str, Any] = Field(default_factory=dict)
    location: Location | None = None
    raw_milestone: str | None = None

    @field_validator("event_timestamp")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("event_timestamp must be timezone-aware")
        return v


class CanonicalInvoiceEvent(_CanonicalBase):
    classification: Literal[Classification.INVOICE] = Classification.INVOICE
    entity_external_id: str
    event_type: InvoiceEventType
    event_timestamp: datetime
    amount: Money | None = None
    due_at: datetime | None = None
    linked_references: dict[str, Any] = Field(default_factory=dict)
    raw_kind: str | None = None

    @field_validator("event_timestamp")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("event_timestamp must be timezone-aware")
        return v


class CanonicalUnclassifiedEvent(_CanonicalBase):
    classification: Literal[Classification.UNCLASSIFIED] = Classification.UNCLASSIFIED
    summary: str | None = None
    reason: str | None = None


CanonicalEvent = CanonicalShipmentEvent | CanonicalInvoiceEvent | CanonicalUnclassifiedEvent


SHIPMENT_EVENT_TO_STATE: dict[ShipmentEventType, ShipmentState] = {
    ShipmentEventType.PICKED_UP: ShipmentState.PICKED_UP,
    ShipmentEventType.IN_TRANSIT: ShipmentState.IN_TRANSIT,
    ShipmentEventType.OUT_FOR_DELIVERY: ShipmentState.OUT_FOR_DELIVERY,
    ShipmentEventType.DELIVERED: ShipmentState.DELIVERED,
    ShipmentEventType.EXCEPTION: ShipmentState.EXCEPTION,
    ShipmentEventType.CANCELLED: ShipmentState.CANCELLED,
}

INVOICE_EVENT_TO_STATE: dict[InvoiceEventType, InvoiceState] = {
    InvoiceEventType.ISSUED: InvoiceState.ISSUED,
    InvoiceEventType.PAID: InvoiceState.PAID,
    InvoiceEventType.VOIDED: InvoiceState.VOIDED,
    InvoiceEventType.REFUNDED: InvoiceState.REFUNDED,
}
