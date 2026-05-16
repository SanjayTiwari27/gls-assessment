"""Vendor adapter registry.

The registry is consulted by the worker (never the receiver). It performs a
cheap fingerprint match against each adapter; the first match wins. If no
deterministic adapter matches, the worker falls through to the LLM universal
adapter.

Order matters: more specific adapters should appear first, but in practice the
fingerprints we use are mutually exclusive.
"""

from __future__ import annotations

from typing import Any

from app.adapters.base import VendorAdapter
from app.adapters.globalfreightpay_v1 import GlobalFreightPayV1Adapter
from app.adapters.maersk_v1 import MaerskV1Adapter
from app.adapters.marine_traffic_v1 import MarineTrafficV1Adapter
from app.adapters.one_v1 import OneV1Adapter
from app.logging import get_logger

_DEFAULT_ADAPTERS: tuple[VendorAdapter, ...] = (
    MaerskV1Adapter(),
    OneV1Adapter(),
    GlobalFreightPayV1Adapter(),
    MarineTrafficV1Adapter(),
)
log = get_logger("adapters.registry")


class AdapterRegistry:
    """Lookup table from payload fingerprint → vendor adapter."""

    def __init__(self, adapters: tuple[VendorAdapter, ...] = _DEFAULT_ADAPTERS) -> None:
        self._adapters = adapters

    def resolve(self, payload: dict[str, Any], headers: dict[str, Any]) -> VendorAdapter | None:
        for adapter in self._adapters:
            try:
                if adapter.matches(payload, headers):
                    return adapter
            except Exception as exc:
                log.warning(
                    "adapter_match_failed",
                    adapter=getattr(adapter, "vendor_id", type(adapter).__name__),
                    error=type(exc).__name__,
                )
                continue
        return None

    def all(self) -> tuple[VendorAdapter, ...]:
        return self._adapters


registry = AdapterRegistry()
