"""Adapter contracts.

Adapters are pure functions of `(payload, headers, event_id)`. They never
touch the DB, network, or LLM. That contract is what allows fixture-based
unit testing of every vendor offline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from app.domain.canonical import CanonicalEvent

AdapterStatus = Literal["ok", "needs_llm", "unsupported", "deferred"]


@dataclass(slots=True)
class AdapterResult:
    """Outcome of an adapter run.

    - ``ok`` → ``canonical_event`` is set and ready for the state machine.
    - ``needs_llm`` → adapter recognized the vendor but could not extract all
      required fields with high confidence; the worker should call the LLM.
    - ``unsupported`` → adapter does not recognize this vendor at all.
    - ``deferred`` → processing should be retried later (for example LLM budget
      exhaustion).
    """

    status: AdapterStatus
    canonical_event: CanonicalEvent | None = None
    confidence: float = 0.0
    missing_fields: list[str] = field(default_factory=list)
    schema_version: str = "v1"
    detail: dict[str, Any] = field(default_factory=dict)


class VendorAdapter(Protocol):
    vendor_id: str
    schema_version: str

    def matches(self, payload: dict[str, Any], headers: dict[str, Any]) -> bool:
        """Cheap fingerprinting: does this adapter own this payload?"""

    def normalize(
        self,
        payload: dict[str, Any],
        headers: dict[str, Any],
        event_id: str,
    ) -> AdapterResult:
        """Pure function: payload → AdapterResult."""
