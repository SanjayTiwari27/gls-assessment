"""Schema-driven adapter — executes a vendor_schemas.schema_doc against a payload.

This is Path A's extraction engine. Given a schema_doc (learned by the LLM on
first touch, or seeded by a human), it deterministically extracts canonical
fields from any matching payload using declared JSON paths.

No LLM calls. Uses existing parse_timestamp / parse_money utilities.

schema_doc shape (stored in vendor_schemas.schema_doc):
{
  "classification": "shipment",
  "entity_external_id_path": "$.transport_doc.number",
  "entity_external_id_template": "{transport_doc.number}:{container}",
  "event_timestamp_path": "$.milestone_at",
  "event_timestamp_format": "iso8601",
  "raw_event_type_path": "$.milestone",
  "amount_path": "$.transaction.amount",
  "due_at_path": "$.transaction.due_at",
  "reference_ids": {
    "mbl_number": "$.transport_doc.number",
    "container": "$.container",
    "carrier_scac": "$.carrier_scac"
  },
  "linked_references": {
    "carrier": "$.carrier",
    "linked_bl": "$.linked_bl"
  },
  "location": {
    "code": "$.port.code",
    "name": "$.port.name"
  },
  "value_maps": ["$.references"],
  "discriminators": ["$.milestone"]
}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.adapters.parsers import parse_money, parse_timestamp
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


@dataclass(slots=True)
class ExtractionResult:
    """Result of schema-driven extraction."""

    success: bool
    canonical_event: CanonicalEvent | None = None
    raw_event_type: str | None = None
    error: str | None = None
    missing_paths: list[str] | None = None


class SchemaDrivenAdapter:
    """Executes a schema_doc against a payload to produce a CanonicalEvent.

    This adapter is stateless — it receives the schema_doc and event_type_mapping
    as arguments, not from instance state. The caller (worker pipeline) handles
    DB lookups.
    """

    def extract(
        self,
        *,
        payload: dict[str, Any],
        headers: dict[str, Any],
        event_id: str,
        vendor_id: str,
        schema_doc: dict[str, Any],
        canonical_state: str | None,
        classification: str | None = None,
        schema_id: int | None = None,
    ) -> ExtractionResult:
        """Extract a CanonicalEvent from payload using schema_doc field paths.

        Args:
            payload: Raw webhook payload.
            headers: Request headers.
            event_id: Content-addressed event ID.
            vendor_id: Resolved vendor identity.
            schema_doc: The declarative extraction schema from vendor_schemas.
            canonical_state: Resolved canonical state (e.g. "shipment.in_transit").
                If None, extraction still succeeds but raw_event_type is returned
                for the caller to resolve via Classifier.
            classification: Override classification. If None, uses schema_doc's.
            schema_id: vendor_schemas.id for audit trail.

        Returns:
            ExtractionResult with the canonical event or error details.
        """
        cls = classification or schema_doc.get("classification")
        if not cls:
            return ExtractionResult(success=False, error="no classification in schema_doc")

        raw_event_type = self._resolve_path(
            payload, schema_doc.get("raw_event_type_path")
        )

        if cls == "unclassified":
            return self._extract_unclassified(
                payload, event_id, vendor_id, schema_doc, raw_event_type
            )

        # For shipment/invoice we need canonical_state to determine event_type
        if canonical_state is None:
            return ExtractionResult(
                success=False,
                raw_event_type=str(raw_event_type) if raw_event_type else None,
                error="canonical_state not resolved",
            )

        if cls == "shipment":
            return self._extract_shipment(
                payload, event_id, vendor_id, schema_doc, canonical_state, raw_event_type
            )

        if cls == "invoice":
            return self._extract_invoice(
                payload, event_id, vendor_id, schema_doc, canonical_state, raw_event_type
            )

        return ExtractionResult(success=False, error=f"unknown classification: {cls}")

    def _extract_shipment(
        self,
        payload: dict[str, Any],
        event_id: str,
        vendor_id: str,
        schema_doc: dict[str, Any],
        canonical_state: str,
        raw_event_type: Any,
    ) -> ExtractionResult:
        missing: list[str] = []

        ext_id = self._resolve_entity_id(payload, schema_doc)
        if not ext_id:
            missing.append("entity_external_id")

        ts_raw = self._resolve_timestamp_path(payload, schema_doc)
        if not ts_raw:
            missing.append("event_timestamp")

        if missing:
            return ExtractionResult(
                success=False,
                raw_event_type=str(raw_event_type) if raw_event_type else None,
                missing_paths=missing,
                error=f"missing required paths: {missing}",
            )

        try:
            event_ts = parse_timestamp(str(ts_raw))
        except (ValueError, TypeError) as exc:
            return ExtractionResult(
                success=False,
                raw_event_type=str(raw_event_type) if raw_event_type else None,
                error=f"timestamp parse failed: {exc}",
            )

        try:
            event_type = ShipmentEventType(canonical_state)
        except ValueError:
            return ExtractionResult(
                success=False,
                raw_event_type=str(raw_event_type) if raw_event_type else None,
                error=f"invalid shipment state: {canonical_state}",
            )

        location = self._extract_location(payload, schema_doc)
        reference_ids = self._extract_map(payload, schema_doc.get("reference_ids", {}))

        canonical = CanonicalShipmentEvent(
            event_id=event_id,
            vendor_id=vendor_id,
            entity_external_id=ext_id,
            event_type=event_type,
            event_timestamp=event_ts,
            reference_ids=reference_ids,
            location=location,
            raw_milestone=str(raw_event_type) if raw_event_type else None,
            source=Source.DETERMINISTIC,
            confidence=0.92,
        )

        return ExtractionResult(
            success=True,
            canonical_event=canonical,
            raw_event_type=str(raw_event_type) if raw_event_type else None,
        )

    def _extract_invoice(
        self,
        payload: dict[str, Any],
        event_id: str,
        vendor_id: str,
        schema_doc: dict[str, Any],
        canonical_state: str,
        raw_event_type: Any,
    ) -> ExtractionResult:
        missing: list[str] = []

        ext_id = self._resolve_entity_id(payload, schema_doc)
        if not ext_id:
            missing.append("entity_external_id")

        ts_raw = self._resolve_timestamp_path(payload, schema_doc)
        if not ts_raw:
            missing.append("event_timestamp")

        if missing:
            return ExtractionResult(
                success=False,
                raw_event_type=str(raw_event_type) if raw_event_type else None,
                missing_paths=missing,
                error=f"missing required paths: {missing}",
            )

        try:
            event_ts = parse_timestamp(str(ts_raw))
        except (ValueError, TypeError) as exc:
            return ExtractionResult(
                success=False,
                raw_event_type=str(raw_event_type) if raw_event_type else None,
                error=f"timestamp parse failed: {exc}",
            )

        try:
            event_type = InvoiceEventType(canonical_state)
        except ValueError:
            return ExtractionResult(
                success=False,
                raw_event_type=str(raw_event_type) if raw_event_type else None,
                error=f"invalid invoice state: {canonical_state}",
            )

        amount = self._extract_amount(payload, schema_doc)
        due_at = None
        due_raw = self._resolve_path(payload, schema_doc.get("due_at_path"))
        if due_raw:
            try:
                due_at = parse_timestamp(str(due_raw))
            except (ValueError, TypeError):
                pass

        linked_refs = self._extract_map(payload, schema_doc.get("linked_references", {}))

        canonical = CanonicalInvoiceEvent(
            event_id=event_id,
            vendor_id=vendor_id,
            entity_external_id=ext_id,
            event_type=event_type,
            event_timestamp=event_ts,
            amount=amount,
            due_at=due_at,
            linked_references=linked_refs,
            raw_kind=str(raw_event_type) if raw_event_type else None,
            source=Source.DETERMINISTIC,
            confidence=0.92,
        )

        return ExtractionResult(
            success=True,
            canonical_event=canonical,
            raw_event_type=str(raw_event_type) if raw_event_type else None,
        )

    def _extract_unclassified(
        self,
        payload: dict[str, Any],
        event_id: str,
        vendor_id: str,
        schema_doc: dict[str, Any],
        raw_event_type: Any,
    ) -> ExtractionResult:
        summary_path = schema_doc.get("summary_path")
        summary = None
        if summary_path:
            val = self._resolve_path(payload, summary_path)
            if val:
                summary = str(val)[:500]

        canonical = CanonicalUnclassifiedEvent(
            event_id=event_id,
            vendor_id=vendor_id,
            summary=summary,
            reason=schema_doc.get("reason", "schema_driven_unclassified"),
            source=Source.DETERMINISTIC,
            confidence=0.90,
        )

        return ExtractionResult(
            success=True,
            canonical_event=canonical,
            raw_event_type=str(raw_event_type) if raw_event_type else None,
        )

    # ------------------------------------------------------------------ #
    # Timestamp resolution (supports fallback paths)
    # ------------------------------------------------------------------ #

    def _resolve_timestamp_path(self, payload: dict[str, Any], schema_doc: dict[str, Any]) -> Any:
        """Resolve event timestamp, supporting single path or ordered fallback list.

        schema_doc can declare either:
          "event_timestamp_path": "$.milestone_at"
        or:
          "event_timestamp_paths": ["$.transaction.settled_at", "$.transaction.issued_at", ...]

        Returns the first non-null resolved value.
        """
        paths = schema_doc.get("event_timestamp_paths")
        if paths and isinstance(paths, list):
            for p in paths:
                val = self._resolve_path(payload, p)
                if val is not None:
                    return val
            return None
        return self._resolve_path(payload, schema_doc.get("event_timestamp_path"))

    # ------------------------------------------------------------------ #
    # Path resolution
    # ------------------------------------------------------------------ #

    def _resolve_path(self, payload: dict[str, Any], path: str | None) -> Any:
        """Walk a dotted JSON path like '$.transport_doc.number' against payload.

        Supports:
          - Simple dot notation: $.a.b.c
          - Array indexing: $.events[0].code
          - Bare array element: $.items[] (returns first element)
        """
        if not path:
            return None

        parts = _parse_path(path)
        current: Any = payload

        for part in parts:
            if current is None:
                return None
            if part == "$":
                continue
            elif part.endswith("[]"):
                key = part[:-2]
                if key:
                    if not isinstance(current, dict):
                        return None
                    current = current.get(key)
                if isinstance(current, list) and current:
                    current = current[0]
                elif isinstance(current, list):
                    return None
            elif part.endswith("]"):
                bracket = part.index("[")
                key = part[:bracket]
                idx_str = part[bracket + 1 : -1]
                if key:
                    if not isinstance(current, dict):
                        return None
                    current = current.get(key)
                if isinstance(current, list):
                    try:
                        current = current[int(idx_str)]
                    except (IndexError, ValueError):
                        return None
                else:
                    return None
            else:
                if not isinstance(current, dict):
                    return None
                current = current.get(part)

        return current

    def _resolve_entity_id(self, payload: dict[str, Any], schema_doc: dict[str, Any]) -> str | None:
        """Resolve entity external ID, supporting both simple path and template."""
        template = schema_doc.get("entity_external_id_template")
        if template:
            return self._resolve_template(payload, template)

        path = schema_doc.get("entity_external_id_path")
        val = self._resolve_path(payload, path)
        return str(val) if val else None

    def _resolve_template(self, payload: dict[str, Any], template: str) -> str | None:
        """Resolve a template like '{transport_doc.number}:{container}'.

        Each {path} placeholder is resolved against the payload. If any required
        segment (non-optional) is missing, returns None.
        """
        import re

        parts = re.split(r"(\{[^}]+\})", template)
        result_parts: list[str] = []
        has_value = False

        for part in parts:
            if part.startswith("{") and part.endswith("}"):
                inner_path = "$." + part[1:-1]
                val = self._resolve_path(payload, inner_path)
                if val:
                    result_parts.append(str(val))
                    has_value = True
                else:
                    result_parts.append("")
            else:
                result_parts.append(part)

        if not has_value:
            return None

        result = "".join(result_parts)
        # Clean up empty segments from template separators
        result = result.strip(":").strip("-")
        return result or None

    def _extract_location(
        self, payload: dict[str, Any], schema_doc: dict[str, Any]
    ) -> Location | None:
        loc_spec = schema_doc.get("location")
        if not loc_spec or not isinstance(loc_spec, dict):
            return None

        code = self._resolve_path(payload, loc_spec.get("code"))
        name = self._resolve_path(payload, loc_spec.get("name"))
        lat = self._resolve_path(payload, loc_spec.get("latitude"))
        lon = self._resolve_path(payload, loc_spec.get("longitude"))

        if not code and not name:
            return None

        return Location(
            code=str(code) if code else None,
            name=str(name) if name else None,
            latitude=float(lat) if lat is not None else None,
            longitude=float(lon) if lon is not None else None,
        )

    def _extract_amount(
        self, payload: dict[str, Any], schema_doc: dict[str, Any]
    ) -> Money | None:
        amount_path = schema_doc.get("amount_path")
        if not amount_path:
            return None

        amount_raw = self._resolve_path(payload, amount_path)
        if not amount_raw:
            return None

        try:
            currency, amount_minor = parse_money(str(amount_raw))
            return Money(currency=currency, amount_minor=amount_minor)
        except ValueError:
            return None

    def _extract_map(
        self, payload: dict[str, Any], path_map: dict[str, str]
    ) -> dict[str, Any]:
        """Extract a dict of {label: resolved_value} from a path map spec."""
        result: dict[str, Any] = {}
        for key, path in path_map.items():
            val = self._resolve_path(payload, path)
            if val is not None:
                result[key] = val
        return result


def _parse_path(path: str) -> list[str]:
    """Split a JSON path like '$.transport_doc.number' into segments."""
    if path.startswith("$."):
        path = path[2:]
    elif path.startswith("$"):
        path = path[1:]

    segments: list[str] = []
    current = ""
    i = 0
    while i < len(path):
        ch = path[i]
        if ch == ".":
            if current:
                segments.append(current)
                current = ""
        elif ch == "[":
            if current:
                bracket_end = path.index("]", i)
                current += path[i : bracket_end + 1]
                i = bracket_end
            else:
                bracket_end = path.index("]", i)
                current = path[i : bracket_end + 1]
                i = bracket_end
        else:
            current += ch
        i += 1

    if current:
        segments.append(current)

    return segments
