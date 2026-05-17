"""Schema Discovery — infer a schema_doc from a successful LLM extraction.

After Path B (LLM) successfully extracts a CanonicalEvent from a new payload,
this module reverse-maps the extracted values back to their positions in the
original payload to build a declarative schema_doc.

The schema_doc is then persisted to vendor_schemas so all future events with
the same structural fingerprint use Path A (deterministic, zero LLM cost).

Algorithm:
  1. For each extracted canonical field, search the payload for the matching value.
  2. Record the JSON path where the value was found.
  3. Build the schema_doc in our reverse-mapping format.
  4. Validate: re-run SchemaDrivenAdapter with the inferred schema_doc against
     the same payload — if it produces the same extraction, the schema is correct.
"""

from __future__ import annotations

from typing import Any

import asyncpg

from app.adapters.fingerprint import structural_fingerprint
from app.adapters.schema_driven import SchemaDrivenAdapter
from app.logging import get_logger

log = get_logger("adapters.schema_discoverer")


class SchemaDiscoverer:
    """Infer and persist a schema_doc from a successful LLM extraction."""

    def __init__(self) -> None:
        self._adapter = SchemaDrivenAdapter()

    async def discover_and_persist(
        self,
        *,
        pool: asyncpg.Pool,
        vendor_id: str,
        payload: dict[str, Any],
        llm_output: dict[str, Any],
        event_id: str,
    ) -> int | None:
        """Infer a schema_doc and persist it. Returns the schema_id or None on failure.

        Args:
            pool: DB connection pool.
            vendor_id: Resolved vendor identity.
            payload: The original raw webhook payload.
            llm_output: The validated dict returned by LLM (matches v1_target_schema).
            event_id: The triggering event's ID (stored as source_event_id).

        Returns:
            The vendor_schemas.id of the persisted row, or None if inference failed.
        """
        classification = llm_output.get("classification")
        if not classification:
            return None

        schema_doc = self._infer_schema_doc(payload, llm_output, classification)
        if schema_doc is None:
            log.info("schema_inference_failed", event_id=event_id, vendor_id=vendor_id)
            return None

        # Validate: re-run extraction and check it produces the key fields
        if not self._validate_schema(payload, event_id, vendor_id, schema_doc, llm_output):
            log.info("schema_validation_failed", event_id=event_id, vendor_id=vendor_id)
            return None

        # Persist
        fp = structural_fingerprint(payload)
        schema_id = await self._persist(
            pool=pool,
            vendor_id=vendor_id,
            fingerprint=fp,
            schema_doc=schema_doc,
            event_id=event_id,
        )

        # Also persist the event type mapping if we have one
        event_type = llm_output.get("event_type")
        raw_event_type = self._find_raw_event_type(payload, schema_doc)
        if event_type and raw_event_type and classification in ("shipment", "invoice"):
            await self._persist_event_type(
                pool=pool,
                vendor_id=vendor_id,
                raw_event_type=str(raw_event_type),
                classification=classification,
                canonical_state=event_type,
            )

        log.info(
            "schema_discovered",
            event_id=event_id,
            vendor_id=vendor_id,
            schema_id=schema_id,
            classification=classification,
        )
        return schema_id

    def _infer_schema_doc(
        self,
        payload: dict[str, Any],
        llm_output: dict[str, Any],
        classification: str,
    ) -> dict[str, Any] | None:
        """Build a schema_doc by reverse-mapping extracted values to payload paths."""
        fields: dict[str, Any] = {}

        if classification == "shipment":
            self._map_field(fields, "entity_external_id", llm_output.get("entity_external_id"), payload)
            self._map_field(fields, "event_timestamp", llm_output.get("event_timestamp"), payload)
            self._map_raw_event_type(fields, llm_output.get("raw_milestone"), payload)
            fields["raw_milestone"] = fields.get("event_type")  # same source as event_type
            self._map_location(fields, llm_output.get("location"), payload)
            self._map_reference_ids(fields, llm_output.get("reference_ids"), payload)

        elif classification == "invoice":
            self._map_field(fields, "entity_external_id", llm_output.get("entity_external_id"), payload)
            self._map_field(fields, "event_timestamp", llm_output.get("event_timestamp"), payload)
            self._map_raw_event_type(fields, llm_output.get("raw_kind"), payload)
            fields["raw_kind"] = fields.get("event_type")  # same source as event_type
            self._map_amount(fields, llm_output.get("amount"), payload)
            self._map_field(fields, "due_at", llm_output.get("due_at"), payload)
            self._map_linked_references(fields, llm_output.get("linked_references"), payload)

        elif classification == "unclassified":
            self._map_field(fields, "summary", llm_output.get("summary"), payload)
            self._map_field(fields, "reason", llm_output.get("reason"), payload)

        else:
            return None

        # Must have at least entity_external_id and event_timestamp for shipment/invoice
        if classification in ("shipment", "invoice") and (
            "entity_external_id" not in fields or "event_timestamp" not in fields
        ):
            return None

        return {"classification": classification, "fields": fields}

    def _map_field(
        self,
        fields: dict[str, Any],
        canonical_key: str,
        extracted_value: Any,
        payload: dict[str, Any],
    ) -> None:
        """Find the path in payload where extracted_value lives, add to fields."""
        if extracted_value is None:
            return

        # Direct match first
        path = self._find_value_path(payload, extracted_value)
        if path:
            fields[canonical_key] = path
            return

        # For entity_external_id: detect composite IDs like "A:B" and build a template
        if canonical_key == "entity_external_id" and isinstance(extracted_value, str):
            template = self._infer_composite_template(payload, extracted_value)
            if template:
                fields[canonical_key] = {"template": template}
                return

        # For event_timestamp: try matching just the date/time portion (LLM may
        # have normalized timezone but the raw value is still in the payload)
        if canonical_key in ("event_timestamp", "due_at") and isinstance(extracted_value, str):
            # Try without timezone suffix (LLM converts to UTC, payload has local tz)
            path = self._find_timestamp_path(payload, extracted_value)
            if path:
                fields[canonical_key] = path

    def _map_raw_event_type(
        self,
        fields: dict[str, Any],
        raw_value: Any,
        payload: dict[str, Any],
    ) -> None:
        """Map the event_type field (the raw vendor string that needs classification)."""
        if raw_value is None:
            return
        path = self._find_value_path(payload, raw_value)
        if path:
            fields["event_type"] = path

    def _map_location(
        self,
        fields: dict[str, Any],
        location: Any,
        payload: dict[str, Any],
    ) -> None:
        """Map location sub-fields."""
        if not location or not isinstance(location, dict):
            return
        loc: dict[str, str] = {}
        for key in ("code", "name", "latitude", "longitude"):
            val = location.get(key)
            if val is not None:
                path = self._find_value_path(payload, val)
                if path:
                    loc[key] = path
        if loc:
            fields["location"] = loc

    def _map_reference_ids(
        self,
        fields: dict[str, Any],
        refs: Any,
        payload: dict[str, Any],
    ) -> None:
        """Map reference_ids dict."""
        if not refs or not isinstance(refs, dict):
            return
        mapped: dict[str, str] = {}
        for key, val in refs.items():
            if val is not None:
                path = self._find_value_path(payload, val)
                if path:
                    mapped[key] = path
        if mapped:
            fields["reference_ids"] = mapped

    def _map_linked_references(
        self,
        fields: dict[str, Any],
        refs: Any,
        payload: dict[str, Any],
    ) -> None:
        """Map linked_references dict."""
        if not refs or not isinstance(refs, dict):
            return
        mapped: dict[str, str] = {}
        for key, val in refs.items():
            if val is not None:
                path = self._find_value_path(payload, val)
                if path:
                    mapped[key] = path
        if mapped:
            fields["linked_references"] = mapped

    def _map_amount(
        self,
        fields: dict[str, Any],
        amount: Any,
        payload: dict[str, Any],
    ) -> None:
        """Map the amount field — find the raw string that parse_money would consume."""
        if not amount or not isinstance(amount, dict):
            return
        # The LLM returns parsed {currency, amount_minor}, but we need to find
        # the raw string in the payload that produces this when parsed.
        # Strategy: look for any string value that contains the currency code.
        currency = amount.get("currency", "")
        path = self._find_money_path(payload, currency)
        if path:
            fields["amount"] = path

    def _infer_composite_template(self, payload: dict[str, Any], composite: str) -> str | None:
        """Try to decompose a composite value like 'A:B' or 'A-B' into a template.

        Looks for separators (:, -, _) and checks if each component exists in the payload.
        Returns a template string like '{transport_doc.number}:{container}' or None.
        """
        for sep in (":", "-", "_", "/"):
            if sep not in composite:
                continue
            parts = composite.split(sep)
            if len(parts) < 2 or len(parts) > 4:
                continue

            # Try to find each part in the payload
            paths: list[str] = []
            all_found = True
            for part in parts:
                part = part.strip()
                if not part:
                    all_found = False
                    break
                path = self._find_value_path(payload, part)
                if path is None:
                    all_found = False
                    break
                # Convert $.a.b.c → {a.b.c} for template
                paths.append("{" + path[2:] + "}")  # strip "$."

            if all_found and paths:
                return sep.join(paths)

        return None

    def _find_timestamp_path(self, payload: dict[str, Any], ts_value: str) -> str | None:
        """Find a timestamp in the payload that corresponds to the LLM's extracted value.

        The LLM may have converted timezone (e.g., +08:00 → Z) or reformatted the
        date entirely (e.g., "28/04/2026 09:42 WIB" → "2026-04-28T02:42:00Z").
        We look for payload values that look like timestamps and share date components.
        """
        import re

        # Extract year, month, day from ISO ts_value
        date_match = re.match(r"(\d{4})-(\d{2})-(\d{2})", ts_value)
        if not date_match:
            return None
        year, month, day = date_match.groups()

        results: list[str] = []

        def _looks_like_timestamp(val: str) -> bool:
            """Heuristic: contains at least a year and some time-like chars."""
            if year not in val:
                return False
            # Must have date separators and time components
            has_date_sep = any(c in val for c in "/-.")
            has_time = ":" in val
            return has_date_sep and has_time

        def _date_matches(val: str) -> bool:
            """Check if the value contains the same day/month/year components."""
            # ISO format: YYYY-MM-DD
            if f"{year}-{month}-{day}" in val:
                return True
            # European format: DD/MM/YYYY or DD.MM.YYYY
            if f"{day}/{month}/{year}" in val or f"{day}.{month}.{year}" in val:
                return True
            # US format: MM/DD/YYYY
            if f"{month}/{day}/{year}" in val:
                return True
            return False

        def _walk(node: Any, path: str) -> None:
            if isinstance(node, dict):
                for k, v in node.items():
                    _walk(v, f"{path}.{k}")
            elif isinstance(node, list):
                for i, el in enumerate(node):
                    _walk(el, f"{path}[{i}]")
            elif isinstance(node, str) and _looks_like_timestamp(node) and _date_matches(node):
                results.append(path)

        _walk(payload, "$")

        # Prefer paths with time-related key names
        time_keys = {"time", "at", "date", "timestamp", "ts", "issued", "settled"}

        def _score(p: str) -> tuple[int, int]:
            last_key = p.rsplit(".", 1)[-1].lower()
            key_match = 0 if any(k in last_key for k in time_keys) else 1
            return (key_match, len(p))

        if results:
            results.sort(key=_score)
            return results[0]
        return None

    def _find_money_path(self, payload: dict[str, Any], currency: str) -> str | None:
        """Find a path whose value looks like a money string containing the currency."""
        if not currency:
            return None

        results: list[str] = []

        def _walk(node: Any, path: str) -> None:
            if isinstance(node, str) and currency in node.upper():
                results.append(path)
            elif isinstance(node, dict):
                for k, v in node.items():
                    _walk(v, f"{path}.{k}")
            elif isinstance(node, list):
                for i, el in enumerate(node):
                    _walk(el, f"{path}[{i}]")

        _walk(payload, "$")
        # Prefer the shortest/most specific path
        return results[0] if results else None

    def _find_value_path(self, payload: dict[str, Any], value: Any) -> str | None:
        """Search payload recursively for a leaf matching the extracted value.

        Returns the first matching JSON path or None.
        """
        if value is None:
            return None

        # Normalize comparison: stringify for loose matching
        target = str(value).strip()
        results: list[str] = []

        def _walk(node: Any, path: str) -> None:
            if isinstance(node, dict):
                for k, v in node.items():
                    _walk(v, f"{path}.{k}")
            elif isinstance(node, list):
                for i, el in enumerate(node):
                    _walk(el, f"{path}[{i}]")
            else:
                # Leaf — compare
                if node is not None and str(node).strip() == target:
                    results.append(path)

        _walk(payload, "$")

        # If multiple matches, prefer shorter paths (more specific)
        if results:
            results.sort(key=len)
            return results[0]
        return None

    def _find_raw_event_type(self, payload: dict[str, Any], schema_doc: dict[str, Any]) -> Any:
        """Extract the raw event type from payload using the inferred schema_doc."""
        fields = schema_doc.get("fields", {})
        event_type_path = fields.get("event_type")
        if not event_type_path or not isinstance(event_type_path, str):
            return None
        return self._adapter._resolve_path(payload, event_type_path)

    def _validate_schema(
        self,
        payload: dict[str, Any],
        event_id: str,
        vendor_id: str,
        schema_doc: dict[str, Any],
        llm_output: dict[str, Any],
    ) -> bool:
        """Validate: run the inferred schema_doc and check key fields match."""
        classification = schema_doc.get("classification")
        event_type = llm_output.get("event_type")

        # For unclassified, just check extraction succeeds
        if classification == "unclassified":
            result = self._adapter.extract(
                payload=payload,
                headers={},
                event_id=event_id,
                vendor_id=vendor_id,
                schema_doc=schema_doc,
                canonical_state=None,
            )
            return result.success

        # For shipment/invoice, check with the canonical_state
        if not event_type:
            return False

        result = self._adapter.extract(
            payload=payload,
            headers={},
            event_id=event_id,
            vendor_id=vendor_id,
            schema_doc=schema_doc,
            canonical_state=event_type,
        )

        if not result.success:
            return False

        # Check key fields match
        ev = result.canonical_event
        expected_ext_id = llm_output.get("entity_external_id")
        if (
            expected_ext_id
            and hasattr(ev, "entity_external_id")
            and ev.entity_external_id != expected_ext_id
            # Allow if one contains the other (template vs direct path)
            and expected_ext_id not in ev.entity_external_id
            and ev.entity_external_id not in expected_ext_id
        ):
            return False

        return True

    async def _persist(
        self,
        *,
        pool: asyncpg.Pool,
        vendor_id: str,
        fingerprint: str,
        schema_doc: dict[str, Any],
        event_id: str,
    ) -> int:
        """Write the schema_doc to vendor_schemas."""
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO vendor_schemas
                    (vendor_id, structural_fingerprint, schema_doc, status, created_by, source_event_id)
                VALUES ($1, $2, $3::jsonb, 'provisional', 'schema_discoverer', $4)
                ON CONFLICT (vendor_id, structural_fingerprint, schema_version)
                DO UPDATE SET
                    schema_doc = EXCLUDED.schema_doc,
                    source_event_id = EXCLUDED.source_event_id
                WHERE vendor_schemas.status = 'provisional'
                RETURNING id
                """,
                vendor_id,
                fingerprint,
                schema_doc,
                event_id,
            )
            return row["id"]

    async def _persist_event_type(
        self,
        *,
        pool: asyncpg.Pool,
        vendor_id: str,
        raw_event_type: str,
        classification: str,
        canonical_state: str,
    ) -> None:
        """Persist the event type mapping alongside the schema."""
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO vendor_event_type_map
                    (vendor_id, raw_event_type, classification, canonical_state, confidence, source)
                VALUES ($1, $2, $3, $4, 0.85, 'schema_discoverer')
                ON CONFLICT (vendor_id, raw_event_type) DO NOTHING
                """,
                vendor_id,
                raw_event_type,
                classification,
                canonical_state,
            )
