"""DB-backed vendor schema registry.

Replaces the hardcoded adapter tuple. The worker computes a structural
fingerprint of the payload and looks up vendor_schemas for a matching
(vendor_id, fingerprint) row. If found, returns the schema_doc for the
SchemaDrivenAdapter to execute. If not, returns None and the caller falls
through to the LLM discovery path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import asyncpg

from app.adapters.fingerprint import structural_fingerprint
from app.logging import get_logger

log = get_logger("adapters.registry")


@dataclass(slots=True)
class SchemaMatch:
    """Result of a registry lookup."""

    schema_id: int
    vendor_id: str
    schema_doc: dict[str, Any]
    schema_version: int
    status: str


class SchemaRegistry:
    """DB-backed schema lookup by (vendor_id, structural_fingerprint)."""

    async def lookup(
        self,
        pool: asyncpg.Pool,
        *,
        vendor_id: str,
        payload: dict[str, Any],
    ) -> SchemaMatch | None:
        """Find a matching schema for this vendor+payload shape.

        Computes the structural fingerprint and queries vendor_schemas.
        Returns the best match (active preferred over provisional) or None.
        """
        fp = structural_fingerprint(payload)

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, vendor_id, schema_doc, schema_version, status
                FROM vendor_schemas
                WHERE vendor_id = $1
                  AND structural_fingerprint = $2
                  AND status IN ('provisional', 'active')
                ORDER BY
                    CASE status WHEN 'active' THEN 0 ELSE 1 END,
                    schema_version DESC
                LIMIT 1
                """,
                vendor_id,
                fp,
            )

        if row is None:
            return None

        return SchemaMatch(
            schema_id=row["id"],
            vendor_id=row["vendor_id"],
            schema_doc=row["schema_doc"],
            schema_version=row["schema_version"],
            status=row["status"],
        )

    async def lookup_event_type(
        self,
        pool: asyncpg.Pool,
        *,
        vendor_id: str,
        raw_event_type: str,
    ) -> tuple[str, str] | None:
        """Look up (classification, canonical_state) from vendor_event_type_map.

        Returns (classification, canonical_state) or None if unmapped.
        """
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT classification, canonical_state
                FROM vendor_event_type_map
                WHERE vendor_id = $1 AND raw_event_type = $2
                """,
                vendor_id,
                raw_event_type,
            )

        if row is None:
            return None
        return (row["classification"], row["canonical_state"])

    async def persist_event_type(
        self,
        pool: asyncpg.Pool,
        *,
        vendor_id: str,
        raw_event_type: str,
        classification: str,
        canonical_state: str,
        confidence: float,
        source: str = "llm",
    ) -> None:
        """Persist a new event type mapping. Does not overwrite human-reviewed rows."""
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO vendor_event_type_map
                    (vendor_id, raw_event_type, classification, canonical_state, confidence, source)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (vendor_id, raw_event_type) DO UPDATE
                    SET classification = EXCLUDED.classification,
                        canonical_state = EXCLUDED.canonical_state,
                        confidence = EXCLUDED.confidence,
                        source = EXCLUDED.source
                WHERE NOT vendor_event_type_map.reviewed_by_human
                """,
                vendor_id,
                raw_event_type,
                classification,
                canonical_state,
                confidence,
                source,
            )

    async def increment_success(self, pool: asyncpg.Pool, schema_id: int) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE vendor_schemas SET success_count = success_count + 1 WHERE id = $1",
                schema_id,
            )

    async def increment_failure(self, pool: asyncpg.Pool, schema_id: int) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE vendor_schemas SET failure_count = failure_count + 1 WHERE id = $1",
                schema_id,
            )


registry = SchemaRegistry()
