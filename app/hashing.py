"""Canonical JSON + content-addressed hashing.

The `event_id` of every received webhook is derived from a canonical
serialization of its body (sorted keys, no whitespace), so semantically
identical payloads collide and `INSERT ... ON CONFLICT DO NOTHING` becomes a
free deduplication primitive.
"""

from __future__ import annotations

import hashlib
from typing import Any

import orjson

_RECORD_SEP = b"\x1f"


def canonical_json(obj: Any) -> bytes:
    """Stable, sorted, compact JSON bytes suitable for hashing."""
    return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS)


def sha256_hex(*parts: bytes) -> str:
    h = hashlib.sha256()
    for i, p in enumerate(parts):
        if i:
            h.update(_RECORD_SEP)
        h.update(p)
    return h.hexdigest()


def compute_event_id(payload: Any, vendor_event_id: str | None = None) -> str:
    """Stable id for a received webhook.

    We do not require the vendor to be known at the receiver — the assessment
    spec accepts "any arbitrary JSON". When the vendor itself ships an event id
    we include it so that semantically identical heartbeat payloads from the
    same vendor remain distinguishable across deliveries.
    """
    parts: list[bytes] = [canonical_json(payload)]
    if vendor_event_id:
        parts.append(vendor_event_id.encode("utf-8"))
    return sha256_hex(*parts)


def llm_cache_key(
    prompt_version: str,
    payload: Any,
    target_schema_version: str,
    vendor_scope: str | None = None,
) -> str:
    parts = [
        prompt_version.encode("utf-8"),
        canonical_json(payload),
        target_schema_version.encode("utf-8"),
    ]
    if vendor_scope:
        parts.append(vendor_scope.encode("utf-8"))
    return sha256_hex(*parts)
