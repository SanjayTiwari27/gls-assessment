"""Structural fingerprinting for dynamic adapter resolution.

Produces a stable hash from a JSON payload's *shape* (keys + leaf types) without
considering values. Two payloads from the same vendor schema produce the same
fingerprint regardless of field values, optional nulls, or array element count.

Edge cases handled:
  1. Same key name, different structure (scalar vs object) → type included in tuple.
  2. Arrays with optional fields → all element keys unioned under path[].
  3. Missing keys vs null keys → distinct fingerprints (accepted fragmentation).
  4. Map-shaped objects with variable keys → caller can supply map_paths to skip.
  5. Vendor identity collision → lookup is always (vendor_id, fingerprint).
  6. Noisy vendors → rate-limited externally, not here.
"""

from __future__ import annotations

import hashlib
from typing import Any

import orjson


def structural_fingerprint(payload: Any, *, map_paths: list[str] | None = None) -> str:
    """Compute a stable structural hash of a JSON payload.

    Args:
        payload: Parsed JSON value (typically a dict from a webhook body).
        map_paths: Optional list of JSON paths (e.g. "$.references") whose child
            keys should be ignored (treated as value-map containers). Used for
            edge case 4 — re-fingerprinting after schema discovery declares
            certain subtrees as maps.

    Returns:
        Hex SHA-256 of the sorted (path, leaf_type) set.
    """
    skip_set = set(map_paths or [])
    paths: set[tuple[str, str]] = set()

    def _walk(node: Any, path: str) -> None:
        if skip_set and path in skip_set:
            # Treat this entire subtree as a single "map" leaf — don't enumerate keys.
            paths.add((path, "map"))
            return

        if isinstance(node, dict):
            paths.add((path, "object"))
            for k in node:
                _walk(node[k], f"{path}.{k}")
        elif isinstance(node, list):
            paths.add((path, "array"))
            for el in node:
                _walk(el, f"{path}[]")
        elif isinstance(node, bool):
            paths.add((path, "boolean"))
        elif isinstance(node, (int, float)):
            paths.add((path, "number"))
        elif isinstance(node, str):
            paths.add((path, "string"))
        elif node is None:
            paths.add((path, "null"))

    _walk(payload, "$")
    canonical = orjson.dumps(sorted(paths), option=orjson.OPT_NON_STR_KEYS)
    return hashlib.sha256(canonical).hexdigest()
