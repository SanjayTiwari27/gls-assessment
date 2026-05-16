# Reference Architecture

This document defines the components, their contracts, and the data flow. Treat it as the source of truth when designing or extending the system.

## High-Level Data Flow

```
                       (sub-second, lightweight)
  Vendor  ─HTTPS─►  Receiver (FastAPI)
                       │
                       │ 1. verify signature
                       │ 2. compute event_id = sha256(vendor || canonical_payload || vendor_event_id?)
                       │ 3. INSERT raw_events (event_id, vendor, payload, headers, received_at)
                       │       ON CONFLICT (event_id) DO NOTHING
                       │ 4. enqueue(event_id) on Redis/SQS
                       ▼
                    202 Accepted

                       (async, retryable, idempotent)
  Queue ─►  Worker
              │
              │ a. SELECT raw_events WHERE event_id = $1   (immutable input)
              │ b. adapter = VendorAdapterRegistry.for(vendor)
              │ c. canonical_event = adapter.normalize(payload, headers)
              │       └─ on failure / low confidence ──► LLMFallback.classify_extract(...)
              │                                              └─ cached by (prompt_v || payload_hash || schema_v)
              │ d. BEGIN
              │       lookup or create entity row (FOR UPDATE)
              │       guard transition by (event_timestamp, allowed_transitions)
              │       UPDATE entity_state, INSERT applied_events(entity_id, event_id) ON CONFLICT DO NOTHING
              │       INSERT outbox(event_id, kind, payload)
              │    COMMIT
              │ e. ack message
              ▼
        Outbox Dispatcher ─► downstream side-effects (email, partner API, ledger, ...)
```

## Components

### 1. Receiver (FastAPI)

**Responsibility:** authenticate, persist raw event, enqueue. Nothing else.

**Contract:**
- Input: any vendor's webhook HTTP request.
- Output: `202 Accepted` with `{ "event_id": "..." }` within p99 < 250ms.
- Side-effects: 1 row in `raw_events`, 1 enqueue.

**Forbidden in receiver:** business joins, LLM calls, normalization, retries to external services, multi-statement transactions on business tables.

### 2. Vendor Adapter

**Responsibility:** turn a raw payload from one vendor into a `CanonicalEvent`.

**Contract:** pure function `(raw_payload: dict, headers: dict) -> AdapterResult`.

```
AdapterResult = {
  status: "ok" | "needs_llm" | "unsupported",
  canonical_event: CanonicalEvent | None,
  confidence: float in [0.0, 1.0],
  missing_fields: list[str],
  schema_version: str,
}
```

Adapters are organized by `vendor_id` and versioned (`acme_v1`, `acme_v2`). The registry resolves the right one from headers / payload shape. Adapters **do not** touch the DB, network, or LLM.

### 3. Canonical Event

The single internal representation everything downstream depends on:

```
CanonicalEvent = {
  event_id: str,                  # globally unique, == raw_events.event_id
  vendor_id: str,
  entity_type: "shipment" | "invoice",
  entity_external_id: str,        # vendor-scoped
  event_type: str,                # canonical, e.g. "shipment.picked_up"
  event_timestamp: datetime,      # vendor's, UTC
  payload_normalized: dict,       # canonical field names
  source: "deterministic" | "llm",
  confidence: float,
  schema_version: str,
  llm_metadata: { model, prompt_version, tokens_in, tokens_out, cost } | None,
}
```

### 4. LLM Fallback

**Responsibility:** classify event type and/or extract missing fields when the deterministic adapter cannot.

**Contract:**
- Input: `(raw_payload, headers, missing_fields, target_schema)`.
- Output: structured JSON conforming to `target_schema`, or raises `LLMValidationError` after one self-correction retry.
- Cached by `sha256(prompt_template_version || payload_hash || schema_version)`.
- Records `model, prompt_version, tokens_in, tokens_out, latency_ms, cost_estimate, decision` per call.
- Subject to a per-vendor and global budget guard.

The LLM is gated — see SKILL.md "LLM Gating Policy".

### 5. Entity State Machine

**Responsibility:** apply canonical events to entity projections (`shipments`, `invoices`).

**Contract:**
- Transitions are **data-defined** (`ALLOWED_TRANSITIONS: dict[from_state, set[to_state]]`).
- Each transition is applied inside one DB transaction with a row lock or optimistic version check.
- Idempotency is enforced by `applied_events(entity_id, event_id) UNIQUE`.
- Out-of-order events whose `event_timestamp < entity.last_applied_ts` are recorded (in `stale_event_log`) but do not move state.

### 6. Outbox + Dispatcher

**Responsibility:** decouple "we decided X" from "we told the world about X".

- Workers write to `outbox` in the same transaction as the state transition.
- A separate dispatcher polls `outbox` and delivers to downstream services with retry + DLQ.
- Outbox rows are keyed by `event_id + side_effect_kind` to keep delivery idempotent end-to-end.

### 7. Replay Tool

**Responsibility:** re-derive projections from `raw_events` after bugs or schema changes.

- Selects events by vendor / time-range / entity.
- Optionally truncates affected projection rows.
- Pushes events back through the normal worker pipeline (not a separate code path).
- Replays must reach the same final state given the same input — this is the regression test for idempotency.

## Storage Layout (Postgres)

Minimum table set:

- `raw_events` — append-only log. PK `event_id`. Columns: `vendor_id`, `received_at`, `headers jsonb`, `payload jsonb`, `signature_verified bool`. **Never updated, never deleted (except by retention).**
- `vendor_adapters` (optional) — registry/version metadata, or kept in code.
- `applied_events` — idempotency table for state transitions. PK `(entity_id, event_id)`.
- `shipments`, `invoices` — projections. Columns include `last_applied_event_id`, `last_applied_ts`, `version`, plus domain fields. Rebuildable from `raw_events`.
- `stale_event_log` — events received with timestamps older than `last_applied_ts`. For auditability, not error-handling.
- `llm_cache` — keyed by prompt+payload hash; stores normalized output and metadata.
- `outbox` — pending side-effects. Columns: `event_id`, `kind`, `payload`, `status`, `attempts`, `next_attempt_at`.
- `requires_human_review` — events that failed deterministic + LLM paths.

All business tables carry `created_at`, `updated_at`, and (where applicable) `version` for optimistic concurrency.

## Queue Contract

- Job = `{ event_id }`. Workers always re-read the full event from `raw_events` — never trust the queue message body for data.
- At-least-once delivery assumed. Workers MUST be idempotent (see `applied_events`).
- Failed jobs go to DLQ after N attempts with exponential backoff; DLQ items are inspectable and replayable through the same worker code path.

## Failure Modes & How They're Handled

| Failure                                | Handling                                                                                   |
| -------------------------------------- | ------------------------------------------------------------------------------------------ |
| Vendor retries (duplicate event)       | `raw_events` insert is a no-op via `ON CONFLICT`. Worker is idempotent via `applied_events`. |
| Out-of-order delivery                  | Timestamp-guarded transitions; stale events recorded, not applied.                          |
| Adapter can't parse                    | Falls through to LLM if event type allow-listed; otherwise `requires_human_review`.         |
| LLM returns invalid JSON               | One self-correcting retry → otherwise `requires_human_review`. No partial writes.           |
| Worker crashes mid-transaction         | Transaction rolls back; message redelivered; idempotency guards prevent double-apply.       |
| Downstream side-effect (email) fails   | Outbox dispatcher retries; never blocks the worker.                                         |
| Schema change in vendor                | New adapter version added; old events still process via their original adapter version.     |
| Bug in normalization shipped to prod   | Fix → bump adapter version → replay affected `raw_events` slice → projections re-derived.   |

## SLOs (Starting Targets)

- Receiver ack: p95 < 150ms, p99 < 250ms.
- Worker lag (event_id received → applied): p95 < 5s under normal load.
- LLM fallback rate: < 10% of events long-term (push toward < 2% as adapters mature).
- LLM cache hit rate: > 60% within first week of a new vendor.
- Replay-from-raw correctness: 100% — bit-for-bit identical projections.
