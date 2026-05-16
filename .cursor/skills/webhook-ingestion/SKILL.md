---
name: webhook-ingestion
description: Guides the design, implementation, and review of a production-grade webhook ingestion and normalization system on FastAPI + Postgres + Redis + a queue, where LLMs are used as a fallback parser. Use when working on webhook receivers, vendor event handlers, payload normalization, idempotency, deduplication, entity lifecycle state machines (shipment, invoice), out-of-order or duplicate events, replay/backfill, queue workers, or any code path that receives or processes vendor webhooks.
---

# Webhook Ingestion & Normalization

You are designing/implementing a system that ingests unstructured webhook payloads from many vendors, normalizes them, and maintains entity lifecycle state (shipment, invoice) — under low-latency ingestion constraints, with LLMs available but expensive.

Think like a **distributed systems engineer, backend architect, data engineer, and pragmatic AI integrator**. Not like a prototype builder, a frontend developer, or a "just call GPT" engineer.

## The Five Non-Negotiable Principles

Every design and code change MUST be evaluated against these. If a change violates one, stop and redesign.

1. **Fast Acknowledgment** — Vendors expect sub-second `2xx`. The ingest endpoint does the minimum: authenticate, compute dedupe key, persist raw event, enqueue, return `202`. No DB joins, no LLM, no business logic on the hot path.
2. **Async Processing** — All heavy work (normalization, LLM calls, state transitions, side-effects) runs in workers consuming a queue. The HTTP layer never blocks on it.
3. **Event-Driven Design** — Every webhook is an immutable event appended to a `raw_events` log. Downstream state is a **projection** of that log and must be **replayable** from it.
4. **Idempotency First** — Assume duplicates _will_ happen (vendor retries, at-least-once queues, replays). Every operation (insert, state transition, side-effect) must be safe to apply N times. Dedupe keys are derived deterministically, not generated.
5. **LLM as Fallback, Not Default** — Try deterministic parsers first (vendor adapter → schema mapping → regex/JSONPath). Only call the LLM when deterministic parsing fails or confidence is low. Cache LLM outputs by payload hash + vendor + schema version.

## Reference Architecture (1-line)

`Vendor → HTTPS receiver (FastAPI) → raw_events (Postgres) → queue (Redis/SQS) → worker → normalizer (deterministic → LLM fallback) → entity state machine → projections + outbox`

Read [ARCHITECTURE.md](ARCHITECTURE.md) for component contracts and data flow. Read [PATTERNS.md](PATTERNS.md) for concrete implementations.

## Decision Flow — Before You Write Any Code

Before touching any file in the ingestion path, answer these in order:

1. **Is this code on the hot path (HTTP receiver)?** If yes, it must be O(1) work: auth + hash + insert + enqueue. Anything else belongs in a worker.
2. **Is this operation idempotent?** If you cannot describe the dedupe key and the "second-time" semantics in one sentence, the design is wrong.
3. **Can this event arrive out of order?** If yes, the state transition must be guarded by an event timestamp / sequence — never a "current state" read.
4. **Does this need an LLM?** Default answer is **no**. Justify why a deterministic path cannot handle the vendor/payload before adding an LLM call.
5. **Can this be replayed from `raw_events`?** If a bug ships, you must be able to truncate projections and re-derive them. No state should be born only inside a worker.

## Hot Path vs Deferred Work

| Hot path (≤ ~50ms, in HTTP handler)        | Deferred (worker)                          |
| ------------------------------------------ | ------------------------------------------ |
| Verify signature / shared secret           | Vendor adapter selection                   |
| Compute `event_id` (vendor + payload hash) | Schema normalization                       |
| `INSERT ... ON CONFLICT DO NOTHING` raw    | LLM classification / extraction (fallback) |
| Enqueue job by `event_id`                  | Entity lookup + state transition           |
| Return `202 Accepted`                      | Outbox writes for downstream side-effects  |

**Hot path forbidden list:** LLM calls, joins across entities, external HTTP (other than enqueue), retries with backoff, schema inference, anything > 1 SQL statement against business tables.

## Idempotency Rules

- **Dedupe key** = `sha256(vendor_id || canonical_json(payload) || vendor_event_id_if_present)`. Store it as the primary key (or unique constraint) of `raw_events`. Use `INSERT ... ON CONFLICT DO NOTHING`.
- **Worker idempotency** = state transitions are guarded by `(entity_id, event_timestamp, event_id)`. A worker re-processing the same `event_id` must produce the same final state with no duplicate side-effects.
- **Side-effects** (emails, webhooks-out, ledger writes) go through a **transactional outbox** keyed by `event_id`. Never call external services inline from a worker.
- **Never** trust vendor-supplied IDs alone — vendors reuse, recycle, and collide IDs. Always salt with `vendor_id`.

## Out-of-Order & Duplicate Events

- Each event carries `event_timestamp` (vendor's) and `received_at` (ours). State machines transition based on `event_timestamp`, not arrival order.
- A "stale" event (timestamp older than current state's `last_applied_ts`) is **recorded** (it stays in `raw_events`) but does not move the state. Log it as `stale_event_skipped` — this is normal, not an error.
- Use **monotonic state machines** where possible (e.g., shipment status has a partial order: `created < picked_up < in_transit < out_for_delivery < delivered`). Reject transitions that would move backward unless the event explicitly represents a correction.

## LLM Gating Policy

LLMs are expensive, slow, and non-deterministic. Treat them like a paid external API with a 5% error rate.

**Call the LLM only when ALL of the following are true:**

1. The vendor adapter could not classify or extract required fields.
2. The deterministic confidence score is below threshold (e.g., < 0.8).
3. The `(payload_hash, vendor, schema_version)` is not already in the LLM cache.
4. The event type is in the allow-list of "LLM-eligible" types.

**Mandatory around every LLM call:**

- **Structured output** with a strict JSON schema. Validate before persisting. On validation failure → retry once with the schema error appended → otherwise mark `requires_human_review`.
- **Temperature 0** (or provider equivalent). Determinism > creativity.
- **Cache** the result keyed by `sha256(prompt_template_version || payload_hash || schema_version)`.
- **Record** `llm_provider`, `model`, `prompt_version`, `tokens_in`, `tokens_out`, `latency_ms`, `cost_estimate` against the event. This is non-optional — you must be able to audit every LLM decision.
- **Budget guard**: a per-vendor and global rate limiter. If exceeded, mark events `pending_llm` and process when budget frees, do not drop.

**LLM output is a hypothesis, not a fact.** Persist normalized fields alongside `source = 'llm' | 'deterministic'` and `confidence`. Downstream consumers can choose their tolerance.

## Entity State Machines (Shipment, Invoice)

- Define each entity's states and the allowed transitions as **data** (a table or const map), not scattered `if`s. See [PATTERNS.md](PATTERNS.md).
- Apply transitions inside a single DB transaction that also marks the event as `applied` for that entity. Two workers consuming the same event must result in exactly one applied transition (use `SELECT ... FOR UPDATE` or an `INSERT ... ON CONFLICT` on `(entity_id, event_id)`).
- The state row stores `last_applied_event_id`, `last_applied_ts`, and a `version` integer for optimistic concurrency.
- Projections are rebuildable: there must be a script that, given `raw_events`, regenerates the projection from scratch.

## Schema Normalization Layer

- One **vendor adapter** per vendor. It owns: signature verification, event-type mapping, field extraction, schema version detection.
- Adapters output a **canonical event** (`CanonicalEvent { entity_type, entity_external_id, event_type, event_timestamp, payload_normalized, source, confidence }`). Everything downstream consumes only canonical events.
- Adapters are **pure functions** of `(raw_payload, headers)`. No DB, no network, no LLM inside an adapter — that allows unit-testing every vendor offline with fixtures.
- When a new vendor schema version appears, version the adapter (`adapter_v2`) rather than mutating the existing one. Old events must replay correctly against the version they were ingested with.

## Observability & Replay

- Every event carries a **trace ID** that flows: receiver → queue → worker → state transition → outbox.
- Metrics, per-vendor: ingest QPS, ack latency p50/p95/p99, queue depth, worker lag, LLM-call rate, LLM-cache hit rate, stale-event rate, `requires_human_review` rate.
- **Replay** is a first-class operation. There must be a documented command to: select a time range / vendor / entity from `raw_events`, truncate the affected projection rows, and re-run the worker pipeline. If replay is not possible, the design is incomplete.

## Review Checklist

Before approving any change to the ingestion or normalization path, verify:

- [ ] Hot path does no LLM call, no business join, no external HTTP other than enqueue.
- [ ] Every insert/transition has a clear dedupe key and is safe to apply twice.
- [ ] Out-of-order arrival is handled (timestamps, not arrival order, drive state).
- [ ] LLM is gated by deterministic-first attempt + cache + structured output + audit trail.
- [ ] Adapter is a pure function and has fixture-based unit tests.
- [ ] State machine transitions are data-defined and transactional.
- [ ] The change is replayable from `raw_events` (or replay is explicitly updated).
- [ ] Metrics + trace IDs are emitted for every new code path.
- [ ] No secret material (signing keys, LLM keys) is logged.

## Anti-Patterns (Reject on Sight)

- "Just call the LLM on every payload, it'll figure it out."
- Doing the normalization inline in the HTTP handler.
- Using vendor's `event_id` as the sole dedupe key.
- Reading `current_state` then writing `new_state` in two statements with no row lock or version check.
- Mutating an adapter in place when the vendor changes its schema.
- Storing only the **normalized** event and discarding the raw payload. (You will need it. Always.)
- Calling an external service (email, billing, downstream API) directly from a worker without an outbox.
- Treating queue retries and HTTP retries as separate problems — they share the same idempotency contract.
- Logging entire payloads at INFO level (PII risk, log-bill risk). Use sampled debug or redact.
- Adding a feature flag that bypasses idempotency "just for backfill".

## When You're Unsure

If a requirement seems to push against one of the five principles, **surface the tradeoff explicitly** to the user before implementing — describe the principle at risk, the proposed exception, the blast radius, and a safer alternative. Do not silently weaken the contract.

## Additional Resources

- [ARCHITECTURE.md](ARCHITECTURE.md) — components, contracts, data flow, table shapes.
- [PATTERNS.md](PATTERNS.md) — concrete FastAPI / Postgres / Redis / worker code patterns.
