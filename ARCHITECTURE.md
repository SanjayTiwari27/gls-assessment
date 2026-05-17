# Architecture — production roadmap & trade-offs

This document captures, plane-by-plane, what the system handles today, what it
intentionally does _not_ handle and why, and what work remains before this
service should sit in front of real vendor traffic.

It is a companion to [`README.md`](README.md). The README is the "what and how"
for someone running the service; this file is the "what's still missing and
where the rough edges are" for someone reviewing or operating it.

The system is split into two planes with different SLOs and failure budgets:

1. **Ingestion plane** — `POST /webhooks` → durable raw_events row + queue job.
   Synchronous, sub-second, owns delivery durability.
2. **Processing plane** — worker consumes the queue, normalizes, applies state
   transitions. Asynchronous, owns business correctness.

---

## Plane 1 — Ingestion plane

### Assumptions

The design rests on the following assumptions about vendor behavior and the
operating environment. They are not invariants the system enforces — if one
becomes false, the corresponding piece of the design has to change.

- **Per-vendor type stability.** For a given `(vendor, schema_version, event_kind)`,
  field shapes and types are stable across deliveries. Maersk's `milestone_at`
  is always an ISO-8601 string; GlobalFreightPay's `amount` is always
  `"<currency> <numeric>"` in the same locale; ONE's `milestone_local_time`
  is always `"DD/MM/YYYY HH:MM <tz>"`. _This is what justifies the
  deterministic adapter being a pure function with hardcoded field accessors._
  If a vendor silently changes a field type without bumping its
  `schema_version`, the adapter would mis-parse. Mitigation: the adapter
  contract returns `status=needs_llm` when fields look wrong, the LLM
  fallback handles the drifted payload, and per-vendor unit tests with
  fixture payloads catch regressions in CI.

- **Schema-blind normalization at the receiver is therefore unnecessary.**
  Following from the above: numeric / timestamp / unicode equivalence
  (`42` vs `42.0`, `+08:00` vs `Z`, NFC vs NFD) is a non-issue within a
  vendor's stable schema. Across vendor library upgrades it can drift, but
  the canonical layer in Plane 2 collapses producer drift correctly because
  it parses everything into typed objects (`datetime`, `Decimal`, `Money`)
  before the state machine sees it. The receiver is deliberately bytes-only;
  any pre-hash normalization would risk silent false dedup with no upside
  for known vendors.

- **Vendors are at-least-once, not adversarial.** They will retry on
  transient failure and may send duplicates, but they are not deliberately
  attempting hash collisions, dedup bypasses, or storage exhaustion.
  Adversarial defenses (rate limiting, abuse heuristics, IP allow-listing)
  belong at the LB / WAF tier, not in the receiver.

- **Vendor payloads are bounded.** Real webhook bodies sit comfortably under
  the 1 MB cap. Anything larger is treated as malformed or malicious and
  rejected with `413`.

- **Vendor payloads are JSON objects at the top level.** Arrays, scalars,
  and `null` are rejected with `400`. This is a webhook receiver, not a
  generic JSON sink, and webhook conventions agree on this shape.

- **Vendor event delivery order is arbitrary.** The receiver treats every
  delivery as a candidate "next" event and stores it without sequencing
  logic. Order-tolerance is Plane 2's responsibility, enforced via
  `event_timestamp` guards in the state machine.

- **Postgres is the source of truth; Redis is a cache.** If Postgres is
  unreachable, the receiver correctly fails — it does not buffer in Redis,
  on local disk, or in memory. The receiver's availability ceiling is
  therefore equal to Postgres's availability. Redis can be down (modulo the
  ingest-side atomicity gap noted in "Steps to productionize") without
  affecting receipt correctness once that gap is closed.

- **The receiver is stateless and horizontally scalable.** No in-process
  dedup cache, no consensus protocol. Every dedup decision goes through the
  `raw_events` primary key, which is the only correct serialization point.
  N receivers can run behind a load balancer without coordination.

- **The `vendor_event_id` mixed into the hash is a distinguisher, not an
  authority.** Vendors sometimes reuse ids, sometimes omit them, sometimes
  emit different ids across retries of the same event. We use
  `sha256(canonical_json(payload))` as the primary identity and add
  `vendor_event_id` only when present, to keep heartbeat-style identical
  bodies distinguishable across deliveries.

If any assumption above is broken by a real vendor in production, the fix is
local: a per-vendor adapter change, an extra config knob, or a small Plane 1
tweak. None of them require a redesign.

### Scope

Everything between `request.body()` and the `202` response. The receiver is the
**only** synchronous part of the system. Its job is _receipt durability_, not
business logic. Vendor detection, classification, normalization, LLM calls,
state transitions, and side-effects all live in Plane 2.

### Invariant

> Either the event is durably stored AND the worker will see it, or the
> response is non-2xx.

Whether that invariant actually holds end-to-end is the subject of "Steps to
productionize" below.

### Pipeline

HTTP POST → request.body() # full read into memory → size + content checks # 400 / 413 → orjson parse, must be a JSON object # 400 → vendor_event_id from X-Event-Id or known body keys → event_id = sha256(canonical_json(payload), vendor_event_id?) → optional HMAC verify on /webhooks/{vendor_id} → INSERT raw_events ON CONFLICT (event_id) DO NOTHING RETURNING → if inserted: arq.enqueue_job(job_id="webhook.process:event_id") → 202 { event_id, deduplicated, trace_id }

### Edge cases handled

- Empty body, oversized body, invalid JSON, non-object top-level → cleanly
  4xx with bounded-cardinality metric labels.
- Duplicate vendor deliveries → free no-op via PK + `ON CONFLICT DO NOTHING`;
  receiver returns `deduplicated: true` and does **not** enqueue.
- Concurrent identical deliveries → PK is the serialization point; loser
  returns `deduplicated: true`.
- Whitespace / key-order differences in payload → same `event_id`
  (canonical JSON with sorted keys).
- Heartbeat-style payloads with identical bodies but distinct vendor event ids
  → distinct `event_id` (vendor id mixed into the hash).
- Defense-in-depth at the queue layer: `_job_id="webhook.process:event_id"`
  so even if the receiver enqueued twice, arq deduplicates.
- Trace_id propagated receiver → queue → worker → state transition.
- Constant-time HMAC comparison (`hmac.compare_digest`) when signature
  verification is configured.

---

### Steps to productionize

These are _real gaps_, not deliberate choices. In rough priority order:

#### 1. Receiver-to-queue atomicity (highest priority)

**Problem.** The receiver crosses two systems in sequence: Postgres
(`INSERT raw_events`) then Redis (`arq.enqueue_job`). If the second call fails
or the process dies between them, we have a durably-stored event that no worker
will ever see — and the next vendor retry hashes to the same `event_id` and is
deduped, so it never recovers automatically.

**Three fix levels, ordered by effort vs robustness:**

1. _Stuck-events sweeper._ A periodic job:
   `SELECT event_id FROM raw_events WHERE processing_status='queued'
AND received_at < now() - interval '2 minutes'` and re-enqueue.
   ~30 lines. Cleans up the gap within minutes. Acceptable as week-1 fix.
2. _Ingest-side transactional outbox._ Add an `enqueue_outbox` table; INSERT
   into both `raw_events` and `enqueue_outbox` in the same transaction; a tiny
   dispatcher process pushes to Redis with retries. The receiver only depends
   on Postgres being up. Redis can be down for hours and the system recovers
   when it returns. ~80 lines + one extra table. The right week-3 answer.
3. _Single-system queue._ Use Postgres + `LISTEN/NOTIFY` instead of Redis.
   Strongest atomicity, weakest throughput. Probably overkill for this
   workload, but worth knowing as the option of last resort.

We'd ship (1) immediately and (2) when we needed real Redis-failure tolerance.

#### 2. Hot-path timeouts

**Problem.** No `command_timeout` on the asyncpg pool. No timeout on
`arq.enqueue_job`. A slow Postgres or Redis silently blows the receiver's p99
SLO and ties up worker threads.

**Fix.**

- Set `command_timeout=0.5` (or whatever the SLO budget is) on the asyncpg
  pool.
- Wrap `enqueue_process` in `asyncio.wait_for(..., timeout=0.3)`.
- On either timeout, return `503` with `Retry-After`.
- Define and document the SLO explicitly: e.g. p99 < 250 ms ack latency,
  measured by `webhook_ingest_latency_seconds`.

#### 3. Auto-promote retry-exhausted events to `requires_human_review`

**Problem.** When a worker job exhausts `WORKER_MAX_RETRIES`, arq drops it.
The only trail is `raw_events.processing_status='failed'` with the last error
captured. Nothing automatically lands in `requires_human_review`. This is
where Plane 1 ends and Plane 2 begins, but the symptom is "stuck events" so
operators look here first.

**Fix.** In `app/workers/processor.py`, wrap the job in a retry-exhaustion
detector (arq exposes `ctx['job_try']`). On the final attempt, before
re-raising, INSERT into `requires_human_review` with the failure detail.
Pair with a small DLQ-inspector UI for operators.

#### 4. Streaming size cap

**Problem.** `RECEIVER_MAX_PAYLOAD_BYTES=1_000_000` is checked **after**
`request.body()` reads the entire body into memory. A megabyte cap is small
enough that this is academic, but defense-in-depth says we should reject
oversized requests _during_ the read.

**Fix.** Either configure uvicorn's `--limit-request-body` flag (or the LB
upstream) to reject pre-body, or read the body in chunks with a running total.

#### 5. Replay-window check on signed deliveries

**Problem.** The current HMAC verifier validates the signature against the body
but does not bound _time_. A captured signed delivery can be replayed forever.
Real vendors (Stripe, GitHub) all include an `X-Timestamp` header and require
the receiver to reject if `|now - timestamp| > 5 minutes`.

**Fix.** Add a `webhook_signature_max_skew_s` config knob and a per-vendor
verifier that reads the relevant timestamp header and rejects out-of-window.

#### 6. Per-vendor signature formats

**Problem.** One built-in verifier (`sha256=…` or hex). Real vendors use
several conventions: Stripe `t=…,v1=…`, GitHub `sha256=…`, Slack `v0=…`,
custom canonicalization rules, etc.

**Fix.** Convert `_verify_signature` from a function into a per-vendor
`SignatureVerifier` strategy. Adapter registry already has the right shape;
mirror it.

#### 7. Production secret handling

**Problem.** Vendor secrets live in `WEBHOOK_VENDOR_SECRETS` env. No KMS,
no rotation.

**Fix.** Read secrets through a `SecretSource` interface implemented by an
AWS-Secrets-Manager / GCP-Secret-Manager / Vault client; cache with a short
TTL; rotate by versioning the secret and accepting both versions during a
window.

#### 8. Storage TTL & partitioning

**Problem.** `raw_events` grows unbounded. Cold replay rarely needs events
older than ~90 days.

**Fix.** Range-partition `raw_events` by `received_at` weekly. Detach
partitions older than the retention window, archive them to object storage,
and teach the replay tool to re-hydrate from cold storage transparently.

#### 9. Metric label cardinality guard

**Problem.** Today only the worker uses `vendor_id` as a metric label, and the
worker derives it (bounded set). But `POST /webhooks/{vendor_id}` accepts an
arbitrary string from the URL. Any future change that labels a counter or
histogram with that raw value would create unbounded label cardinality and
crash Prometheus scrapes.

**Fix.** A `safe_vendor_label(s: str) -> str` helper that maps anything not in
the registered-vendor set to `"unknown"`. Use it for _all_ metric labels.

---

### Trade-offs (deliberate, documented)

These are conscious choices, not gaps:

- **Single `POST /webhooks` endpoint** — the spec said "any arbitrary JSON",
  so vendor identity is derived in the worker by `AdapterRegistry`.
  `POST /webhooks/{vendor_id}` exists for vendor-scoped verification but is
  not the canonical route.
- **Content-addressed `event_id`** — `sha256(canonical_json(payload),
vendor_event_id?)`. Survives whitespace, key-order, and most producer
  drift. Theoretical false-dedup risk if a vendor genuinely sends two
  distinct events with byte-identical bodies and no event id; rare in
  practice (real vendors include monotonic ids/timestamps), and the cost
  of a derived dedup key is "vendor retries are free".
- **No numeric / unicode normalization** in canonical JSON. orjson preserves
  int vs float and does not NFC-normalize strings. We accept the small
  false-distinct-hash risk in exchange for not silently rewriting vendor data.
- **No app-level rate limiting or DDoS mitigation.** Belongs at the LB / WAF.
  Not pretending to do it badly here.
- **Signature verification defaults to permissive** (`webhook_signature_enforce=False`)
  so unconfigured vendors are not blocked during onboarding. Production
  deployments should default this to `True` and require an explicit
  per-vendor opt-out.
- **Headers stored as JSONB**, lower-cased, allow-listed to a fixed set
  (`content-type`, `user-agent`, `x-event-id`, `x-request-id`,
  `x-forwarded-for`). We do not preserve every header — vendors send a lot
  of cookies and tracking junk that has no business in our store.
- **No PII redaction in logs** because we don't log payloads at all on the
  hot path. Only `event_id`, `vendor_id`, `trace_id`. PII redaction becomes
  necessary the moment we start logging payload fragments anywhere.
- **No transactional outbox / downstream side-effect dispatch.** Out of scope
  for the assessment; would be the standard production extension.

---

## Plane 2 — Processing plane
