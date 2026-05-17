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

### Assumptions

The design rests on the following assumptions about vendor behavior and the
processing environment. They are not invariants the system enforces — if one
becomes false, the corresponding piece of the design has to change.

- **Vendor payload shapes are stable within a version.** For a given vendor,
  the set of JSON keys and their leaf types (string, number, object, array)
  does not change between deliveries of the same event kind. A vendor may
  add new fields (new fingerprint → Path B re-learns), but existing fields
  keep their type and position. _This is what justifies the structural
  fingerprint as a schema identity._

- **A single LLM call produces a correct extraction schema for a new shape.**
  The Schema Discovery prompt (Path B) receives the full payload and must
  produce a declarative `schema_doc` that resolves all required fields on the
  triggering payload. One self-correcting retry is allowed. If both fail, the
  event is marked `requires_human_review` — the system does not guess.

- **Event type vocabulary is finite per vendor.** A vendor's `raw_event_type`
  field (e.g. `"Loaded onboard and sailed"`, `"settled in full"`) draws from
  a bounded set. Once the system has mapped a `(vendor_id, raw_event_type)` →
  `canonical_state`, that mapping holds for all future events with the same
  string. _This is what justifies the `vendor_event_type_map` table as a
  permanent cache._

- **Vendor identity is resolvable before schema lookup.** The worker must
  know `vendor_id` before computing the fingerprint, because the lookup key
  is `(vendor_id, fingerprint)`. Identity comes from: the URL path on
  ingestion (`POST /webhooks/{vendor_id}`), or content inference in the
  worker (e.g. `carrier_scac`, `source`, `issuer`). Two unrelated vendors
  with identical payload shapes get distinct schema rows.

- **The LLM is an expensive, non-deterministic external API.** It is never
  on the hot path (Plane 1) and never on the steady-state processing path
  (Path A). It is invoked only for genuinely new vendor shapes (Path B) and
  genuinely new event type strings (classification). The architecture
  minimizes LLM calls by design: learn once, execute deterministically forever.

- **Schema-driven extraction is a pure function.** Given a `schema_doc` and
  a payload, the `SchemaDrivenAdapter` produces the same `CanonicalEvent`
  deterministically. No network, no randomness, no side effects. This makes
  replay safe and testing trivial.

- **The state machine is the sole owner of projection consistency.** The
  processing plane can feed any `CanonicalEvent` into `apply_event`; the
  state machine enforces idempotency (via `applied_events` PK), out-of-order
  protection (via timestamp guard), and transition validity (via
  `ALLOWED_TRANSITIONS`). The processing plane trusts these guarantees and
  does not re-implement them.

### Scope

Everything between "worker picks up an event_id from the queue" and "projection
row is updated (or event is logged as stale/rejected/review)." The processing
plane is **fully asynchronous** — it has no SLA with the vendor (that's Plane 1's
job) and can take seconds or even minutes for Path B (LLM discovery).

### Invariant

> Given the same `raw_events` log and the same `vendor_schemas` + `vendor_event_type_map`
> state, replaying all events produces byte-identical projections.

This is enforced by:

- `applied_events.vendor_schema_id` recording which schema version was used.
- The replay CLI running the exact same `process_event` code path as live.
- The state machine's idempotency and timestamp guards.

### Pipeline

```
arq worker receives job: event_id
    ↓
Re-fetch payload + headers from raw_events (queue is stateless)
    ↓
Resolve vendor_id:
  • raw_events.vendor_id if set (from /webhooks/{vendor_id})
  • else: content inference (carrier_scac, source, issuer)
    ↓
Compute structural_fingerprint(payload):
  �� Walk JSON tree, collect (path, leaf_type) set
  • SHA-256 of sorted canonical representation
  • Invariant: same shape → same hash regardless of values
    ↓
DB lookup: vendor_schemas WHERE (vendor_id, fingerprint)
           AND status IN ('provisional', 'active')
    ↓
┌─── HIT → Path A (known shape, deterministic, zero LLM) ───┐
│                                                              │
│  Load schema_doc from vendor_schemas row                     │
│      ↓                                                       │
│  Extract raw_event_type from schema_doc.raw_event_type_path  │
│      ↓                                                       │
│  DB lookup: vendor_event_type_map(vendor_id, raw_event_type) │
│      ├── HIT  → (classification, canonical_state)            │
│      └── MISS → heuristic classify + persist mapping         │
│                  (future: LLM Prompt B, tiny, cheap)          │
│      ↓                                                       │
│  SchemaDrivenAdapter.extract(schema_doc, payload, state)     │
│      • resolve entity_external_id (path or template)         │
│      • parse_timestamp (single or fallback paths)            │
│      • parse_money (amount_path)                             │
│      • extract location, reference_ids, linked_references    │
│      ↓                                                       │
│  CanonicalEvent (Shipment | Invoice | Unclassified)          │
│      ↓                                                       │
│  Increment vendor_schemas.success_count                      │
│                                                              │
└──────────────────────────────────────────────────────────────┘
    ↓
┌──�� MISS → Path B (unknown shape, LLM learns once) ──────────┐
│                                                               │
│  LLMUniversalAdapter.normalize(payload, headers, event_id)    │
│      ↓                                                        │
│  LLMFallback orchestrator:                                    │
│      1. Cache lookup (prompt_version, payload_hash, schema)   │
│      2. Budget guard (per-vendor + global daily limit)        │
│      3. OpenAI call (temperature=0, json_object mode)         │
│      4. JSON schema validation against v1_target_schema.json  │
│      5. One self-correcting retry on validation failure       │
│      6. Audit row (tokens, latency, cost, decision)           │
│      ↓                                                        │
│  Build CanonicalEvent from LLM extraction                     │
│      ↓                                                        │
│  (Future: persist schema_doc → vendor_schemas for Path A)     │
│                                                               │
│  OR: budget_exceeded → mark pending_llm, do not drop          │
│  OR: invalid after retry → mark requires_human_review         │
│                                                               │
└───────────────────────────────────────────────────────────────┘
    ↓
Upsert canonical_events row (classification, source, confidence)
    ↓
apply_event (single Postgres transaction):
    1. Get or create entities row (vendor_id, entity_type, external_id)
    2. INSERT applied_events ON CONFLICT DO NOTHING
       → returns NULL on duplicate → "already_applied", stop
    3. INSERT projection (shipments/invoices) ON CONFLICT DO NOTHING
       → if inserted → "applied_initial", done
       → else: race-lost, fall through to existing-row path
    4. SELECT ... FOR UPDATE on projection row
    5. Timestamp guard: event_timestamp < last_applied_ts → stale_event_log
    6. Transition check: target_state ∈ ALLOWED_TRANSITIONS[current_state]
    7. UPDATE projection (state, version++, references merged)
    ↓
Update raw_events.processing_status = 'processed'
    ↓
Emit metrics: vendor, classification, outcome
```

### Edge cases handled

- **Same event processed twice** → `applied_events(entity_id, event_id)` PK
  is the hard idempotency guarantee. Second pass returns `already_applied`
  without touching the projection.

- **Out-of-order arrivals** → timestamp guard compares incoming
  `event_timestamp` against `last_applied_ts` on the projection. Older
  events are logged to `stale_event_log` but never walk state backward.

- **Race between concurrent workers on same entity** → `SELECT ... FOR UPDATE`
  serializes concurrent writes. The `INSERT ... ON CONFLICT DO NOTHING
RETURNING` pattern for initial creation is race-tolerant (loser falls through
  to the locked-read path).

- **Optimistic concurrency** → `WHERE version = $N` on projection UPDATEs.
  A concurrent version bump causes the UPDATE to affect 0 rows (detectable
  if needed for retry, though the `FOR UPDATE` lock makes this mostly
  theoretical).

- **Vendor sends new payload shape** → new fingerprint, no schema match,
  Path B fires. LLM discovers the schema. Future events with the same shape
  → Path A (zero LLM cost).

- **Vendor sends new event type string for a known shape** → Path A fires,
  schema extracts the raw string, `vendor_event_type_map` miss triggers
  heuristic classification + persistence. Future events with the same string
  → cached mapping.

- **LLM budget exhausted** → event marked `pending_llm` in raw_events, not
  silently dropped. A future budget reset or manual intervention retries it.

- **LLM produces invalid output** → one self-correcting retry with the
  validation error appended to the prompt. If still invalid →
  `requires_human_review`. No partial writes.

- **Unknown vendor (no content signals for inference)** → `vendor_id='unknown'`.
  Fingerprint still produces a match if the same shape was seen before.
  Otherwise Path B classifies it.

- **Terminal state receives same-state event** → invoices treat as no-op
  (timestamp + references updated). Shipments reject (terminal states have
  empty transition sets). _This is a known inconsistency — see trade-offs._

---

### Steps to productionize

These are _real gaps_, not deliberate choices. In rough priority order:

#### 1. Schema Discovery auto-persist (highest priority for cost)

**Problem.** Path B (LLM fallback) produces a correct `CanonicalEvent` but
does not persist a `schema_doc` back to `vendor_schemas`. This means the same
vendor+shape will hit the LLM on every future event instead of graduating to
Path A.

**Fix.** Implement `SchemaDiscoverer` (~80 lines):

- After successful LLM extraction, infer a `schema_doc` from the extraction
  result (reverse-map extracted values → payload paths).
- Validate the inferred schema against the triggering payload (all paths
  must resolve to non-null values of the declared type).
- Persist to `vendor_schemas` with `status='provisional'`.
- Persist `raw_event_type → canonical_state` to `vendor_event_type_map`.
- All future events from that shape → Path A.

#### 2. LLM-based event type classification (Prompt B)

**Problem.** When `vendor_event_type_map` misses, the system uses a keyword
heuristic (`_heuristic_classify`). This works for the assessment payloads but
will fail for vendor-specific jargon that doesn't match English keywords.

**Fix.** Implement `Classifier` that calls the LLM with a tiny prompt:

- Input: vendor_id + raw_event_type string + ~200 bytes context.
- Output: `{classification, canonical_state, confidence}`.
- Persist to `vendor_event_type_map` permanently.
- After first mapping, never called again for that `(vendor, event_type)`.
- ~40 tokens input, ~20 tokens output. Cost: <$0.001 per unique event type.

#### 3. Provisional → active schema promotion

**Problem.** LLM-generated schemas start as `status='provisional'` but are
immediately used for extraction. A bad schema (hallucinated path) will
silently produce wrong extractions until `failure_count` alerts an operator.

**Fix.** For the first K events from a provisional schema, run a parallel
one-shot LLM extraction and compare to the `SchemaDrivenAdapter` output.
If agreement rate >95% over K events → promote to `active`. If <95% →
deprecate, retry Path B. K=5 is reasonable for assessment; K=20 for
production.

#### 4. Stuck `pending_llm` event recovery

**Problem.** Events marked `pending_llm` (budget exceeded) sit in raw_events
forever. The receiver dedupes future vendor retries, so they never recover
automatically.

**Fix.** A periodic sweeper job:

```sql
SELECT event_id FROM raw_events
WHERE processing_status = 'pending_llm'
  AND processed_at < now() - interval '1 hour'
```

Re-enqueue these. The budget guard checks daily totals, so the next day's
budget window will allow them through. ~15 lines + one cron job.

#### 5. OpenAI client lifecycle management

**Problem.** `build_default_provider()` creates an `httpx.AsyncClient` but
no shutdown hook closes it. `reset_pipeline_singletons()` drops the reference
without calling `aclose()`.

**Fix.** Register cleanup in worker `shutdown()` and FastAPI `lifespan`.
Call `provider.aclose()` before nulling the singleton. ~10 lines.

#### 6. Schema merge CLI for fingerprint fragmentation

**Problem.** A vendor that sometimes includes an optional field (e.g.
`shipper_ref`) produces two fingerprints for the same logical schema. Each
fingerprint gets its own `vendor_schemas` row. This is correct but
operationally noisy.

**Fix.** Ops CLI command: `merge-schemas --vendor maersk --into <schema_id>`.
Merges `success_count`/`failure_count`, deprecates the source row, updates
the fingerprint index. ~50 lines.

#### 7. Rate limit on Path B (adversarial/noisy vendors)

**Problem.** A vendor sending payloads with random extra keys generates a
new fingerprint per payload → unbounded Path B LLM calls.

**Fix.** Redis counter: `max_provisional_schemas_per_vendor_per_hour = 10`.
Excess events → `requires_human_review`, not LLM. Enforced before the
LLMUniversalAdapter call. ~20 lines.

#### 8. `_mark_review` atomicity

**Problem.** Two `conn.execute()` calls (INSERT requires_human_review +
UPDATE raw_events) run on the same connection but not in an explicit
transaction. If one fails the other persists.

**Fix.** Already fixed in current code — wrapped in `conn.transaction()`.

#### 9. Vendor metric label cardinality

**Problem.** `canonical.vendor_id` from LLM-generated slugs gets used as a
Prometheus label. Unbounded cardinality crashes Prometheus scrapes.

**Fix.** `safe_vendor_label(s: str) -> str` that maps anything not in a
registered-vendor set to `"other"`. Apply to all metric `.labels()` calls.

---

### Trade-offs (deliberate, documented)

These are conscious choices, not gaps:

- **Two-path architecture over single-path LLM.** Path A exists to eliminate
  LLM cost and latency for steady-state traffic. The complexity of maintaining
  `vendor_schemas` + `vendor_event_type_map` is justified because production
  vendors send thousands of events per day with stable shapes — paying for an
  LLM call on each would be 100–1000× more expensive than one DB read.

- **Heuristic classification over LLM for event type resolution (for now).**
  The keyword matcher handles the assessment payloads correctly and costs
  nothing. LLM-based classification (Prompt B) is the production answer but
  isn't needed to demonstrate the architecture.

- **Fingerprint fragmentation accepted over fuzzy matching.** Missing-vs-null
  and optional-field variations produce distinct fingerprints. Each costs one
  extra Path B call to learn. The alternative (fuzzy fingerprint matching)
  risks false schema reuse across genuinely different shapes — worse failure
  mode. Merge CLI handles the ops overhead.

- **No schema versioning within a vendor+fingerprint.** `schema_version`
  exists in the table but is always 1 today. Production needs version bumps
  when a schema is corrected (with replay using the original version for
  historical events). The column and the `applied_events.vendor_schema_id`
  FK are in place for this.

- **Terminal shipment states reject same-state re-deliveries.** A second
  `DELIVERED` notification on an already-delivered shipment returns
  `transition_rejected`, not a no-op. Invoices treat same-state as no-op.
  This inconsistency is intentional for shipments (a second delivery event
  is suspicious and worth flagging) but could be relaxed if vendors prove
  noisy.

- **Content-based vendor inference as fallback.** When `POST /webhooks`
  (without vendor_id path) is used, the worker infers vendor from payload
  fields (carrier_scac, source, issuer). This is fragile for unknown vendors
  but acceptable for the assessment scope where the 4 sample vendors have
  unique fingerprint signals.

- **Schema_doc is a flat path declaration, not a full JSONPath spec.** We
  support `$.a.b.c`, `$.arr[0].field`, `$.arr[].field`, and entity ID
  templates (`{a.b}:{c}`). We do not support filters, recursive descent,
  or computed expressions. This covers all assessment payloads and most
  real-world webhook shapes. Truly exotic structures fall through to Path B
  permanently (acceptable for the long tail).

- **LLM budget guard uses Postgres aggregation, not Redis token buckets.**
  Correct across processes but adds a DB round-trip before every LLM call.
  Acceptable at low LLM call volume (Path B is rare in steady state).
  Production should use Redis token buckets when Path B volume justifies it.

- **No transactional outbox for downstream side-effects.** The processing
  plane writes projections but does not emit events to downstream consumers.
  Adding an outbox table + dispatcher is the standard production extension;
  the TX shape in `apply_event` supports it without modification.
