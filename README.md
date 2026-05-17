# AI Webhook Ingestion & Normalization Service

## Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PLANE 1 — INGESTION (sync, <100ms)                │
│                                                                             │
│  Vendor POST ──► FastAPI /webhooks/{vendor_id}                              │
│                       │                                                     │
│                       ├─ Reject: >1MB body (413)                            │
│                       ├─ Reject: invalid JSON (400)                         │
│                       ├─ HMAC signature verify (optional enforce)            │
│                       │                                                     │
│                       ▼                                                     │
│              event_id = sha256(canonical_json(payload))                      │
│                       │                                                     │
│                       ▼                                                     │
│        INSERT raw_events ON CONFLICT DO NOTHING ──► deduplicated? done      │
│                       │                                                     │
│                       ▼                                                     │
│        Enqueue job (event_id only) ──► Redis/arq ──► 202 Accepted           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                      PLANE 2 — PROCESSING (async worker)                    │
│                                                                             │
│  Worker picks job_id ──► re-fetch payload from raw_events                   │
│                       │                                                     │
│                       ▼                                                     │
│         Compute structural_fingerprint(payload)                             │
│         = sha256(sorted (json_path, leaf_type) pairs)                       │
│                       │                                                     │
│         ┌─────────────┴──────────────┐                                      │
│         │  vendor_schemas DB lookup   │                                      │
│         │  WHERE vendor_id + fingerprint                                    │
│         └─────────────┬──────────────┘                                      │
│                       │                                                     │
│              HIT?─────┼─────MISS?                                           │
│               │       │       │                                             │
│               ▼       │       ▼                                             │
│  ┌─────────────────┐  │  ┌─────────────────────────────────────────┐        │
│  │ PATH A          │  │  │ PATH B                                  │        │
│  │ (deterministic) │  │  │ (LLM fallback — learns once)            │        │
│  │                 │  │  │                                         │        │
│  │ schema_doc      │  │  │ cache lookup ──► budget guard ──►       │        │
│  │ templates +     │  │  │ OpenAI gpt-4o-mini (temp=0) ──►        │        │
│  │ jmespath-style  │  │  │ jsonschema validate ──►                 │        │
│  │ extraction      │  │  │ 1 self-correct retry if invalid ──►     │        │
│  │                 │  │  │ audit row                               │        │
│  │ event_type via  │  │  │                                         │        │
│  │ vendor_event_   │  │  │ ┌─── Schema Discovery ───┐              │        │
│  │ type_map        │  │  │ │ reverse-map LLM output │              │        │
│  │                 │  │  │ │ to payload paths ──►   │              │        │
│  │ Cost: $0        │  │  │ │ persist schema_doc     │              │        │
│  └────────┬────────┘  │  │ │ to vendor_schemas     │              │        │
│           │           │  │ └────────────────────────┘              │        │
│           │           │  │                                         │        │
│           │           │  │ Cost: ~$0.0003/event (one-time)         │        │
│           │           │  └────────────┬────────────────────────────┘        │
│           │           │               │                                     │
│           ▼           │               ▼                                     │
│         CanonicalEvent (tagged union)                                       │
│         = CanonicalShipmentEvent | CanonicalInvoiceEvent                    │
│           | CanonicalUnclassifiedEvent                                      │
│                       │                                                     │
│                       ▼                                                     │
│              Normalizer                                                     │
│              • timestamps → UTC                                             │
│              • money → integer minor units                                  │
│              • confidence → [0, 1]                                          │
│              • vendor_id override (pipeline > LLM)                          │
│                       │                                                     │
│                       ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │            STATE MACHINE (single Postgres TX)                 │           │
│  │                                                              │           │
│  │  1. SELECT entity FOR UPDATE (serialize)                     │           │
│  │  2. INSERT applied_events ON CONFLICT DO NOTHING             │           │
│  │     → already applied? exit (idempotent)                     │           │
│  │  3. Timestamp guard: event_ts < last_applied_ts?             │           │
│  │     → stale_event_log, skip                                  │           │
│  │  4. Transition check: target_state in allowed(current)?      │           │
│  │     → no: log + reject                                       │           │
│  │  5. UPDATE projection (version + 1)                          │           │
│  └──────────────────────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### State Machines

```
SHIPMENT:
  None ──► PICKED_UP ──► IN_TRANSIT ──► OUT_FOR_DELIVERY ──► DELIVERED (terminal)
                │              │                │
                │              ▼                ▼
                ├────────► EXCEPTION ◄──────────┘
                │              │
                │              ├──► IN_TRANSIT (recovery)
                │              ├──► OUT_FOR_DELIVERY
                │              ├──► DELIVERED
                │              └──► CANCELLED (terminal)
                │
                └────────► CANCELLED (terminal)

INVOICE:
  None ──► ISSUED ──► PAID ──► REFUNDED (terminal)
               │
               └──► VOIDED (terminal)
```

---

## Design Choices

| Decision | Reasoning |
|----------|-----------|
| **Content-addressed event_id** | `sha256(canonical_json(payload))` — sorted keys, tight separators. Dedup is a DB primitive (`ON CONFLICT DO NOTHING`), not application logic. Survives restarts, replays, multi-process. |
| **Structural fingerprinting** | SHA-256 of `(json_path, leaf_type)` set. Values ignored — only shape matters. Same shape → same schema → deterministic extraction. |
| **LLM as one-time teacher** | Unknown shape triggers 1 LLM call ($0.0003). Schema Discovery reverse-maps output to payload paths, persists `schema_doc`. All future events with same fingerprint → Path A (zero cost). |
| **Schema Discovery over hardcoded adapters** | No per-vendor code. `schema_doc` is declarative JSONB: path templates, type hints, separator rules. `SchemaDrivenAdapter` executes it as a pure function. New vendors onboard by learning, not coding. |
| **Two-prompt LLM design** | Prompt A: full extraction (classification + all fields). Prompt B: lightweight event-type classifier (~40 tokens, $0.0002). Each cached permanently per unique input. |
| **temperature=0 + jsonschema validation** | Maximizes reproducibility. Schema validation catches hallucinations. One self-correcting retry with error feedback before failing to human review. |
| **FOR UPDATE serialization** | Row-level lock on projection during state machine TX. No optimistic-retry loops, no lost updates, no phantom reads. |
| **Timestamp guard over vector clocks** | Simple and sufficient: `event_ts < last_applied_ts` rejects late arrivals. No global ordering needed — each entity is independent. |
| **Queue is stateless** | Job payload is just `event_id`. Worker re-fetches from `raw_events`. Queue loss = re-enqueue from DB (replay tool). No message schema drift. |
| **Vendor_id as part of entity identity** | `entities(vendor_id, entity_type, external_id)` — same BL number from two vendors creates two entities. Prevents cross-vendor pollution. |

---

## Trade-offs

| What | Traded for | Consequence |
|------|-----------|-------------|
| **Fingerprint strictness** | Correctness over efficiency | Optional fields create new fingerprints (each costs one Path B call). Accepted: false schema reuse is worse than an extra $0.0003 call. |
| **Provisional schemas used immediately** | Fast convergence over safety | Inferred schemas are used without human validation. Mitigated by: validation at discovery time + failure_count tracking + fallback to Path B on extraction failure. |
| **arq/Redis over Kafka** | Simplicity over scale | One `docker compose up`. Job contract is "process this event_id" — swappable to SQS/Kafka without touching pipeline logic. |
| **No transactional outbox** | Dev speed over delivery guarantee | Enqueue happens after DB insert. Crash between insert and enqueue = stuck event. Mitigated by: pending-status index + replay tool recovers them. |
| **Daily budget via Postgres SUM** | Correctness over throughput | Aggregates `llm_audit` per day. Works across processes. Not as fast as Redis token bucket but sufficient for assessment-scale. |
| **Single-process worker** | Simplicity over throughput | One arq worker. Scale-out = add workers (stateless). No partitioning needed yet. |
| **No multi-tenancy** | Speed over production readiness | Single namespace. Production needs `org_id` column + RLS on every table + per-tenant budgets. |
| **Flat SQL migrations** | Zero dependencies over convenience | No Alembic/Flyway. Tiny `schema_migrations` table. Adequate for < 50 migrations. |

---

## Further Upgrades

**Reliability:**
- Transactional outbox pattern (eliminate enqueue-after-commit gap)
- Dead letter queue inspector UI with one-click replay
- Partitioned `raw_events` by `received_at` (weekly, archive to S3 after 90d)
- Blue/green replay: replay into shadow projections, diff, swap atomically

**Schema system:**
- Provisional → active promotion pipeline (compare LLM vs SchemaDrivenAdapter for K events, promote on agreement)
- Fingerprint merge tool (combine optional-field variants into one schema with nullable paths)
- Schema versioning with backward compatibility checks
- Admin UI for manual schema editing and human-review resolution

**Scale:**
- Replace arq with SQS/Kafka for partitioned processing
- Redis token-bucket budget guard (sub-ms vs current Postgres SUM)
- Connection pooling via PgBouncer
- Read replicas for query endpoints
- Horizontal worker scaling with entity-level partitioning (consistent hash on entity_id)

**Security:**
- Per-vendor HMAC secrets in KMS (not env vars)
- mTLS between worker and Postgres
- Multi-tenancy: `org_id` + RLS + per-tenant LLM budgets
- Payload encryption at rest

**Observability:**
- Cost & quality dashboards (LLM spend, cache-hit rate, stale rate, human-review rate)
- Alerting on: budget exhaustion, human-review queue growth, schema failure_count spikes
- Distributed tracing (OpenTelemetry spans across receiver → queue → worker → state machine)

---

## Run

```bash
cp .env.example .env       # set OPENAI_API_KEY
make up                    # postgres + redis + api + worker (auto-migrates)
make seed                  # POST sample payloads
make test                  # full suite
```

## Stack

FastAPI · Postgres 16 (asyncpg) · Redis 7 (arq) · Pydantic v2 · structlog · Prometheus · OpenAI GPT-4o-mini (fallback only)
