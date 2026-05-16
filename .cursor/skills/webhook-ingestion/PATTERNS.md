# Concrete Patterns

Reference implementations for the most important moving parts. Use these shapes; don't reinvent them per vendor.

## 1. Canonicalizing a Payload for Hashing

Hashing requires a **canonical** byte form so semantically identical payloads collide. Sort keys, no whitespace, UTC timestamps as ISO-8601 strings, stable number formatting.

```python
import hashlib
import json

def canonical_json(obj) -> bytes:
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")

def compute_event_id(vendor_id: str, payload: dict, vendor_event_id: str | None) -> str:
    h = hashlib.sha256()
    h.update(vendor_id.encode("utf-8"))
    h.update(b"\x1f")
    h.update(canonical_json(payload))
    if vendor_event_id:
        h.update(b"\x1f")
        h.update(vendor_event_id.encode("utf-8"))
    return h.hexdigest()
```

Why include `vendor_event_id` when present: two different webhook deliveries can carry identical payloads (e.g., periodic heartbeats) and should be distinct events.

## 2. FastAPI Receiver (Hot Path)

```python
from fastapi import APIRouter, Request, HTTPException, Header
from app.db import db_pool
from app.queue import enqueue
from app.security import verify_signature
from app.hashing import compute_event_id

router = APIRouter()

@router.post("/webhooks/{vendor_id}", status_code=202)
async def receive(
    vendor_id: str,
    request: Request,
    x_signature: str | None = Header(default=None),
):
    raw = await request.body()
    if not verify_signature(vendor_id, raw, x_signature):
        raise HTTPException(status_code=401, detail="bad signature")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid json")

    vendor_event_id = payload.get("id") or request.headers.get("x-event-id")
    event_id = compute_event_id(vendor_id, payload, vendor_event_id)

    async with db_pool.acquire() as conn:
        inserted = await conn.fetchval(
            """
            INSERT INTO raw_events (event_id, vendor_id, payload, headers, received_at)
            VALUES ($1, $2, $3::jsonb, $4::jsonb, now())
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            event_id, vendor_id, payload, dict(request.headers),
        )

    if inserted is not None:
        await enqueue("webhook.process", {"event_id": event_id})

    return {"event_id": event_id, "deduplicated": inserted is None}
```

Notes:
- Signature check fails fast with 401 (no row written).
- Enqueue only happens when the row was actually inserted — duplicate deliveries are silent no-ops.
- The handler is `async` end-to-end. No sync I/O.
- No `try/except` around business logic — there is none on this path.

## 3. Vendor Adapter (Pure Function)

```python
from dataclasses import dataclass
from typing import Literal

@dataclass
class CanonicalEvent:
    event_id: str
    vendor_id: str
    entity_type: Literal["shipment", "invoice"]
    entity_external_id: str
    event_type: str
    event_timestamp: datetime
    payload_normalized: dict
    source: Literal["deterministic", "llm"]
    confidence: float
    schema_version: str

@dataclass
class AdapterResult:
    status: Literal["ok", "needs_llm", "unsupported"]
    canonical_event: CanonicalEvent | None
    confidence: float
    missing_fields: list[str]
    schema_version: str

class AcmeAdapterV1:
    vendor_id = "acme"
    schema_version = "acme_v1"

    EVENT_TYPE_MAP = {
        "PICKED_UP": "shipment.picked_up",
        "IN_TRANSIT": "shipment.in_transit",
        "DELIVERED": "shipment.delivered",
    }

    def normalize(self, payload: dict, headers: dict, event_id: str) -> AdapterResult:
        vendor_type = payload.get("status")
        canonical_type = self.EVENT_TYPE_MAP.get(vendor_type)

        ext_id = payload.get("tracking_number")
        ts = payload.get("event_time")

        missing = [k for k, v in {
            "event_type": canonical_type,
            "entity_external_id": ext_id,
            "event_timestamp": ts,
        }.items() if not v]

        if missing:
            return AdapterResult(
                status="needs_llm",
                canonical_event=None,
                confidence=0.0,
                missing_fields=missing,
                schema_version=self.schema_version,
            )

        return AdapterResult(
            status="ok",
            canonical_event=CanonicalEvent(
                event_id=event_id,
                vendor_id=self.vendor_id,
                entity_type="shipment",
                entity_external_id=ext_id,
                event_type=canonical_type,
                event_timestamp=parse_iso(ts),
                payload_normalized={"raw_status": vendor_type},
                source="deterministic",
                confidence=1.0,
                schema_version=self.schema_version,
            ),
            confidence=1.0,
            missing_fields=[],
            schema_version=self.schema_version,
        )
```

Test it with fixture files only (`tests/fixtures/acme/*.json`) — no DB, no network.

## 4. LLM Fallback With Cache, Schema, and Audit

```python
class LLMFallback:
    CONFIDENCE_FLOOR = 0.8

    def __init__(self, provider, cache, budget_guard, audit_log):
        self.provider = provider
        self.cache = cache
        self.budget = budget_guard
        self.audit = audit_log

    async def classify_extract(
        self,
        *,
        event_id: str,
        vendor_id: str,
        payload: dict,
        target_schema: dict,
        prompt_version: str,
    ) -> dict:
        cache_key = sha256_key(prompt_version, payload, target_schema)
        cached = await self.cache.get(cache_key)
        if cached is not None:
            await self.audit.record(event_id, source="llm_cache", **cached["meta"])
            return cached["data"]

        if not await self.budget.allow(vendor_id):
            raise BudgetExceeded(vendor_id)

        prompt = build_prompt(prompt_version, payload, target_schema)
        result = await self.provider.complete(
            prompt=prompt,
            schema=target_schema,
            temperature=0,
        )

        try:
            data = validate_against_schema(result.text, target_schema)
        except SchemaError as e:
            retry = await self.provider.complete(
                prompt=prompt + "\n\nThe previous output failed validation: " + str(e),
                schema=target_schema,
                temperature=0,
            )
            data = validate_against_schema(retry.text, target_schema)

        meta = {
            "model": result.model,
            "prompt_version": prompt_version,
            "tokens_in": result.tokens_in,
            "tokens_out": result.tokens_out,
            "latency_ms": result.latency_ms,
            "cost_estimate": result.cost_estimate,
        }
        await self.cache.set(cache_key, {"data": data, "meta": meta})
        await self.audit.record(event_id, source="llm", **meta)
        return data
```

Rules baked in:
- Cache before call; cache after success.
- Budget guard before any network call.
- Temperature 0.
- Schema validation with exactly one self-correcting retry.
- Every call audited per `event_id`, including cache hits.

## 5. State Machine as Data + Transactional Application

```python
ALLOWED_TRANSITIONS = {
    "shipment": {
        None:               {"created", "picked_up"},
        "created":          {"picked_up", "cancelled"},
        "picked_up":        {"in_transit", "cancelled"},
        "in_transit":       {"out_for_delivery", "exception", "cancelled"},
        "out_for_delivery": {"delivered", "exception"},
        "exception":        {"in_transit", "delivered", "cancelled"},
        "delivered":        set(),
        "cancelled":        set(),
    },
}

EVENT_TO_STATE = {
    "shipment.created":          "created",
    "shipment.picked_up":        "picked_up",
    "shipment.in_transit":       "in_transit",
    "shipment.out_for_delivery": "out_for_delivery",
    "shipment.delivered":        "delivered",
    "shipment.exception":        "exception",
    "shipment.cancelled":        "cancelled",
}

async def apply_event(conn, ev: CanonicalEvent) -> str:
    target_state = EVENT_TO_STATE[ev.event_type]

    async with conn.transaction():
        row = await conn.fetchrow(
            """
            SELECT id, state, last_applied_ts, version
            FROM shipments
            WHERE vendor_id = $1 AND external_id = $2
            FOR UPDATE
            """,
            ev.vendor_id, ev.entity_external_id,
        )

        if row is None:
            entity_id = await conn.fetchval(
                """
                INSERT INTO shipments (vendor_id, external_id, state, last_applied_event_id, last_applied_ts, version)
                VALUES ($1, $2, $3, $4, $5, 1)
                ON CONFLICT (vendor_id, external_id) DO NOTHING
                RETURNING id
                """,
                ev.vendor_id, ev.entity_external_id, target_state, ev.event_id, ev.event_timestamp,
            )
            if entity_id is None:
                row = await conn.fetchrow(
                    "SELECT id, state, last_applied_ts, version FROM shipments WHERE vendor_id=$1 AND external_id=$2 FOR UPDATE",
                    ev.vendor_id, ev.entity_external_id,
                )

        if row is not None:
            applied = await conn.fetchval(
                """
                INSERT INTO applied_events (entity_id, event_id)
                VALUES ($1, $2)
                ON CONFLICT (entity_id, event_id) DO NOTHING
                RETURNING entity_id
                """,
                row["id"], ev.event_id,
            )
            if applied is None:
                return "already_applied"

            if ev.event_timestamp < row["last_applied_ts"]:
                await conn.execute(
                    "INSERT INTO stale_event_log (entity_id, event_id, reason) VALUES ($1,$2,$3)",
                    row["id"], ev.event_id, "older_than_last_applied",
                )
                return "stale_skipped"

            if target_state not in ALLOWED_TRANSITIONS["shipment"][row["state"]]:
                await conn.execute(
                    "INSERT INTO stale_event_log (entity_id, event_id, reason) VALUES ($1,$2,$3)",
                    row["id"], ev.event_id, f"disallowed_transition:{row['state']}->{target_state}",
                )
                return "transition_rejected"

            await conn.execute(
                """
                UPDATE shipments
                SET state = $1,
                    last_applied_event_id = $2,
                    last_applied_ts = $3,
                    version = version + 1,
                    updated_at = now()
                WHERE id = $4 AND version = $5
                """,
                target_state, ev.event_id, ev.event_timestamp, row["id"], row["version"],
            )

        await conn.execute(
            """
            INSERT INTO outbox (event_id, kind, payload, status)
            VALUES ($1, $2, $3::jsonb, 'pending')
            ON CONFLICT (event_id, kind) DO NOTHING
            """,
            ev.event_id, f"shipment.transitioned_to.{target_state}", {"entity_external_id": ev.entity_external_id},
        )

    return "applied"
```

Key properties:
- One transaction: lock entity → check idempotency → apply transition → write outbox.
- `applied_events (entity_id, event_id) UNIQUE` is the hard idempotency guarantee.
- Out-of-order events are logged but never move state backward.
- Disallowed transitions are recorded, not raised — they're data quality signals, not bugs.

## 6. Worker Loop Skeleton

```python
async def worker(queue, db_pool, registry, llm):
    async for msg in queue.consume("webhook.process"):
        event_id = msg.body["event_id"]
        try:
            async with db_pool.acquire() as conn:
                raw = await conn.fetchrow(
                    "SELECT vendor_id, payload, headers FROM raw_events WHERE event_id = $1",
                    event_id,
                )
                if raw is None:
                    await msg.ack()
                    continue

                adapter = registry.for_vendor(raw["vendor_id"], raw["payload"], raw["headers"])
                result = adapter.normalize(raw["payload"], raw["headers"], event_id)

                if result.status == "needs_llm":
                    extracted = await llm.classify_extract(
                        event_id=event_id,
                        vendor_id=raw["vendor_id"],
                        payload=raw["payload"],
                        target_schema=adapter.target_schema(),
                        prompt_version=adapter.prompt_version,
                    )
                    canonical = adapter.build_canonical_from_llm(extracted, raw, event_id)
                elif result.status == "ok":
                    canonical = result.canonical_event
                else:
                    await mark_requires_review(conn, event_id, reason="unsupported")
                    await msg.ack()
                    continue

                await apply_event(conn, canonical)

            await msg.ack()
        except Exception as e:
            await msg.nack(retry=True)
            log.exception("worker_failed", event_id=event_id, error=str(e))
```

Always re-fetch the event from `raw_events`. The queue message is just a pointer; the source of truth is the log.

## 7. Replay From `raw_events`

```python
async def replay(db_pool, queue, *, vendor_id=None, since=None, until=None, entity_external_id=None):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT event_id FROM raw_events
            WHERE ($1::text IS NULL OR vendor_id = $1)
              AND ($2::timestamptz IS NULL OR received_at >= $2)
              AND ($3::timestamptz IS NULL OR received_at < $3)
              AND ($4::text IS NULL OR payload->>'tracking_number' = $4)
            ORDER BY received_at ASC
            """,
            vendor_id, since, until, entity_external_id,
        )

    for r in rows:
        await queue.enqueue("webhook.process", {"event_id": r["event_id"]})
```

Replay re-uses the normal worker path. There is no special "replay mode" in the worker — that's the whole point: any pipeline change must work for live and replay traffic identically.

## 8. Outbox Dispatcher (Sketch)

```python
async def outbox_dispatcher(db_pool, sender):
    while True:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_id, kind, payload, attempts FROM outbox
                WHERE status = 'pending' AND next_attempt_at <= now()
                ORDER BY created_at ASC
                LIMIT 100
                FOR UPDATE SKIP LOCKED
                """,
            )
            for r in rows:
                try:
                    await sender.deliver(r["kind"], r["payload"], idempotency_key=f"{r['event_id']}:{r['kind']}")
                    await conn.execute("UPDATE outbox SET status='sent' WHERE event_id=$1 AND kind=$2", r["event_id"], r["kind"])
                except TransientError:
                    backoff = min(2 ** r["attempts"], 600)
                    await conn.execute(
                        "UPDATE outbox SET attempts = attempts + 1, next_attempt_at = now() + ($1 || ' seconds')::interval WHERE event_id=$2 AND kind=$3",
                        str(backoff), r["event_id"], r["kind"],
                    )
        await asyncio.sleep(0.5)
```

`FOR UPDATE SKIP LOCKED` lets you horizontally scale the dispatcher with no extra coordination. Always include an `idempotency_key` when calling downstream — your guarantees stop at your edge otherwise.

## 9. Testing Strategy

- **Adapter tests** — pure unit tests over fixture payloads. Cover every event-type mapping and every "needs_llm" branch.
- **Idempotency tests** — feed the same event 1× and N× into the worker; assert identical final projection and exactly one row in `applied_events`, exactly one row in `outbox`.
- **Out-of-order tests** — feed events in reverse timestamp order; assert state ends in the latest event's state and stale events are logged.
- **Replay tests** — load a fixture sequence into `raw_events`, run worker forward; truncate projections; replay; assert byte-identical projections.
- **LLM fallback tests** — stub the LLM to return (a) valid JSON, (b) invalid-then-valid JSON, (c) always-invalid JSON; assert no partial writes in case (c) and `requires_human_review` set.
- **Load tests** — receiver under N rps with payloads of various sizes; assert p99 ack < 250ms.
