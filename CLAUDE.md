# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Full stack (postgres + redis + api + worker, auto-migrates)
make up
make down

# Development (requires postgres + redis running)
make api        # uvicorn with --reload
make worker     # arq worker

# Data
make migrate    # apply DB migrations
make seed       # POST the 6 sample payloads to localhost:8000

# Tests
make test           # full suite
make test-unit      # skip e2e (no live DB needed)
make test-e2e       # e2e only (requires docker compose up)

# Single test
python -m pytest tests/test_pipeline.py::test_name -q

# Replay CLI
make replay ARGS="--help"
make replay ARGS="--truncate-projections"

# Code quality
make fmt        # ruff format
make lint       # ruff check
make typecheck  # mypy

# Install deps
make install    # pip install -e ".[dev]"
```

E2e tests are marked with `@pytest.mark.e2e` and skipped when Postgres is unreachable.

## Architecture

The system is split into two planes with different SLOs:

### Plane 1 — Ingestion (synchronous, sub-second)

`POST /webhooks` → [`app/api/receiver.py`](app/api/receiver.py)

Hot path is intentionally thin: read body → size/JSON check → `sha256(canonical_json(payload))` → `INSERT raw_events ON CONFLICT DO NOTHING` → enqueue job → `202`. No LLM, no joins, no business logic. The `event_id` is content-addressed (derived from the payload), so vendor retries are a free no-op at the DB layer and at the queue layer (`_job_id="webhook.process:{event_id}"`).

### Plane 2 — Processing (async worker)

`arq` worker (`app/workers/processor.py`) consumes job ids and calls `process_event` from [`app/workers/pipeline.py`](app/workers/pipeline.py). The queue message is just a pointer — `process_event` re-fetches the payload from `raw_events` so the queue is stateless.

Processing flow:
1. **AdapterRegistry** ([`app/adapters/registry.py`](app/adapters/registry.py)) fingerprints the payload against four deterministic adapters (Maersk, ONE, GlobalFreightPay, MarineTraffic). First match wins.
2. **LLMUniversalAdapter** ([`app/adapters/llm_universal.py`](app/adapters/llm_universal.py)) handles anything that doesn't match, delegating to `LLMFallback`.
3. **LLMFallback** ([`app/llm/fallback.py`](app/llm/fallback.py)) enforces: cache lookup → budget guard → provider call at `temperature=0` → jsonschema validation → one self-correcting retry → audit row. On second failure: `requires_human_review`.
4. **apply_event** ([`app/domain/state_machine.py`](app/domain/state_machine.py)) runs in one Postgres transaction: `SELECT FOR UPDATE` → `INSERT applied_events ON CONFLICT DO NOTHING` (idempotency) → timestamp guard (stale events never walk state backward) → allowed-transition check → update projection.

### Key data contracts

- **`CanonicalEvent`** ([`app/domain/canonical.py`](app/domain/canonical.py)) — the single internal type all adapters and the LLM emit. Downstream (state machine, storage) only ever sees this. It is a tagged union: `CanonicalShipmentEvent | CanonicalInvoiceEvent | CanonicalUnclassifiedEvent`.
- **`Money`** stores currency as ISO-4217 code + integer minor units (no floats).
- **`event_timestamp`** must be timezone-aware on all canonical events.

### State machines

Transition tables live as data in [`app/domain/state_machine.py`](app/domain/state_machine.py) (`SHIPMENT_TRANSITIONS`, `INVOICE_TRANSITIONS`). Initial state `None` is intentionally permissive — first observed event for an entity can be any non-terminal state. `DELIVERED` and `CANCELLED` (shipment) and `VOIDED`/`REFUNDED` (invoice) are terminal with no allowed transitions.

### LLM provider

`LLMProvider` is a Protocol ([`app/llm/provider.py`](app/llm/provider.py)). Uses [`app/llm/openai_provider.py`](app/llm/openai_provider.py) with `response_format=json_object`. The LLM is only invoked for Path B (unknown vendor shapes). Cache key is `(prompt_version, payload_hash, schema_version)` — bumping either version invalidates the cache cleanly. Requires `OPENAI_API_KEY` in `.env`.

### Replay

[`app/tools/replay.py`](app/tools/replay.py) re-runs events from `raw_events` through the **same** `process_event` pipeline — no special code path. `--truncate-projections` rebuilds all state from scratch. The byte-identical-projection invariant is asserted in `tests/test_replay.py`.

### Config

All config via env vars / `.env`, loaded once via `get_settings()` (`lru_cache`). Copy `.env.example` to `.env`. Key vars: `DATABASE_URL`, `REDIS_URL`, `LLM_PROVIDER`, `OPENAI_API_KEY`, `LLM_GLOBAL_DAILY_BUDGET_USD`, `LLM_PER_VENDOR_DAILY_BUDGET_USD`.

### Adding a new deterministic adapter

1. Create `app/adapters/{vendor}_v1.py` implementing `VendorAdapter` (see `app/adapters/base.py`). `matches()` fingerprints the payload; `normalize()` returns `AdapterResult`.
2. Register it in `_DEFAULT_ADAPTERS` in [`app/adapters/registry.py`](app/adapters/registry.py). More specific adapters should appear first.
3. Add fixture payloads and unit tests in `tests/test_adapters.py`.
