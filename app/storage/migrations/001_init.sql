-- 001_init.sql
-- Storage layout for the webhook ingestion service. The intent of this schema
-- is encoded in the constraints (PKs and uniques), not in the application:
--   - raw_events is append-only and content-addressed (event_id = sha256 of payload).
--   - applied_events makes worker re-execution a no-op per (entity, event).
--   - shipments / invoices are PROJECTIONS over raw_events and are rebuildable.

BEGIN;

CREATE TABLE IF NOT EXISTS raw_events (
    event_id          TEXT PRIMARY KEY,
    vendor_id         TEXT,                          -- determined at ingest if known, else by worker
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    headers           JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload           JSONB NOT NULL,
    signature_verified BOOLEAN NOT NULL DEFAULT FALSE,
    processing_status TEXT NOT NULL DEFAULT 'queued',   -- queued | pending_llm | processed | review | failed
    processing_error  TEXT,
    processed_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS raw_events_received_at_idx ON raw_events (received_at);
CREATE INDEX IF NOT EXISTS raw_events_vendor_received_at_idx ON raw_events (vendor_id, received_at);
CREATE INDEX IF NOT EXISTS raw_events_status_idx ON raw_events (processing_status);

-- Universal entity registry. Both shipment and invoice projections point at
-- entries here; we keep one row per (vendor_id, entity_type, external_id).
CREATE TABLE IF NOT EXISTS entities (
    id              BIGSERIAL PRIMARY KEY,
    vendor_id       TEXT NOT NULL,
    entity_type     TEXT NOT NULL,                  -- 'shipment' | 'invoice'
    external_id     TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (vendor_id, entity_type, external_id)
);

CREATE TABLE IF NOT EXISTS shipments (
    entity_id              BIGINT PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
    vendor_id              TEXT NOT NULL,
    external_id            TEXT NOT NULL,
    state                  TEXT NOT NULL,
    last_applied_event_id  TEXT NOT NULL,
    last_applied_ts        TIMESTAMPTZ NOT NULL,
    version                INTEGER NOT NULL DEFAULT 1,
    reference_ids          JSONB NOT NULL DEFAULT '{}'::jsonb,
    location               JSONB,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (vendor_id, external_id)
);

CREATE TABLE IF NOT EXISTS invoices (
    entity_id              BIGINT PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
    vendor_id              TEXT NOT NULL,
    external_id            TEXT NOT NULL,
    state                  TEXT NOT NULL,
    last_applied_event_id  TEXT NOT NULL,
    last_applied_ts        TIMESTAMPTZ NOT NULL,
    version                INTEGER NOT NULL DEFAULT 1,
    currency               TEXT,
    amount_minor           BIGINT,
    due_at                 TIMESTAMPTZ,
    linked_references      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (vendor_id, external_id)
);

-- Idempotency table for state-machine transitions. The composite PK is the
-- hard guarantee: a worker re-processing the same event_id is a no-op.
CREATE TABLE IF NOT EXISTS applied_events (
    entity_id     BIGINT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    event_id      TEXT NOT NULL REFERENCES raw_events(event_id) ON DELETE RESTRICT,
    applied_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    target_state  TEXT NOT NULL,
    PRIMARY KEY (entity_id, event_id)
);

-- Auditability for events that arrive but cannot or should not move state.
CREATE TABLE IF NOT EXISTS stale_event_log (
    id             BIGSERIAL PRIMARY KEY,
    entity_id      BIGINT REFERENCES entities(id) ON DELETE CASCADE,
    event_id       TEXT NOT NULL REFERENCES raw_events(event_id) ON DELETE RESTRICT,
    reason         TEXT NOT NULL,
    detail         JSONB,
    observed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- LLM call cache. Keyed by (prompt_version, payload_hash, target_schema) so
-- that re-deliveries of identical payloads do not double-bill the provider.
CREATE TABLE IF NOT EXISTS llm_cache (
    cache_key       TEXT PRIMARY KEY,
    output          JSONB NOT NULL,
    model           TEXT NOT NULL,
    prompt_version  TEXT NOT NULL,
    schema_version  TEXT NOT NULL,
    tokens_in       INTEGER,
    tokens_out      INTEGER,
    latency_ms      INTEGER,
    cost_estimate   NUMERIC(10, 6),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-call audit log. Kept separate from llm_cache so that cache hits are also
-- auditable and do not collide with cached row.
CREATE TABLE IF NOT EXISTS llm_audit (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL REFERENCES raw_events(event_id) ON DELETE RESTRICT,
    source          TEXT NOT NULL,                  -- 'llm' | 'llm_cache'
    model           TEXT,
    prompt_version  TEXT,
    schema_version  TEXT,
    tokens_in       INTEGER,
    tokens_out      INTEGER,
    latency_ms      INTEGER,
    cost_estimate   NUMERIC(10, 6),
    decision        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS llm_audit_event_idx ON llm_audit (event_id);

-- Events that fail both deterministic and LLM normalization paths.
CREATE TABLE IF NOT EXISTS requires_human_review (
    event_id     TEXT PRIMARY KEY REFERENCES raw_events(event_id) ON DELETE RESTRICT,
    reason       TEXT NOT NULL,
    detail       JSONB,
    captured_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
