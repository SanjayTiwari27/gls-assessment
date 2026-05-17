-- 003_dynamic_adapters.sql
-- DB-driven dynamic adapter system: vendor_schemas + vendor_event_type_map.
-- Replaces the hardcoded adapter registry with a learn-once, execute-forever model.

BEGIN;

CREATE TABLE IF NOT EXISTS vendor_schemas (
    id                       BIGSERIAL PRIMARY KEY,
    vendor_id                TEXT NOT NULL,
    structural_fingerprint   TEXT NOT NULL,
    schema_version           INTEGER NOT NULL DEFAULT 1,
    schema_doc               JSONB NOT NULL,
    status                   TEXT NOT NULL DEFAULT 'provisional',
    created_by               TEXT NOT NULL,
    source_event_id          TEXT REFERENCES raw_events(event_id),
    success_count            BIGINT NOT NULL DEFAULT 0,
    failure_count            BIGINT NOT NULL DEFAULT 0,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    promoted_at              TIMESTAMPTZ,
    UNIQUE (vendor_id, structural_fingerprint, schema_version)
);

CREATE INDEX IF NOT EXISTS vendor_schemas_lookup_idx
    ON vendor_schemas (vendor_id, structural_fingerprint)
    WHERE status IN ('provisional', 'active');

CREATE TABLE IF NOT EXISTS vendor_event_type_map (
    vendor_id           TEXT NOT NULL,
    raw_event_type      TEXT NOT NULL,
    canonical_state     TEXT NOT NULL,
    classification      TEXT NOT NULL,
    confidence          NUMERIC(4, 3),
    source              TEXT NOT NULL,
    reviewed_by_human   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vendor_id, raw_event_type)
);

ALTER TABLE applied_events
    ADD COLUMN IF NOT EXISTS vendor_schema_id BIGINT REFERENCES vendor_schemas(id);

COMMIT;
