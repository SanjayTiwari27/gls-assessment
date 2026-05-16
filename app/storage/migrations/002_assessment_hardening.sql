-- 002_assessment_hardening.sql
-- Interview-assessment hardening:
--   - Persist normalized canonical output for every processed event.
--   - Keep this additive and replay-safe.

BEGIN;

CREATE TABLE IF NOT EXISTS canonical_events (
    event_id        TEXT PRIMARY KEY REFERENCES raw_events(event_id) ON DELETE RESTRICT,
    classification  TEXT NOT NULL,     -- shipment | invoice | unclassified
    vendor_id       TEXT NOT NULL,
    schema_version  TEXT NOT NULL,
    source          TEXT NOT NULL,     -- deterministic | llm | llm_cache
    confidence      DOUBLE PRECISION NOT NULL,
    canonical       JSONB NOT NULL,
    normalized_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS canonical_events_vendor_idx
    ON canonical_events (vendor_id, normalized_at DESC);

CREATE INDEX IF NOT EXISTS canonical_events_classification_idx
    ON canonical_events (classification, normalized_at DESC);

COMMIT;
