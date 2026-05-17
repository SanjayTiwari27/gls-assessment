-- 004_operational_indexes.sql
-- Operational indexes, CHECK constraints, and hardening for production readiness.

BEGIN;

-- ===== CHECK CONSTRAINTS (data integrity) =====

-- Ensure processing_status is always a known value
ALTER TABLE raw_events
    ADD CONSTRAINT raw_events_status_check
    CHECK (processing_status IN ('queued', 'pending_llm', 'processed', 'review', 'failed'));

-- Ensure shipment state is always a known value
ALTER TABLE shipments
    ADD CONSTRAINT shipments_state_check
    CHECK (state IN ('PICKED_UP', 'IN_TRANSIT', 'OUT_FOR_DELIVERY', 'DELIVERED', 'EXCEPTION', 'CANCELLED'));

-- Ensure invoice state is always a known value
ALTER TABLE invoices
    ADD CONSTRAINT invoices_state_check
    CHECK (state IN ('ISSUED', 'PAID', 'VOIDED', 'REFUNDED'));

-- Ensure entity_type is always a known value
ALTER TABLE entities
    ADD CONSTRAINT entities_type_check
    CHECK (entity_type IN ('shipment', 'invoice'));

-- Ensure canonical classification is always a known value
ALTER TABLE canonical_events
    ADD CONSTRAINT canonical_events_classification_check
    CHECK (classification IN ('shipment', 'invoice', 'unclassified'));

-- Ensure confidence is bounded
ALTER TABLE canonical_events
    ADD CONSTRAINT canonical_events_confidence_check
    CHECK (confidence >= 0.0 AND confidence <= 1.0);

-- Ensure version is positive
ALTER TABLE shipments
    ADD CONSTRAINT shipments_version_positive CHECK (version >= 1);
ALTER TABLE invoices
    ADD CONSTRAINT invoices_version_positive CHECK (version >= 1);

-- Ensure amount_minor is non-negative when present
ALTER TABLE invoices
    ADD CONSTRAINT invoices_amount_non_negative
    CHECK (amount_minor IS NULL OR amount_minor >= 0);

-- Ensure vendor_schemas status is valid
ALTER TABLE vendor_schemas
    ADD CONSTRAINT vendor_schemas_status_check
    CHECK (status IN ('provisional', 'active', 'deprecated'));

-- ===== OPERATIONAL INDEXES =====

-- Fast lookup of stale events by entity (ops dashboard)
CREATE INDEX IF NOT EXISTS stale_event_log_entity_idx
    ON stale_event_log (entity_id, observed_at DESC);

-- Fast lookup of canonical events by entity_external_id (entity timeline view)
CREATE INDEX IF NOT EXISTS canonical_events_entity_ext_id_idx
    ON canonical_events ((canonical->>'entity_external_id'), normalized_at DESC);

-- Fast lookup of requires_human_review by capture time (ops queue)
CREATE INDEX IF NOT EXISTS human_review_captured_idx
    ON requires_human_review (captured_at DESC);

-- Partial index for pending events (worker recovery)
CREATE INDEX IF NOT EXISTS raw_events_pending_idx
    ON raw_events (received_at)
    WHERE processing_status IN ('queued', 'pending_llm');

-- Vendor schemas: fast lookup by vendor for admin UI
CREATE INDEX IF NOT EXISTS vendor_schemas_vendor_status_idx
    ON vendor_schemas (vendor_id, status, created_at DESC);

COMMIT;
