-- =============================================================================
-- RAW LAYER
-- Append-only landing zone. Every event lands here exactly as received.
-- NEVER updated, NEVER deleted by the pipeline. This is the source of truth
-- for replays, audits, and backfills.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_transactions (
    ingest_id         BIGSERIAL PRIMARY KEY,
    transaction_id    TEXT,
    payload           JSONB        NOT NULL,   -- full original event, untouched
    kafka_topic       TEXT,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_txn_id      ON raw_transactions (transaction_id);
CREATE INDEX IF NOT EXISTS idx_raw_ingested_at ON raw_transactions (ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_payload_gin ON raw_transactions USING GIN (payload);
