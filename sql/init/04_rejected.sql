-- =============================================================================
-- DATA QUALITY QUARANTINE
-- Records that fail validation are NOT dropped silently — they are parked here
-- with the reason(s) they failed, so they can be inspected, fixed, or replayed.
-- =============================================================================

CREATE TABLE IF NOT EXISTS rejected_transactions (
    reject_id         BIGSERIAL PRIMARY KEY,
    transaction_id    TEXT,
    payload           JSONB        NOT NULL,   -- the offending record
    failed_checks     TEXT[]       NOT NULL,   -- list of failed DQ checks
    reason            TEXT,
    rejected_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rejected_txn   ON rejected_transactions (transaction_id);
CREATE INDEX IF NOT EXISTS idx_rejected_time  ON rejected_transactions (rejected_at);
CREATE INDEX IF NOT EXISTS idx_rejected_check ON rejected_transactions USING GIN (failed_checks);
