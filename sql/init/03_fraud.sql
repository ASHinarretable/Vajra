-- =============================================================================
-- FRAUD LAYER
-- One row per rule firing. A single transaction can trigger multiple rules,
-- so this is intentionally NOT keyed by transaction_id alone.
-- =============================================================================

CREATE TABLE IF NOT EXISTS fraud_events (
    fraud_id          BIGSERIAL PRIMARY KEY,
    transaction_id    TEXT        NOT NULL,
    user_id           TEXT        NOT NULL,
    rule_triggered    TEXT        NOT NULL,   -- e.g. IMPOSSIBLE_TRAVEL
    severity          TEXT        NOT NULL,   -- LOW / MEDIUM / HIGH / CRITICAL
    reason            TEXT,                   -- human-readable explanation
    amount            NUMERIC(14,2),
    city              TEXT,
    event_time        TIMESTAMPTZ,
    detected_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fraud_txn      ON fraud_events (transaction_id);
CREATE INDEX IF NOT EXISTS idx_fraud_user     ON fraud_events (user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_rule     ON fraud_events (rule_triggered);
CREATE INDEX IF NOT EXISTS idx_fraud_severity ON fraud_events (severity);
CREATE INDEX IF NOT EXISTS idx_fraud_time     ON fraud_events (detected_at);
