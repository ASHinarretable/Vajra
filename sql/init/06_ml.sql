-- =============================================================================
-- ML SCORING COLUMNS
-- Populated in real time by the consumer (scorer.py) and backfilled in bulk by
-- ml/score_backfill.py. fraud_score is the model's fraud probability [0,1];
-- ml_flagged is fraud_score >= ML_THRESHOLD.
-- =============================================================================

ALTER TABLE processed_transactions
    ADD COLUMN IF NOT EXISTS fraud_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ml_flagged  BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_proc_fraud_score ON processed_transactions (fraud_score);
CREATE INDEX IF NOT EXISTS idx_proc_ml_flagged  ON processed_transactions (ml_flagged);
