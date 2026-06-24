-- =============================================================================
-- PROCESSED (CLEANED + CURATED) LAYER
-- Validated, typed, feature-enriched transactions. This is what analytics and
-- downstream ML read from. One row per valid transaction.
-- =============================================================================

CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id        TEXT PRIMARY KEY,
    user_id               TEXT        NOT NULL,
    user_name             TEXT,
    merchant              TEXT,
    merchant_category     TEXT,
    city                  TEXT,
    device                TEXT,
    device_os             TEXT,
    amount                NUMERIC(14,2) NOT NULL,
    currency              TEXT        DEFAULT 'INR',
    payment_method        TEXT,
    transaction_type      TEXT,                 -- P2P / P2M
    status                TEXT,                 -- SUCCESS / FAILED / PENDING
    upi_handle            TEXT,
    payee_vpa             TEXT,
    bank                  TEXT,
    ip_address            TEXT,
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    event_time            TIMESTAMPTZ NOT NULL,

    -- ---- engineered features (inputs for future ML / rules) ----
    txns_last_hour        INT,
    avg_amount_30d        NUMERIC(14,2),
    amount_zscore         NUMERIC(10,4),
    merchant_frequency    INT,
    device_changed        BOOLEAN,
    location_changed      BOOLEAN,
    payment_velocity      INT,                  -- txns in last 60s
    is_weekend            BOOLEAN,
    is_night              BOOLEAN,
    seconds_since_prev    INT,
    km_from_prev          DOUBLE PRECISION,

    is_flagged            BOOLEAN     DEFAULT FALSE,
    processed_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_proc_user       ON processed_transactions (user_id);
CREATE INDEX IF NOT EXISTS idx_proc_event_time ON processed_transactions (event_time);
CREATE INDEX IF NOT EXISTS idx_proc_merchant   ON processed_transactions (merchant);
CREATE INDEX IF NOT EXISTS idx_proc_city       ON processed_transactions (city);
CREATE INDEX IF NOT EXISTS idx_proc_flagged    ON processed_transactions (is_flagged);
