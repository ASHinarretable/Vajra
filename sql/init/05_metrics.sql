-- =============================================================================
-- CURATED AGGREGATES (analytics serving layer)
-- These tables are refreshed by Airflow batch jobs (see airflow/dags) and are
-- what Power BI / dashboards point at — keeping heavy aggregation off the
-- hot transactional path.
-- =============================================================================

CREATE TABLE IF NOT EXISTS merchant_metrics (
    merchant            TEXT,
    merchant_category   TEXT,
    window_date         DATE,
    txn_count           BIGINT,
    total_amount        NUMERIC(18,2),
    avg_amount          NUMERIC(14,2),
    success_count       BIGINT,
    failed_count        BIGINT,
    fraud_count         BIGINT,
    updated_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (merchant, window_date)
);

CREATE TABLE IF NOT EXISTS customer_metrics (
    user_id             TEXT,
    window_date         DATE,
    txn_count           BIGINT,
    total_amount        NUMERIC(18,2),
    avg_amount          NUMERIC(14,2),
    distinct_cities     INT,
    distinct_devices    INT,
    fraud_count         BIGINT,
    updated_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, window_date)
);

-- Convenience views for dashboards / quick checks ----------------------------

CREATE OR REPLACE VIEW v_executive_summary AS
SELECT
    count(*)                                                   AS total_transactions,
    count(*) FILTER (WHERE status = 'SUCCESS')                 AS successful,
    count(*) FILTER (WHERE status = 'FAILED')                  AS failed,
    round(100.0 * count(*) FILTER (WHERE status = 'SUCCESS')
          / NULLIF(count(*), 0), 2)                            AS success_rate_pct,
    round(avg(amount), 2)                                      AS avg_amount,
    count(*) FILTER (WHERE is_flagged)                         AS flagged_transactions
FROM processed_transactions;

CREATE OR REPLACE VIEW v_fraud_by_rule AS
SELECT rule_triggered,
       severity,
       count(*)            AS events,
       round(sum(amount), 2) AS amount_at_risk
FROM fraud_events
GROUP BY rule_triggered, severity
ORDER BY events DESC;
