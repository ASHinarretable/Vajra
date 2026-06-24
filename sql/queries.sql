-- Handy queries to inspect the pipeline once it's running.
-- Connect:  docker exec -it vajra-postgres psql -U vajra -d vajra

-- Executive summary (success rate, avg amount, flagged count)
SELECT * FROM v_executive_summary;

-- Fraud breakdown by rule and severity
SELECT * FROM v_fraud_by_rule;

-- Latest 20 fraud alerts
SELECT detected_at, severity, rule_triggered, user_id, amount, reason
FROM fraud_events
ORDER BY detected_at DESC
LIMIT 20;

-- Layer row counts (raw vs processed vs rejected vs fraud)
SELECT 'raw' AS layer, count(*) FROM raw_transactions
UNION ALL SELECT 'processed', count(*) FROM processed_transactions
UNION ALL SELECT 'rejected', count(*) FROM rejected_transactions
UNION ALL SELECT 'fraud_events', count(*) FROM fraud_events;

-- Top merchants by volume
SELECT merchant, count(*) AS txns, round(sum(amount), 2) AS total
FROM processed_transactions
GROUP BY merchant ORDER BY txns DESC LIMIT 10;

-- Transactions by city
SELECT city, count(*) AS txns FROM processed_transactions
GROUP BY city ORDER BY txns DESC;

-- Detection check: how many injected frauds did we catch, by scenario?
-- (raw payload carries the ground-truth label _injected_fraud)
SELECT r.payload->>'_injected_fraud' AS injected_scenario,
       count(DISTINCT r.transaction_id) AS injected,
       count(DISTINCT f.transaction_id) AS detected
FROM raw_transactions r
LEFT JOIN fraud_events f ON f.transaction_id = r.transaction_id
WHERE r.payload->>'_injected_fraud' IS NOT NULL
GROUP BY 1 ORDER BY 1;
