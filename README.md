# Vajra — Real-Time UPI Payment Analytics & Fraud Detection Platform

A streaming **data platform** that ingests UPI-style transactions, validates and
enriches them, applies an explainable fraud-rules engine, and lands data across
**raw / processed / fraud / quarantine** layers for analytics and future ML.

> This is the data platform that *enables* fraud detection — raw vs curated
> layers, multi-topic Kafka, data-quality quarantine, feature engineering as a
> first-class stage, and batch orchestration around continuous stream consumers.

---

## Architecture

```
 Transaction Generator (Python)
        │  produces (keyed by user_id)
        ▼
   Kafka: transactions.raw
        │
        ▼
 Fraud/Processing Consumer ─────────────► raw_transactions      (RAW layer, never edited)
        │
        ├─ data-quality gate ──fail──────► rejected_transactions (quarantine)
        │
        ├─ feature engineering (stateful, per-user rolling features)
        │
        ├─ fraud rules engine
        │
        ├──► processed_transactions  +  Kafka: transactions.cleaned
        │
        └──► fraud_events            +  Kafka: transactions.suspicious

 Airflow (every 15m) ──► merchant_metrics / customer_metrics ──► Power BI
```

## Tech stack

Python · Kafka (KRaft, no Zookeeper) · PostgreSQL · Docker Compose · Airflow
(scaffold) · PySpark & Power BI (roadmap).

---

## Quick start

```bash
cp .env.example .env          # defaults work as-is
docker compose up --build
```

That starts: Kafka + topic bootstrap, Postgres (schema auto-loaded from
`sql/init/`), the producer, and the consumer. You'll see `[ALERT] ...` lines in
the consumer logs within a few seconds.

### Inspect the data

```bash
docker exec -it vajra-postgres psql -U vajra -d vajra
```

```sql
SELECT * FROM v_executive_summary;
SELECT * FROM v_fraud_by_rule;
-- more in sql/queries.sql
```

### Run the tests

```bash
pip install -r consumer/requirements.txt pytest
pytest -q
```

---

## What's where

| Path | Purpose |
|------|---------|
| `producer/` | Realistic UPI transaction generator + 5 injected fraud scenarios |
| `consumer/` | Validation → features → rules → persistence (the processing path) |
| `sql/init/` | Schema for raw / processed / fraud / rejected / metrics layers |
| `sql/queries.sql` | Ready-made inspection & detection-accuracy queries |
| `airflow/dags/` | Batch enrichment DAG (refreshes curated aggregates) |
| `tests/` | Unit tests for validation, features, rules |

## Fraud scenarios (deterministic, explainable)

| Rule | Trigger |
|------|---------|
| `IMPOSSIBLE_TRAVEL` | Two txns implying >800 km/h between cities |
| `VELOCITY` | ≥4 transactions within 60s |
| `HIGH_VALUE` | Amount ≫ user's own 30-day average (z-score / multiplier) |
| `NEW_DEVICE` | New device **and** new city together |
| `REPEATED_FAILURES` | ≥3 consecutive failures then a success |

The producer tags each injected fraud with a ground-truth label in the raw
payload, so `sql/queries.sql` can measure detection accuracy per scenario.

---

## Roadmap

- [ ] Redis for shared/scalable feature state across multiple consumers
- [ ] Airflow service in `docker-compose` + data-quality audit DAG
- [ ] PySpark batch job for heavy historical feature backfills
- [ ] MinIO (S3) for raw event archival (parquet)
- [ ] Prometheus + Grafana for pipeline health
- [ ] Power BI executive & operations dashboards
- [ ] Dead-letter & alerts topics; ML model on the engineered feature set
