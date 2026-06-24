"""Batch enrichment & data-quality DAG (scaffold).

Airflow ORCHESTRATES — it does not consume the stream. The Kafka consumer runs
continuously as its own service; this DAG runs periodically to refresh the
curated aggregates the dashboards read, and to run data-quality audits.

This is a runnable skeleton: tasks use PostgresOperator-style SQL via
psycopg2 so it works without extra providers. Wire it up once an Airflow
service is added to docker-compose (roadmap item).
"""
from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:  # lets this file be imported/linted without Airflow installed
    AIRFLOW_AVAILABLE = False


REFRESH_MERCHANT_METRICS = """
INSERT INTO merchant_metrics
    (merchant, merchant_category, window_date, txn_count, total_amount,
     avg_amount, success_count, failed_count, fraud_count)
SELECT
    p.merchant,
    max(p.merchant_category),
    date(p.event_time)                                    AS window_date,
    count(*),
    sum(p.amount),
    avg(p.amount),
    count(*) FILTER (WHERE p.status = 'SUCCESS'),
    count(*) FILTER (WHERE p.status = 'FAILED'),
    count(*) FILTER (WHERE p.is_flagged)
FROM processed_transactions p
WHERE p.event_time >= now() - interval '1 day'
GROUP BY p.merchant, date(p.event_time)
ON CONFLICT (merchant, window_date) DO UPDATE SET
    txn_count     = EXCLUDED.txn_count,
    total_amount  = EXCLUDED.total_amount,
    avg_amount    = EXCLUDED.avg_amount,
    success_count = EXCLUDED.success_count,
    failed_count  = EXCLUDED.failed_count,
    fraud_count   = EXCLUDED.fraud_count,
    updated_at    = now();
"""

REFRESH_CUSTOMER_METRICS = """
INSERT INTO customer_metrics
    (user_id, window_date, txn_count, total_amount, avg_amount,
     distinct_cities, distinct_devices, fraud_count)
SELECT
    p.user_id,
    date(p.event_time),
    count(*),
    sum(p.amount),
    avg(p.amount),
    count(DISTINCT p.city),
    count(DISTINCT p.device),
    count(*) FILTER (WHERE p.is_flagged)
FROM processed_transactions p
WHERE p.event_time >= now() - interval '1 day'
GROUP BY p.user_id, date(p.event_time)
ON CONFLICT (user_id, window_date) DO UPDATE SET
    txn_count        = EXCLUDED.txn_count,
    total_amount     = EXCLUDED.total_amount,
    avg_amount       = EXCLUDED.avg_amount,
    distinct_cities  = EXCLUDED.distinct_cities,
    distinct_devices = EXCLUDED.distinct_devices,
    fraud_count      = EXCLUDED.fraud_count,
    updated_at       = now();
"""


def _run_sql(sql: str) -> None:
    import os
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "vajra"),
        user=os.getenv("POSTGRES_USER", "vajra"),
        password=os.getenv("POSTGRES_PASSWORD", "vajra"),
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.close()


if AIRFLOW_AVAILABLE:
    default_args = {
        "owner": "vajra",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id="vajra_batch_enrichment",
        description="Refresh curated aggregate tables for dashboards",
        schedule="*/15 * * * *",          # every 15 minutes
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["vajra", "curated"],
    ) as dag:
        refresh_merchants = PythonOperator(
            task_id="refresh_merchant_metrics",
            python_callable=_run_sql,
            op_args=[REFRESH_MERCHANT_METRICS],
        )
        refresh_customers = PythonOperator(
            task_id="refresh_customer_metrics",
            python_callable=_run_sql,
            op_args=[REFRESH_CUSTOMER_METRICS],
        )
        refresh_merchants >> refresh_customers
