"""Postgres persistence for the demo — connects via a single DATABASE_URL
(the standard connection-string format Neon/Render/Supabase all provide).

Schema is created idempotently on startup (no docker-entrypoint-initdb.d here,
since this deploys to a plain web host, not a container with volume mounts).
"""
from __future__ import annotations

import functools
import json
import os

import psycopg2
from psycopg2.extras import Json as _Json, RealDictCursor

# evt dicts contain a datetime (event_time) — default=str handles it.
Json = functools.partial(_Json, dumps=functools.partial(json.dumps, default=str))

DATABASE_URL = os.environ["DATABASE_URL"]
# Neon/most managed Postgres require SSL; add it if the caller didn't.
if "sslmode" not in DATABASE_URL:
    DATABASE_URL += ("&" if "?" in DATABASE_URL else "?") + "sslmode=require"

# Cap table sizes so a long-running free-tier demo never approaches storage quota.
MAX_ROWS_PER_TABLE = 20_000

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_transactions (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id TEXT, merchant TEXT, merchant_category TEXT, city TEXT,
    device TEXT, amount NUMERIC(14,2), status TEXT, bank TEXT,
    amount_zscore NUMERIC(10,4), payment_velocity INT,
    device_changed BOOLEAN, location_changed BOOLEAN,
    is_flagged BOOLEAN DEFAULT FALSE,
    event_time TIMESTAMPTZ, processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS fraud_events (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT, user_id TEXT, rule_triggered TEXT, severity TEXT,
    reason TEXT, amount NUMERIC(14,2), city TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS rejected_transactions (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT, payload JSONB NOT NULL, failed_checks TEXT[],
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fraud_detected ON fraud_events (detected_at DESC);
"""


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_schema() -> None:
    conn = get_conn()
    with conn, conn.cursor() as cur:
        cur.execute(SCHEMA)
    conn.close()


def insert_raw(conn, evt: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO raw_transactions (transaction_id, payload) VALUES (%s, %s)",
            (evt["transaction_id"], Json(evt)),
        )


def insert_rejected(conn, evt: dict, failed: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO rejected_transactions (transaction_id, payload, failed_checks) VALUES (%s, %s, %s)",
            (evt.get("transaction_id"), Json(evt), failed),
        )


def insert_processed(conn, evt: dict, feats: dict, event_time, flagged: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO processed_transactions
               (transaction_id, user_id, merchant, merchant_category, city, device,
                amount, status, bank, amount_zscore, payment_velocity,
                device_changed, location_changed, is_flagged, event_time)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (transaction_id) DO NOTHING""",
            (evt["transaction_id"], evt["user_id"], evt["merchant"], evt["merchant_category"],
             evt["city"], evt["device"], evt["amount"], evt["status"], evt["bank"],
             feats["amount_zscore"], feats["payment_velocity"],
             feats["device_changed"], feats["location_changed"], flagged, event_time),
        )


def insert_fraud_events(conn, rows: list[tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO fraud_events
               (transaction_id, user_id, rule_triggered, severity, reason, amount, city, detected_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            rows,
        )


def trim_table(conn, table: str, keep: int = MAX_ROWS_PER_TABLE) -> None:
    pk = "id" if table != "processed_transactions" else "transaction_id"
    with conn.cursor() as cur:
        cur.execute(
            f"""DELETE FROM {table} WHERE {pk} IN (
                    SELECT {pk} FROM {table} ORDER BY {pk} DESC OFFSET %s
                )""",
            (keep,),
        )


def stats() -> dict:
    conn = get_conn()
    with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE status='SUCCESS') AS success,
                   count(*) FILTER (WHERE status='FAILED') AS failed,
                   count(*) FILTER (WHERE is_flagged) AS flagged,
                   round(avg(amount)::numeric, 2) AS avg_amount
            FROM processed_transactions
        """)
        summary = cur.fetchone()

        cur.execute("SELECT count(*) AS n FROM rejected_transactions")
        summary["rejected"] = cur.fetchone()["n"]

        cur.execute("""
            SELECT rule_triggered, severity, count(*) AS events
            FROM fraud_events GROUP BY 1, 2 ORDER BY events DESC
        """)
        by_rule = cur.fetchall()

        cur.execute("""
            SELECT detected_at, rule_triggered, severity, user_id, amount, reason
            FROM fraud_events ORDER BY detected_at DESC LIMIT 12
        """)
        recent = cur.fetchall()
    conn.close()
    return {"summary": summary, "by_rule": by_rule, "recent": recent}
