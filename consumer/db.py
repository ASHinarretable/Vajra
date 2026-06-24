"""Postgres persistence for the four data layers.

Thin wrapper over psycopg2 with connect-retry (so the consumer can start before
Postgres finishes booting) and one helper per target table.
"""
from __future__ import annotations

import json
import os
import time

import psycopg2
from psycopg2.extras import Json, execute_values


def connect(retries: int = 30, delay: float = 2.0):
    cfg = dict(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "vajra"),
        user=os.getenv("POSTGRES_USER", "vajra"),
        password=os.getenv("POSTGRES_PASSWORD", "vajra"),
    )
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**cfg)
            conn.autocommit = True
            print(f"[db] connected to postgres at {cfg['host']}:{cfg['port']}", flush=True)
            return conn
        except psycopg2.OperationalError as e:
            last_err = e
            print(f"[db] postgres not ready ({attempt}/{retries})...", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"could not connect to postgres: {last_err}")


def insert_raw(conn, evt: dict, meta: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO raw_transactions
               (transaction_id, payload, kafka_topic, kafka_partition, kafka_offset)
               VALUES (%s, %s, %s, %s, %s)""",
            (evt.get("transaction_id"), Json(evt),
             meta.get("topic"), meta.get("partition"), meta.get("offset")),
        )


def insert_rejected(conn, evt: dict, failed_checks: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO rejected_transactions
               (transaction_id, payload, failed_checks, reason)
               VALUES (%s, %s, %s, %s)""",
            (evt.get("transaction_id"), Json(evt), failed_checks,
             ", ".join(failed_checks)),
        )


def insert_processed(conn, row: dict) -> None:
    cols = list(row.keys())
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO processed_transactions ({", ".join(cols)})
                VALUES ({", ".join(["%s"] * len(cols))})
                ON CONFLICT (transaction_id) DO NOTHING""",
            [row[c] for c in cols],
        )


def insert_fraud_events(conn, rows: list[tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO fraud_events
               (transaction_id, user_id, rule_triggered, severity, reason, amount, city, event_time)
               VALUES %s""",
            rows,
        )
