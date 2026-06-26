"""Backfill ML fraud scores onto every historical processed_transaction.

Loads the trained model from MinIO and writes fraud_score + ml_flagged back to
processed_transactions, so dashboards show a complete picture (not just rows
that arrived after the consumer started scoring in real time).

Run:  docker compose --profile ml run --rm ml-backfill
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from ml.store import FEATURES, load_model

THRESHOLD = float(os.getenv("ML_THRESHOLD", "0.8"))
CHUNK = 5000

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    dbname=os.getenv("POSTGRES_DB", "vajra"),
    user=os.getenv("POSTGRES_USER", "vajra"),
    password=os.getenv("POSTGRES_PASSWORD", "vajra"),
)

QUERY = """
SELECT transaction_id,
       amount, amount_zscore, payment_velocity, txns_last_hour,
       merchant_frequency, avg_amount_30d, seconds_since_prev, km_from_prev,
       device_changed::int   AS device_changed,
       location_changed::int AS location_changed,
       is_night::int         AS is_night,
       is_weekend::int       AS is_weekend
FROM processed_transactions
"""

UPDATE = """
UPDATE processed_transactions p
SET fraud_score = v.score, ml_flagged = v.flag
FROM (VALUES %s) AS v(transaction_id, score, flag)
WHERE p.transaction_id = v.transaction_id
"""


def main() -> None:
    booster, meta = load_model()
    if booster is None:
        raise SystemExit("[backfill] no model in MinIO — run ml-train first.")
    print(f"[backfill] loaded model trained_at={meta.get('trained_at')}", flush=True)

    conn = psycopg2.connect(**PG)
    df = pd.read_sql(QUERY, conn)
    print(f"[backfill] scoring {len(df)} rows...", flush=True)

    X = df[FEATURES].to_numpy(dtype="float32")
    proba = booster.inplace_predict(X)
    flags = proba >= THRESHOLD

    rows = list(zip(df["transaction_id"], [float(s) for s in proba],
                    [bool(f) for f in flags]))

    updated = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), CHUNK):
            execute_values(cur, UPDATE, rows[i:i + CHUNK])
            updated += min(CHUNK, len(rows) - i)
        conn.commit()
    conn.close()

    flagged = int(flags.sum())
    print(f"[backfill] updated={updated}  ml_flagged={flagged} "
          f"({100*flagged/max(len(rows),1):.1f}%)", flush=True)


if __name__ == "__main__":
    main()
