"""Train the fraud XGBoost model and publish it to MinIO.

Target = GROUND TRUTH (was this transaction injected as fraud by the producer),
read from raw_transactions.payload->>'_injected_fraud' — NOT the rule output.
This avoids label leakage (the rules use the same features) and lets us run a
genuine ML-vs-rules comparison against the same ground truth.

Run:  docker compose --profile ml run --rm ml-train
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
import xgboost as xgb
from sklearn.metrics import (average_precision_score, confusion_matrix,
                             precision_score, recall_score, f1_score)
from sklearn.model_selection import train_test_split

from ml.store import FEATURES, save_model

THRESHOLD = float(os.getenv("ML_THRESHOLD", "0.8"))

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    dbname=os.getenv("POSTGRES_DB", "vajra"),
    user=os.getenv("POSTGRES_USER", "vajra"),
    password=os.getenv("POSTGRES_PASSWORD", "vajra"),
)

# Join processed features to the ground-truth label in raw. Dedupe raw by
# transaction_id (bool_or) so the join never fans out.
QUERY = """
SELECT p.amount,
       p.amount_zscore,
       p.payment_velocity,
       p.txns_last_hour,
       p.merchant_frequency,
       p.avg_amount_30d,
       p.seconds_since_prev,
       p.km_from_prev,
       p.device_changed::int   AS device_changed,
       p.location_changed::int AS location_changed,
       p.is_night::int         AS is_night,
       p.is_weekend::int       AS is_weekend,
       p.is_flagged::int        AS rule_pred,
       r.is_fraud::int          AS label
FROM processed_transactions p
JOIN (
    SELECT transaction_id,
           bool_or(payload->>'_injected_fraud' IS NOT NULL) AS is_fraud
    FROM raw_transactions
    GROUP BY transaction_id
) r ON r.transaction_id = p.transaction_id
"""


def _report(name: str, y_true, y_pred) -> dict:
    p = precision_score(y_true, y_pred, zero_division=0)
    r = recall_score(y_true, y_pred, zero_division=0)
    f = f1_score(y_true, y_pred, zero_division=0)
    print(f"  {name:<14} precision={p:.3f}  recall={r:.3f}  f1={f:.3f}")
    return {"precision": round(p, 4), "recall": round(r, 4), "f1": round(f, 4)}


def main() -> None:
    print("[train] loading data from Postgres...", flush=True)
    conn = psycopg2.connect(**PG)
    df = pd.read_sql(QUERY, conn)
    conn.close()

    n = len(df)
    pos = int(df["label"].sum())
    print(f"[train] rows={n}  fraud(label)={pos} ({100*pos/max(n,1):.1f}%)", flush=True)
    if n < 500 or pos < 50:
        raise SystemExit("[train] not enough data/positives yet — let the pipeline run longer.")

    X = df[FEATURES]
    y = df["label"]
    rule_pred = df["rule_pred"]

    X_tr, X_te, y_tr, y_te, _, rule_te = train_test_split(
        X, y, rule_pred, test_size=0.25, random_state=42, stratify=y
    )

    neg, posn = int((y_tr == 0).sum()), int((y_tr == 1).sum())
    spw = neg / max(posn, 1)

    clf = xgb.XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.1,
        subsample=0.9, colsample_bytree=0.9,
        scale_pos_weight=spw, eval_metric="aucpr",
        tree_method="hist", missing=np.nan, n_jobs=4, random_state=42,
    )
    print("[train] fitting XGBoost...", flush=True)
    clf.fit(X_tr, y_tr)

    proba = clf.predict_proba(X_te)[:, 1]
    ml_pred = (proba >= THRESHOLD).astype(int)
    pr_auc = average_precision_score(y_te, proba)

    print(f"\n[train] === Evaluation on held-out test set (n={len(y_te)}) ===")
    print(f"  PR-AUC (ML): {pr_auc:.3f}   threshold={THRESHOLD}")
    ml_metrics = _report("ML (XGBoost)", y_te, ml_pred)
    rule_metrics = _report("Rules engine", y_te, rule_te)

    tn, fp, fn, tp = confusion_matrix(y_te, ml_pred).ravel()
    print(f"  ML confusion: TP={tp} FP={fp} FN={fn} TN={tn}")

    print("\n[train] feature importances (gain):")
    importances = sorted(zip(FEATURES, clf.feature_importances_),
                         key=lambda x: x[1], reverse=True)
    for name, imp in importances:
        print(f"    {name:<20} {imp:.4f}")

    metadata = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "rows": n, "positives": pos,
        "features": FEATURES,
        "threshold": THRESHOLD,
        "pr_auc": round(float(pr_auc), 4),
        "ml_metrics": ml_metrics,
        "rule_metrics": rule_metrics,
        "feature_importances": {k: round(float(v), 4) for k, v in importances},
    }
    save_model(clf.get_booster(), metadata)
    print(f"\n[train] model + metadata uploaded to MinIO (bucket=vajra-models).", flush=True)
    print("[train] restart the consumer to load it: docker compose restart consumer", flush=True)


if __name__ == "__main__":
    main()
