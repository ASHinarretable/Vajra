"""Runtime ML scorer used by the consumer.

Loads the trained model from MinIO once at startup and scores each transaction
inline (~sub-millisecond via xgboost inplace_predict). Designed to FAIL OPEN:
if no model has been trained yet, scoring is a no-op and the rule engine keeps
working — the pipeline never breaks waiting on ML.
"""
from __future__ import annotations

import os

import numpy as np

from ml.store import load_model, runtime_vector


class Scorer:
    def __init__(self):
        self.threshold = float(os.getenv("ML_THRESHOLD", "0.8"))
        self.booster = None
        self.meta = None
        try:
            self.booster, self.meta = load_model()
        except Exception as e:  # MinIO down, etc. — degrade gracefully
            print(f"[scorer] could not load model: {e}", flush=True)

        if self.booster is not None:
            print(f"[scorer] model loaded (trained_at={self.meta.get('trained_at')}, "
                  f"PR-AUC={self.meta.get('pr_auc')}, threshold={self.threshold})", flush=True)
        else:
            print("[scorer] no model available — ML scoring disabled (rules only)", flush=True)

    @property
    def enabled(self) -> bool:
        return self.booster is not None

    def score(self, evt: dict, feats: dict) -> float | None:
        """Return fraud probability in [0,1], or None if no model is loaded."""
        if self.booster is None:
            return None
        vec = np.array([runtime_vector(evt, feats)], dtype="float32")
        return float(self.booster.inplace_predict(vec)[0])

    def severity(self, score: float) -> str:
        if score >= 0.95:
            return "HIGH"
        if score >= 0.88:
            return "MEDIUM"
        return "LOW"
