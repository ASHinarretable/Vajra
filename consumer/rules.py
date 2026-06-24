"""Deterministic fraud rules engine.

Each rule consumes a transaction + its engineered features and may emit a
finding (rule name, severity, human-readable reason). These are intentionally
explainable rules — not ML — which is exactly what makes the platform a good
substrate for ML later: the features are already computed and labelled.
"""
from __future__ import annotations

# Thresholds — centralised so they can later move to config / a rules table.
VELOCITY_MIN_TXNS = 4          # txns within 60s
IMPOSSIBLE_SPEED_KMH = 800     # faster than a commercial flight => impossible
HIGH_VALUE_ZSCORE = 5.0        # std devs above the user's own mean
HIGH_VALUE_ABS_MULT = 20       # or N times their 30d average
FAILED_BURST_MIN = 3           # consecutive failures before a success


def evaluate(evt: dict, feats: dict, recent_statuses: list[str]) -> list[dict]:
    findings: list[dict] = []

    # 1) Impossible travel ----------------------------------------------------
    km = feats.get("km_from_prev")
    secs = feats.get("seconds_since_prev")
    if km and secs and secs > 0:
        speed_kmh = km / (secs / 3600.0)
        if km > 200 and speed_kmh > IMPOSSIBLE_SPEED_KMH:
            findings.append({
                "rule": "IMPOSSIBLE_TRAVEL",
                "severity": "CRITICAL",
                "reason": f"{km:.0f} km in {secs}s (~{speed_kmh:.0f} km/h) between consecutive txns",
            })

    # 2) Velocity -------------------------------------------------------------
    if feats.get("payment_velocity", 0) >= VELOCITY_MIN_TXNS:
        findings.append({
            "rule": "VELOCITY",
            "severity": "HIGH",
            "reason": f"{feats['payment_velocity']} transactions within 60 seconds",
        })

    # 3) High value anomaly ---------------------------------------------------
    amount = float(evt["amount"])
    avg = feats.get("avg_amount_30d") or 0
    z = feats.get("amount_zscore", 0)
    if (z >= HIGH_VALUE_ZSCORE) or (avg > 0 and amount >= avg * HIGH_VALUE_ABS_MULT):
        findings.append({
            "rule": "HIGH_VALUE",
            "severity": "HIGH",
            "reason": f"₹{amount:,.0f} vs 30d avg ₹{avg:,.0f} (z={z:.1f})",
        })

    # 4) New device + new location -------------------------------------------
    if feats.get("device_changed") and feats.get("location_changed"):
        findings.append({
            "rule": "NEW_DEVICE",
            "severity": "MEDIUM",
            "reason": f"new device '{evt.get('device')}' in new city '{evt.get('city')}'",
        })

    # 5) Repeated failures then success --------------------------------------
    # Look at the tail of recent statuses for >=3 FAILED immediately before a SUCCESS.
    if evt.get("status") == "SUCCESS" and len(recent_statuses) >= FAILED_BURST_MIN + 1:
        prior = recent_statuses[-(FAILED_BURST_MIN + 1):-1]
        if all(s == "FAILED" for s in prior):
            findings.append({
                "rule": "REPEATED_FAILURES",
                "severity": "MEDIUM",
                "reason": f"{len(prior)} consecutive failures before a success",
            })

    return findings
