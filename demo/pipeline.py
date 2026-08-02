"""Trimmed fraud pipeline: generation + validation + features + rules, all
in one process (no Kafka/Redis) — intentional for a single-instance free demo.

This is a smaller sibling of producer/ + consumer/ in the main repo. See
ARCHITECTURE docs (kept locally, not in the public repo) for the full,
horizontally-scalable version with Kafka partitioning and Redis-shared state.
"""
from __future__ import annotations

import math
import random
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from domain import BANKS, CITIES, DEVICE_MODELS, FIRST_NAMES, MERCHANTS

SCENARIOS = ["IMPOSSIBLE_TRAVEL", "VELOCITY", "HIGH_VALUE", "NEW_DEVICE", "REPEATED_FAILURES"]
STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"]


# --------------------------------------------------------------------------- #
# User profiles + generation
# --------------------------------------------------------------------------- #
class UserProfile:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.name = random.choice(FIRST_NAMES)
        self.home_city = random.choice(list(CITIES))
        self.bank = random.choice(list(BANKS))
        self.upi_handle = f"{self.name.lower()}{random.randint(1,999)}@{BANKS[self.bank]}"
        self.usual_os = random.choices(["Android", "iOS"], weights=[8, 2])[0]
        self.usual_device = random.choice(DEVICE_MODELS[self.usual_os])
        self.avg_amount = random.choice([400, 600, 800, 1200, 2000])


def _base_txn(user: UserProfile, ts: datetime) -> dict:
    category = random.choice(list(MERCHANTS))
    merchants, (lo, hi) = MERCHANTS[category]
    lat, lon = CITIES[user.home_city]
    amount = round(random.uniform(max(lo, user.avg_amount * 0.3), min(hi, user.avg_amount * 1.8)), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user.user_id,
        "merchant": random.choice(merchants),
        "merchant_category": category,
        "city": user.home_city,
        "device": user.usual_device,
        "amount": amount,
        "status": random.choice(STATUSES),
        "upi_handle": user.upi_handle,
        "bank": user.bank,
        "latitude": round(lat + random.uniform(-0.03, 0.03), 6),
        "longitude": round(lon + random.uniform(-0.03, 0.03), 6),
        "event_time": ts.astimezone(timezone.utc),
        "_injected_fraud": None,
    }


def generate(user: UserProfile, ts: datetime, fraud_rate: float) -> list[dict]:
    if random.random() >= fraud_rate:
        return [_base_txn(user, ts)]

    scenario = random.choice(SCENARIOS)

    if scenario == "HIGH_VALUE":
        t = _base_txn(user, ts)
        shop_merchants, _ = MERCHANTS["Shopping"]
        t["merchant"], t["merchant_category"] = random.choice(shop_merchants), "Shopping"
        t["amount"] = round(user.avg_amount * random.uniform(40, 120), 2)
        t["status"] = "SUCCESS"
        t["_injected_fraud"] = scenario
        return [t]

    if scenario == "NEW_DEVICE":
        t = _base_txn(user, ts)
        new_os = "iOS" if user.usual_os == "Android" else "Android"
        t["device"] = random.choice(DEVICE_MODELS[new_os])
        away = random.choice([c for c in CITIES if c != user.home_city])
        t["city"] = away
        t["latitude"], t["longitude"] = CITIES[away]
        t["_injected_fraud"] = scenario
        return [t]

    if scenario == "IMPOSSIBLE_TRAVEL":
        far = random.choice([c for c in CITIES if c != user.home_city])
        t1 = _base_txn(user, ts - timedelta(minutes=random.randint(2, 8)))
        t2 = _base_txn(user, ts)
        t2["city"] = far
        t2["latitude"], t2["longitude"] = CITIES[far]
        t2["_injected_fraud"] = scenario
        return [t1, t2]

    if scenario == "VELOCITY":
        n = random.randint(4, 6)
        burst = []
        for i in range(n):
            t = _base_txn(user, ts - timedelta(seconds=(n - 1 - i) * 3))
            t["amount"] = round(random.uniform(500, 1000), 2)
            t["status"] = "SUCCESS"
            t["_injected_fraud"] = scenario
            burst.append(t)
        return burst

    if scenario == "REPEATED_FAILURES":
        n_fail = random.randint(3, 5)
        total = n_fail + 1
        seq = []
        for i in range(n_fail):
            t = _base_txn(user, ts - timedelta(seconds=(total - 1 - i) * 2))
            t["status"] = "FAILED"
            t["_injected_fraud"] = scenario
            seq.append(t)
        success = _base_txn(user, ts)
        success["status"] = "SUCCESS"
        success["_injected_fraud"] = scenario
        seq.append(success)
        return seq

    return [_base_txn(user, ts)]


# --------------------------------------------------------------------------- #
# Validation (data-quality gate)
# --------------------------------------------------------------------------- #
def validate(evt: dict) -> list[str]:
    failed = []
    if not evt.get("transaction_id"):
        failed.append("MISSING_TRANSACTION_ID")
    if not evt.get("user_id"):
        failed.append("MISSING_USER_ID")
    amount = evt.get("amount")
    if amount is None or float(amount) <= 0:
        failed.append("INVALID_AMOUNT")
    if evt.get("city") not in CITIES:
        failed.append("INVALID_CITY")
    if evt.get("status") not in ("SUCCESS", "FAILED", "PENDING"):
        failed.append("INVALID_STATUS")
    return failed


# --------------------------------------------------------------------------- #
# Feature engineering (in-memory, single-process — fine for one demo instance)
# --------------------------------------------------------------------------- #
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dlmb = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


@dataclass
class UserState:
    amounts: deque = field(default_factory=lambda: deque(maxlen=100))
    recent_times: deque = field(default_factory=lambda: deque(maxlen=100))
    last_event_time: datetime | None = None
    last_city: str | None = None
    last_lat: float | None = None
    last_lon: float | None = None
    last_device: str | None = None
    recent_statuses: deque = field(default_factory=lambda: deque(maxlen=10))


class FeatureEngine:
    def __init__(self):
        self._state: dict[str, UserState] = defaultdict(UserState)

    def compute(self, evt: dict, event_time: datetime) -> dict:
        s = self._state[evt["user_id"]]
        amount = float(evt["amount"])
        lat, lon = evt.get("latitude"), evt.get("longitude")

        velocity = sum(1 for t in s.recent_times if (event_time - t).total_seconds() <= 60)
        hist = list(s.amounts)
        avg_30d = sum(hist) / len(hist) if hist else amount
        if len(hist) >= 2:
            mean = avg_30d
            std = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
            zscore = (amount - mean) / std if std > 0 else 0.0
        else:
            zscore = 0.0

        device_changed = s.last_device is not None and evt.get("device") != s.last_device
        location_changed = s.last_city is not None and evt.get("city") != s.last_city
        seconds_since_prev = int((event_time - s.last_event_time).total_seconds()) if s.last_event_time else None
        km_from_prev = (
            round(haversine_km(s.last_lat, s.last_lon, lat, lon), 2)
            if s.last_lat is not None and lat is not None else None
        )

        feats = {
            "amount_zscore": round(zscore, 4),
            "payment_velocity": velocity,
            "avg_amount_30d": round(avg_30d, 2),
            "device_changed": device_changed,
            "location_changed": location_changed,
            "seconds_since_prev": seconds_since_prev,
            "km_from_prev": km_from_prev,
        }

        s.amounts.append(amount)
        s.recent_times.append(event_time)
        s.last_event_time = event_time
        s.last_city = evt.get("city")
        s.last_lat, s.last_lon = lat, lon
        s.last_device = evt.get("device")
        s.recent_statuses.append(evt.get("status"))
        return feats

    def recent_statuses(self, user_id: str) -> list[str]:
        return list(self._state[user_id].recent_statuses)


# --------------------------------------------------------------------------- #
# Fraud rules (same 5 rules as the main platform, same thresholds)
# --------------------------------------------------------------------------- #
def evaluate(evt: dict, feats: dict, recent_statuses: list[str]) -> list[dict]:
    findings = []

    km, secs = feats.get("km_from_prev"), feats.get("seconds_since_prev")
    if km and secs and secs > 0:
        speed = km / (secs / 3600.0)
        if km > 200 and speed > 800:
            findings.append({"rule": "IMPOSSIBLE_TRAVEL", "severity": "CRITICAL",
                            "reason": f"{km:.0f} km in {secs}s (~{speed:.0f} km/h)"})

    if feats.get("payment_velocity", 0) >= 4:
        findings.append({"rule": "VELOCITY", "severity": "HIGH",
                        "reason": f"{feats['payment_velocity']} transactions within 60 seconds"})

    amount, avg, z = float(evt["amount"]), feats.get("avg_amount_30d") or 0, feats.get("amount_zscore", 0)
    if z >= 5.0 or (avg > 0 and amount >= avg * 20):
        findings.append({"rule": "HIGH_VALUE", "severity": "HIGH",
                        "reason": f"Rs.{amount:,.0f} vs 30d avg Rs.{avg:,.0f} (z={z:.1f})"})

    if feats.get("device_changed") and feats.get("location_changed"):
        findings.append({"rule": "NEW_DEVICE", "severity": "MEDIUM",
                        "reason": f"new device in new city '{evt.get('city')}'"})

    if evt.get("status") == "SUCCESS" and len(recent_statuses) >= 4:
        prior = recent_statuses[-4:-1]
        if all(s == "FAILED" for s in prior):
            findings.append({"rule": "REPEATED_FAILURES", "severity": "MEDIUM",
                            "reason": f"{len(prior)} consecutive failures before success"})

    return findings
