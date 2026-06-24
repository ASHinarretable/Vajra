"""Realistic UPI transaction generator with deterministic fraud injection.

Each simulated user has a stable profile (home city, usual device, usual IP,
typical spend). Normal transactions vary around that profile. With probability
FRAUD_RATE, a transaction (or a short burst) is steered into one of five
realistic fraud scenarios, tagged so we can later measure detection accuracy.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone, timedelta

from faker import Faker

from domain import CITIES, BANKS, MERCHANTS, DEVICE_MODELS, STATUSES

fake = Faker("en_IN")


class UserProfile:
    """A stable identity that behaves consistently until fraud perturbs it."""

    def __init__(self, user_id: str):
        self.user_id = user_id
        self.name = fake.name()
        self.home_city = random.choice(list(CITIES))
        self.bank = random.choice(list(BANKS))
        handle_suffix = random.choice(BANKS[self.bank])
        self.upi_handle = f"{self.name.split()[0].lower()}{random.randint(1, 999)}@{handle_suffix}"
        self.usual_os = random.choices(["Android", "iOS"], weights=[8, 2])[0]
        self.usual_device = random.choice(DEVICE_MODELS[self.usual_os])
        self.ip_prefix = f"{random.randint(10, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
        self.avg_amount = random.choice([400, 600, 800, 1200, 2000])

    def usual_ip(self) -> str:
        return f"{self.ip_prefix}.{random.randint(1, 254)}"


# Fraud scenario labels (also used downstream to evaluate detection).
SCENARIOS = [
    "IMPOSSIBLE_TRAVEL",
    "VELOCITY",
    "HIGH_VALUE",
    "NEW_DEVICE",
    "REPEATED_FAILURES",
]


def _base_txn(user: UserProfile, ts: datetime) -> dict:
    """A normal-looking transaction consistent with the user's profile."""
    category = random.choice(list(MERCHANTS))
    merchants, (lo, hi) = MERCHANTS[category]
    lat, lon = CITIES[user.home_city]
    amount = round(random.uniform(max(lo, user.avg_amount * 0.3),
                                  min(hi, user.avg_amount * 1.8)), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user.user_id,
        "user_name": user.name,
        "merchant": random.choice(merchants),
        "merchant_category": category,
        "city": user.home_city,
        "device": user.usual_device,
        "device_os": user.usual_os,
        "amount": amount,
        "currency": "INR",
        "payment_method": "UPI",
        "transaction_type": "P2P" if category == "P2P Transfer" else "P2M",
        "status": random.choice(STATUSES),
        "upi_handle": user.upi_handle,
        "payee_vpa": f"{fake.first_name().lower()}@{random.choice(BANKS[random.choice(list(BANKS))])}",
        "bank": user.bank,
        "ip_address": user.usual_ip(),
        "latitude": round(lat + random.uniform(-0.03, 0.03), 6),
        "longitude": round(lon + random.uniform(-0.03, 0.03), 6),
        "event_time": ts.astimezone(timezone.utc).isoformat(),
        # Ground-truth label for evaluation only — the consumer must not read this.
        "_injected_fraud": None,
    }


def normal(user: UserProfile, ts: datetime) -> list[dict]:
    return [_base_txn(user, ts)]


def fraud(user: UserProfile, ts: datetime, scenario: str) -> list[dict]:
    """Return one or more transactions realizing the given fraud scenario."""
    if scenario == "HIGH_VALUE":
        t = _base_txn(user, ts)
        t["amount"] = round(user.avg_amount * random.uniform(40, 120), 2)
        t["merchant_category"] = "Shopping"
        t["status"] = "SUCCESS"
        t["_injected_fraud"] = scenario
        return [t]

    if scenario == "NEW_DEVICE":
        t = _base_txn(user, ts)
        new_os = "iOS" if user.usual_os == "Android" else "Android"
        t["device_os"] = new_os
        t["device"] = random.choice(DEVICE_MODELS[new_os])
        t["ip_address"] = f"{random.randint(10, 223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        away = random.choice([c for c in CITIES if c != user.home_city])
        t["city"] = away
        t["latitude"], t["longitude"] = CITIES[away]
        t["_injected_fraud"] = scenario
        return [t]

    if scenario == "IMPOSSIBLE_TRAVEL":
        # Two txns minutes apart in far-apart cities — physically impossible.
        # Backdated so the latest event is "now" (never future-stamped).
        far = random.choice([c for c in CITIES if c != user.home_city])
        t1 = _base_txn(user, ts - timedelta(minutes=random.randint(2, 8)))
        t2 = _base_txn(user, ts)
        t2["city"] = far
        t2["latitude"], t2["longitude"] = CITIES[far]
        t2["_injected_fraud"] = scenario
        return [t1, t2]

    if scenario == "VELOCITY":
        # A burst of rapid-fire payments within ~20 seconds, ending at "now".
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
        # Several FAILED attempts then a SUCCESS — classic credential testing.
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

    return normal(user, ts)
