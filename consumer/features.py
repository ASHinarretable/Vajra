"""Feature engineering — stateful, per-user rolling features.

State is held in-memory keyed by user_id. Because the producer keys events by
user_id, a single consumer instance sees all of a user's events in order, so
this is correct for one consumer. (At scale this state moves to Redis — noted
in the roadmap — but the interface here stays the same.)
"""
from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance between two points in kilometres."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


@dataclass
class UserState:
    amounts: deque = field(default_factory=lambda: deque(maxlen=200))
    recent_times: deque = field(default_factory=lambda: deque(maxlen=200))   # datetimes
    merchant_counts: dict = field(default_factory=lambda: defaultdict(int))
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
        """Return engineered features and mutate per-user rolling state."""
        s = self._state[evt["user_id"]]
        amount = float(evt["amount"])
        lat, lon = evt.get("latitude"), evt.get("longitude")

        # --- rolling counts over time windows ---
        txns_last_hour = sum(1 for t in s.recent_times if (event_time - t).total_seconds() <= 3600)
        velocity = sum(1 for t in s.recent_times if (event_time - t).total_seconds() <= 60)

        # --- amount stats (computed on history *before* this txn) ---
        hist = list(s.amounts)
        avg_30d = sum(hist) / len(hist) if hist else amount
        if len(hist) >= 2:
            mean = sum(hist) / len(hist)
            var = sum((x - mean) ** 2 for x in hist) / len(hist)
            std = math.sqrt(var)
            zscore = (amount - mean) / std if std > 0 else 0.0
        else:
            zscore = 0.0

        # --- merchant frequency ---
        merchant_freq = s.merchant_counts[evt.get("merchant", "")] + 1

        # --- change detection vs previous transaction ---
        device_changed = s.last_device is not None and evt.get("device") != s.last_device
        location_changed = s.last_city is not None and evt.get("city") != s.last_city

        seconds_since_prev = None
        if s.last_event_time is not None:
            seconds_since_prev = int((event_time - s.last_event_time).total_seconds())

        km_from_prev = None
        if s.last_lat is not None and lat is not None:
            km_from_prev = round(haversine_km(s.last_lat, s.last_lon, lat, lon), 2)

        features = {
            "txns_last_hour": txns_last_hour,
            "avg_amount_30d": round(avg_30d, 2),
            "amount_zscore": round(zscore, 4),
            "merchant_frequency": merchant_freq,
            "device_changed": device_changed,
            "location_changed": location_changed,
            "payment_velocity": velocity,
            "is_weekend": event_time.weekday() >= 5,
            "is_night": event_time.hour >= 23 or event_time.hour < 5,
            "seconds_since_prev": seconds_since_prev,
            "km_from_prev": km_from_prev,
        }

        # --- update state AFTER computing features ---
        s.amounts.append(amount)
        s.recent_times.append(event_time)
        s.merchant_counts[evt.get("merchant", "")] += 1
        s.last_event_time = event_time
        s.last_city = evt.get("city")
        s.last_lat, s.last_lon = lat, lon
        s.last_device = evt.get("device")
        s.recent_statuses.append(evt.get("status"))

        return features

    def recent_statuses(self, user_id: str) -> list[str]:
        return list(self._state[user_id].recent_statuses)
