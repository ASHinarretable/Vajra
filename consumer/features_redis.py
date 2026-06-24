"""Redis-backed feature engine.

Identical interface to the in-memory FeatureEngine in features.py, but stores
all per-user rolling state in Redis. This makes the feature layer horizontally
scalable: you can run N consumer instances and they all share the same state,
so a user's features stay accurate regardless of which instance sees a given
transaction.

When REDIS_URL is set, consumer/main.py uses this instead of FeatureEngine.
"""
from __future__ import annotations

import math
from datetime import datetime

import redis as redis_lib

from features import haversine_km


class RedisFeatureEngine:
    def __init__(self, redis_url: str):
        self._r = redis_lib.from_url(redis_url, decode_responses=True)

    # -- key helpers --------------------------------------------------------
    def _k(self, user_id: str, suffix: str) -> str:
        return f"vajra:u:{user_id}:{suffix}"

    # -- public interface (same as FeatureEngine) ---------------------------
    def compute(self, evt: dict, event_time: datetime) -> dict:
        uid = evt["user_id"]
        ts = event_time.timestamp()
        amount = float(evt["amount"])
        lat, lon = evt.get("latitude"), evt.get("longitude")
        merchant = evt.get("merchant") or ""
        device = evt.get("device") or ""
        city = evt.get("city") or ""

        k_amounts  = self._k(uid, "amounts")
        k_times    = self._k(uid, "times")
        k_merchants= self._k(uid, "merchants")
        k_meta     = self._k(uid, "meta")
        k_statuses = self._k(uid, "statuses")

        # ---- atomic read ---------------------------------------------------
        pipe = self._r.pipeline()
        pipe.lrange(k_amounts, 0, -1)                   # rolling amounts
        pipe.zcount(k_times, ts - 3600, ts)             # txns last hour
        pipe.zcount(k_times, ts - 60, ts)               # velocity (60s)
        pipe.hgetall(k_meta)                             # last-event metadata
        pipe.hget(k_merchants, merchant)                 # merchant visit count
        amounts_raw, txns_last_hour, velocity, meta, merch_raw = pipe.execute()

        # ---- derive features -----------------------------------------------
        amounts = [float(a) for a in amounts_raw]
        avg_30d = sum(amounts) / len(amounts) if amounts else amount

        if len(amounts) >= 2:
            mean = avg_30d
            std  = math.sqrt(sum((x - mean) ** 2 for x in amounts) / len(amounts))
            zscore = (amount - mean) / std if std > 0 else 0.0
        else:
            zscore = 0.0

        merch_freq    = int(merch_raw or 0) + 1
        last_device   = meta.get("device")
        last_city     = meta.get("city")
        last_lat      = float(meta["lat"])  if "lat"  in meta else None
        last_lon      = float(meta["lon"])  if "lon"  in meta else None
        last_ts       = float(meta["ts"])   if "ts"   in meta else None

        device_changed   = last_device is not None and device != last_device
        location_changed = last_city   is not None and city   != last_city
        seconds_since_prev = int(ts - last_ts) if last_ts else None
        km_from_prev = (
            round(haversine_km(last_lat, last_lon, lat, lon), 2)
            if last_lat is not None and lat is not None else None
        )

        features = {
            "txns_last_hour":     int(txns_last_hour),
            "avg_amount_30d":     round(avg_30d, 2),
            "amount_zscore":      round(zscore, 4),
            "merchant_frequency": merch_freq,
            "device_changed":     device_changed,
            "location_changed":   location_changed,
            "payment_velocity":   int(velocity),
            "is_weekend":         event_time.weekday() >= 5,
            "is_night":           event_time.hour >= 23 or event_time.hour < 5,
            "seconds_since_prev": seconds_since_prev,
            "km_from_prev":       km_from_prev,
        }

        # ---- atomic write --------------------------------------------------
        txn_id = evt.get("transaction_id", str(ts))
        up = self._r.pipeline()
        up.lpush(k_amounts, amount);         up.ltrim(k_amounts, 0, 199)
        up.zadd(k_times, {txn_id: ts});     up.zremrangebyscore(k_times, 0, ts - 7200)
        up.hincrby(k_merchants, merchant, 1)
        up.hset(k_meta, mapping={"ts": ts, "city": city,
                                  "lat": lat or "", "lon": lon or "",
                                  "device": device})
        up.lpush(k_statuses, evt.get("status", ""))
        up.ltrim(k_statuses, 0, 9)
        up.execute()

        return features

    def recent_statuses(self, user_id: str) -> list[str]:
        return self._r.lrange(self._k(user_id, "statuses"), 0, -1)
