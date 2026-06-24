"""Unit tests for the data-quality gate, feature engine, and rules engine.

Run from the repo root:  pytest -q
"""
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "consumer"))

from validation import validate          # noqa: E402
from features import FeatureEngine, haversine_km  # noqa: E402
from rules import evaluate               # noqa: E402


def _evt(**over):
    base = {
        "transaction_id": "t1", "user_id": "U1", "merchant": "Uber",
        "merchant_category": "Transport", "city": "Pune", "device": "OnePlus 11",
        "amount": 500, "status": "SUCCESS", "upi_handle": "amit1@okhdfc",
        "latitude": 18.52, "longitude": 73.85,
        "event_time": datetime.now(timezone.utc).isoformat(),
    }
    base.update(over)
    return base


# ----------------------------- validation ----------------------------------

def test_clean_record_passes():
    assert validate(_evt()) == []


def test_negative_amount_rejected():
    assert "NON_POSITIVE_AMOUNT" in validate(_evt(amount=-10))


def test_unknown_city_rejected():
    assert "INVALID_CITY" in validate(_evt(city="Atlantis"))


def test_future_timestamp_rejected():
    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    assert "FUTURE_TIMESTAMP" in validate(_evt(event_time=future))


def test_bad_upi_handle_rejected():
    assert "INVALID_UPI_HANDLE" in validate(_evt(upi_handle="notahandle"))


# ----------------------------- features ------------------------------------

def test_haversine_pune_delhi_is_far():
    # Pune -> Delhi is well over 1000 km.
    assert haversine_km(18.52, 73.85, 28.70, 77.10) > 1000


# ------------------------------- rules -------------------------------------

def test_velocity_flagged():
    eng = FeatureEngine()
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    findings = []
    for i in range(5):
        e = _evt(event_time=(base + timedelta(seconds=i * 3)).isoformat())
        f = eng.compute(e, base + timedelta(seconds=i * 3))
        findings = evaluate(e, f, eng.recent_statuses("U1"))
    assert any(x["rule"] == "VELOCITY" for x in findings)


def test_impossible_travel_flagged():
    eng = FeatureEngine()
    t0 = datetime(2026, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    e1 = _evt(city="Pune", latitude=18.52, longitude=73.85,
              event_time=t0.isoformat())
    eng.compute(e1, t0)
    evaluate(e1, eng.compute(e1, t0), eng.recent_statuses("U1"))

    t1 = t0 + timedelta(minutes=5)
    e2 = _evt(transaction_id="t2", city="Delhi", latitude=28.70, longitude=77.10,
              event_time=t1.isoformat())
    f2 = eng.compute(e2, t1)
    findings = evaluate(e2, f2, eng.recent_statuses("U1"))
    assert any(x["rule"] == "IMPOSSIBLE_TRAVEL" for x in findings)


def test_high_value_flagged():
    eng = FeatureEngine()
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    for i in range(20):
        e = _evt(amount=500, event_time=(base + timedelta(minutes=i)).isoformat())
        f = eng.compute(e, base + timedelta(minutes=i))
        evaluate(e, f, eng.recent_statuses("U1"))
    big = _evt(amount=98000, event_time=(base + timedelta(hours=1)).isoformat())
    f = eng.compute(big, base + timedelta(hours=1))
    findings = evaluate(big, f, eng.recent_statuses("U1"))
    assert any(x["rule"] == "HIGH_VALUE" for x in findings)
