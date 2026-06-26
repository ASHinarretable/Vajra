"""Shared ML utilities: the feature contract + model storage in MinIO.

This module is the single source of truth for:
  * FEATURES   — the exact feature order, used by BOTH training and runtime
                 scoring so the vectors always line up.
  * MinIO load/save of the model artifact + metadata.

Keeping the feature list here (not duplicated in train.py and the consumer)
prevents the classic "training/serving skew" bug where the columns drift apart.
"""
from __future__ import annotations

import json
import os

import boto3

# Order matters — runtime builds its vector in exactly this order.
FEATURES = [
    "amount",
    "amount_zscore",
    "payment_velocity",
    "txns_last_hour",
    "merchant_frequency",
    "avg_amount_30d",
    "seconds_since_prev",
    "km_from_prev",
    "device_changed",
    "location_changed",
    "is_night",
    "is_weekend",
]

BUCKET = os.getenv("MODEL_BUCKET", "vajra-models")
MODEL_KEY = "fraud/model.json"
META_KEY = "fraud/metadata.json"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "vajra")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "vajra2026")


def s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )


def _ensure_bucket(client) -> None:
    try:
        client.head_bucket(Bucket=BUCKET)
    except Exception:
        client.create_bucket(Bucket=BUCKET)


def save_model(booster, metadata: dict) -> None:
    """Persist an xgboost Booster (as JSON) + metadata to MinIO."""
    client = s3()
    _ensure_bucket(client)
    raw = bytes(booster.save_raw(raw_format="json"))
    client.put_object(Bucket=BUCKET, Key=MODEL_KEY, Body=raw)
    client.put_object(Bucket=BUCKET, Key=META_KEY,
                      Body=json.dumps(metadata, indent=2, default=str).encode())


def load_model():
    """Return (Booster, metadata) or (None, None) if no model has been trained."""
    import xgboost as xgb
    client = s3()
    try:
        raw = client.get_object(Bucket=BUCKET, Key=MODEL_KEY)["Body"].read()
        meta_raw = client.get_object(Bucket=BUCKET, Key=META_KEY)["Body"].read()
    except Exception:
        return None, None
    booster = xgb.Booster()
    booster.load_model(bytearray(raw))
    return booster, json.loads(meta_raw)


def runtime_vector(evt: dict, feats: dict) -> list[float]:
    """Build a feature vector (FEATURES order) from a live event + features.

    Missing numeric values become NaN so XGBoost handles them natively, exactly
    as it did at training time.
    """
    nan = float("nan")

    def num(v):
        return float(v) if v is not None else nan

    def flag(v):
        return 1.0 if v else 0.0

    return [
        num(evt.get("amount")),
        num(feats.get("amount_zscore")),
        num(feats.get("payment_velocity")),
        num(feats.get("txns_last_hour")),
        num(feats.get("merchant_frequency")),
        num(feats.get("avg_amount_30d")),
        num(feats.get("seconds_since_prev")),
        num(feats.get("km_from_prev")),
        flag(feats.get("device_changed")),
        flag(feats.get("location_changed")),
        flag(feats.get("is_night")),
        flag(feats.get("is_weekend")),
    ]
