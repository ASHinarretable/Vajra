"""Consumer entrypoint — the enterprise processing path.

For every event on transactions.raw:
    1. land it verbatim in raw_transactions          (RAW layer, never edited)
    2. run data-quality gates                         -> reject -> quarantine
    3. engineer rolling features
    4. evaluate fraud rules
    5. persist processed row + emit transactions.cleaned
    6. on findings: persist fraud_events + emit transactions.suspicious

Feature state backend:
    REDIS_URL set -> RedisFeatureEngine (shared, horizontally scalable)
    REDIS_URL unset -> in-memory FeatureEngine (single instance, no dependency)

Metrics: Prometheus exposition on port 8000 (/metrics).
"""
from __future__ import annotations

import json
import os
import signal
import time

from confluent_kafka import Consumer, Producer, KafkaError
from prometheus_client import Counter, Histogram, start_http_server

import db
from validation import validate, parse_time
from features import FeatureEngine
from rules import evaluate

BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW    = os.getenv("TOPIC_RAW",        "transactions.raw")
TOPIC_CLEANED    = os.getenv("TOPIC_CLEANED",    "transactions.cleaned")
TOPIC_SUSPICIOUS = os.getenv("TOPIC_SUSPICIOUS", "transactions.suspicious")
GROUP        = os.getenv("CONSUMER_GROUP",    "vajra-fraud-consumer")
REDIS_URL    = os.getenv("REDIS_URL")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# ---- Prometheus metrics ---------------------------------------------------
TXN_PROCESSED = Counter("vajra_txns_processed_total",
                         "Transactions successfully processed")
TXN_REJECTED  = Counter("vajra_txns_rejected_total",
                         "Transactions rejected by data-quality gate")
FRAUD_ALERTS  = Counter("vajra_fraud_alerts_total",
                         "Fraud rule firings", ["rule"])
PROCESS_TIME  = Histogram("vajra_processing_seconds",
                           "Per-transaction processing time",
                           buckets=[.005, .01, .025, .05, .1, .25, .5, 1])

_EVENT_FIELDS = [
    "transaction_id", "user_id", "user_name", "merchant", "merchant_category",
    "city", "device", "device_os", "amount", "currency", "payment_method",
    "transaction_type", "status", "upi_handle", "payee_vpa", "bank",
    "ip_address", "latitude", "longitude",
]

_running = True


def _stop(*_):
    global _running
    _running = False


def _build_processed_row(evt: dict, event_time, feats: dict, flagged: bool) -> dict:
    row = {k: evt.get(k) for k in _EVENT_FIELDS}
    row["event_time"] = event_time
    row.update(feats)
    row["is_flagged"] = flagged
    return row


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    # ---- metrics server ---------------------------------------------------
    start_http_server(METRICS_PORT)
    print(f"[consumer] Prometheus metrics on :{METRICS_PORT}/metrics", flush=True)

    # ---- feature engine ---------------------------------------------------
    if REDIS_URL:
        from features_redis import RedisFeatureEngine
        engine = RedisFeatureEngine(REDIS_URL)
        print(f"[consumer] using Redis feature engine @ {REDIS_URL}", flush=True)
    else:
        engine = FeatureEngine()
        print("[consumer] using in-memory feature engine (single instance)", flush=True)

    conn = db.connect()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_RAW])

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "vajra-consumer-out"})

    print(f"[consumer] subscribed to {TOPIC_RAW} @ {BOOTSTRAP}", flush=True)
    processed = rejected = flagged_total = 0

    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[consumer] kafka error: {msg.error()}", flush=True)
            continue

        try:
            evt = json.loads(msg.value())
        except (ValueError, TypeError):
            print("[consumer] skipping unparseable message", flush=True)
            continue

        t0   = time.perf_counter()
        meta = {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()}

        # 1) RAW layer — always, verbatim.
        db.insert_raw(conn, evt, meta)

        # 2) Data-quality gate.
        failures = validate(evt)
        if failures:
            db.insert_rejected(conn, evt, failures)
            TXN_REJECTED.inc()
            rejected += 1
            continue

        # 3) Feature engineering.
        event_time = parse_time(evt["event_time"])
        feats = engine.compute(evt, event_time)

        # 4) Fraud rules.
        findings = evaluate(evt, feats, engine.recent_statuses(evt["user_id"]))
        flagged  = bool(findings)

        # 5) Processed layer + cleaned topic.
        row = _build_processed_row(evt, event_time, feats, flagged)
        db.insert_processed(conn, row)
        producer.produce(TOPIC_CLEANED, key=evt["user_id"].encode(),
                         value=json.dumps(evt, default=str).encode())
        TXN_PROCESSED.inc()
        processed += 1

        # 6) Fraud layer + suspicious topic.
        if findings:
            fraud_rows = [
                (evt["transaction_id"], evt["user_id"], f["rule"], f["severity"],
                 f["reason"], float(evt["amount"]), evt.get("city"), event_time)
                for f in findings
            ]
            db.insert_fraud_events(conn, fraud_rows)
            flagged_total += 1
            alert = {**evt, "fraud_findings": findings}
            producer.produce(TOPIC_SUSPICIOUS, key=evt["user_id"].encode(),
                             value=json.dumps(alert, default=str).encode())
            for f in findings:
                FRAUD_ALERTS.labels(rule=f["rule"]).inc()
                print(f"[ALERT] {f['severity']:<8} {f['rule']:<18} user={evt['user_id']} "
                      f"txn={evt['transaction_id'][:8]} :: {f['reason']}", flush=True)

        PROCESS_TIME.observe(time.perf_counter() - t0)
        producer.poll(0)
        if (processed + rejected) % 100 == 0:
            print(f"[consumer] processed={processed} flagged={flagged_total} rejected={rejected}",
                  flush=True)

    print("[consumer] shutting down...", flush=True)
    producer.flush(10)
    consumer.close()
    conn.close()
    print(f"[consumer] done. processed={processed} flagged={flagged_total} rejected={rejected}",
          flush=True)


if __name__ == "__main__":
    main()
