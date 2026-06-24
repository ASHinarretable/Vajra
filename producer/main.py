"""Producer entrypoint — streams UPI transactions into Kafka (transactions.raw).

Rate, fraud rate and user population are controlled via environment variables
(see .env.example). Keyed by user_id so all of a user's events land on the same
partition — important for ordered, stateful fraud detection downstream.
"""
from __future__ import annotations

import json
import os
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from generator import UserProfile, SCENARIOS, normal, fraud

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "transactions.raw")
RATE = float(os.getenv("PRODUCER_RATE", "5"))          # transactions/sec
FRAUD_RATE = float(os.getenv("FRAUD_RATE", "0.08"))
NUM_USERS = int(os.getenv("NUM_USERS", "200"))

_running = True


def _stop(*_):
    global _running
    _running = False


def _delivery(err, msg):
    if err is not None:
        print(f"[producer] delivery failed: {err}", flush=True)


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "vajra-producer",
        "linger.ms": 50,
        "compression.type": "snappy",
    })

    users = [UserProfile(f"U{1000 + i}") for i in range(NUM_USERS)]
    print(f"[producer] {len(users)} users, {RATE} txn/s, fraud_rate={FRAUD_RATE}, "
          f"-> {BOOTSTRAP}/{TOPIC_RAW}", flush=True)

    interval = 1.0 / RATE if RATE > 0 else 0.2
    sent = 0
    while _running:
        user = random.choice(users)
        now = datetime.now(timezone.utc)

        if random.random() < FRAUD_RATE:
            events = fraud(user, now, random.choice(SCENARIOS))
        else:
            events = normal(user, now)

        for evt in events:
            producer.produce(
                TOPIC_RAW,
                key=user.user_id.encode(),
                value=json.dumps(evt).encode(),
                callback=_delivery,
            )
            sent += 1

        producer.poll(0)
        if sent % 100 == 0:
            print(f"[producer] sent={sent}", flush=True)
        time.sleep(interval)

    print("[producer] flushing...", flush=True)
    producer.flush(10)
    print(f"[producer] stopped. total sent={sent}", flush=True)


if __name__ == "__main__":
    main()
