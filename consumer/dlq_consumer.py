"""Dead-letter queue consumer — reads unparseable/failed messages for manual review.

Messages land here when:
  * JSON parsing fails
  * Validation errors that need investigation
  * Consumer crashes (future: Kafka exactly-once would catch these)

Run: docker compose run --rm dlq-consumer
"""
from __future__ import annotations

import json
import os
import signal

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "transactions.dead-letter")
GROUP = os.getenv("CONSUMER_GROUP", "vajra-dlq-consumer")

_running = True


def _stop(*_):
    global _running
    _running = False


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_DLQ])

    print(f"[dlq] subscribed to {TOPIC_DLQ} @ {BOOTSTRAP}", flush=True)
    processed = 0

    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[dlq] kafka error: {msg.error()}", flush=True)
            continue

        try:
            payload = json.loads(msg.value())
        except (ValueError, TypeError):
            payload = {"raw": msg.value().decode(errors="replace")}

        # Log the DLQ message with severity based on error type
        error = payload.get("error", "unknown")
        raw = payload.get("raw", "")[:200]
        print(f"[DLQ] ⚠️  offset={msg.offset()} error={error} raw={raw}", flush=True)
        processed += 1

        if processed % 50 == 0:
            print(f"[dlq] processed={processed}", flush=True)

    print("[dlq] shutting down...", flush=True)
    consumer.close()
    print(f"[dlq] done. processed={processed}", flush=True)


if __name__ == "__main__":
    main()
