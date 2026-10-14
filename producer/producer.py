"""
Streams synthetic e-commerce order events into Kafka.

Each event is a JSON message keyed by customer_id (so one customer's orders always
land on the same partition, preserving their order). To make the Flink side realistic:
  * ~5% of events arrive LATE (event_time 5-40s in the past) to exercise watermarks.
  * ~3% are high-value orders that should trigger alerts.
  * ~1% are malformed, which Flink skips (json.ignore-parse-errors).

Usage (inside Docker this runs automatically):
  python producer.py                    # stream forever to Kafka
  python producer.py --stdout -n 20     # print 20 events instead (no Kafka needed)
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

CATEGORIES = {
    "Electronics": (25, 400),
    "Home": (8, 150),
    "Apparel": (9, 120),
    "Books": (5, 45),
    "Sports": (15, 250),
    "Beauty": (6, 80),
}
CHANNELS = ["web", "mobile_app", "marketplace"]
HIGH_VALUE_THRESHOLD = 500


def make_event(rng, now, seq):
    """Build one order event. Pure function, so tests can reuse it."""
    category = rng.choice(list(CATEGORIES))
    low, high = CATEGORIES[category]
    quantity = rng.randint(1, 4)
    unit_price = round(rng.uniform(low, high), 2)

    if rng.random() < 0.03:                      # occasional big-ticket order
        quantity, unit_price = rng.randint(3, 6), round(rng.uniform(250, 900), 2)

    event_time = now
    if rng.random() < 0.05:                      # late / out-of-order arrival
        event_time = now - timedelta(seconds=rng.randint(5, 40))

    return {
        "order_id": f"ORD-{int(now.timestamp())}-{seq:06d}",
        "customer_id": rng.randint(1, 500),
        "product_id": rng.randint(1, 60),
        "category": category,
        "channel": rng.choice(CHANNELS),
        "quantity": quantity,
        "unit_price": unit_price,
        "amount": round(quantity * unit_price, 2),
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
    }


def kafka_producer():
    from confluent_kafka import Producer

    conf = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "lz4",
    }
    producer = Producer(conf)
    for attempt in range(30):                     # wait for the broker on startup
        try:
            producer.list_topics(timeout=5)
            return producer
        except Exception as exc:  # noqa: BLE001
            print(f"waiting for Kafka ({attempt + 1}/30): {exc}", flush=True)
            time.sleep(2)
    sys.exit("Kafka not reachable")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=float(os.getenv("EVENTS_PER_SECOND", "10")))
    parser.add_argument("--topic", default=os.getenv("TOPIC", "orders"))
    parser.add_argument("-n", "--max-events", type=int, default=0, help="0 = run forever")
    parser.add_argument("--stdout", action="store_true", help="print events instead of sending")
    args = parser.parse_args()

    rng = random.Random()
    producer = None if args.stdout else kafka_producer()
    sent = 0
    print(f"producing ~{args.rate} events/s to '{args.topic}'", flush=True)

    while args.max_events == 0 or sent < args.max_events:
        event = make_event(rng, datetime.now(timezone.utc).replace(tzinfo=None), sent)
        payload = json.dumps(event)
        if rng.random() < 0.01:
            payload = payload[: len(payload) // 2]   # deliberately broken JSON

        if producer is None:
            print(payload)
        else:
            producer.produce(args.topic, key=str(event["customer_id"]), value=payload)
            producer.poll(0)
        sent += 1
        if sent % 500 == 0:
            print(f"sent {sent} events", flush=True)
        time.sleep(1 / args.rate)

    if producer is not None:
        producer.flush(10)


if __name__ == "__main__":
    main()
