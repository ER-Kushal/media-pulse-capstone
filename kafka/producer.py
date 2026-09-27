"""
MediaPulse - Kafka Producer (simulated live events)
====================================================
Replays your cleaned Silver views/ads data as if it were happening live:
reads data_silver/views_silver.csv and ads_silver.csv, stamps each row
with the current time, and streams it to Kafka topics one row at a time.

This satisfies the "live streaming" requirement using real (cleaned)
data rather than fully synthetic events, and demonstrates the full
producer -> topic -> consumer -> warehouse -> API/frontend chain.

Usage:
    pip install kafka-python pandas
    python kafka/producer.py --rate 5      # 5 events/sec (default 2)
"""

import argparse
import json
import time
import random
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
VIEWS_TOPIC = "mediapulse.views"
ADS_TOPIC = "mediapulse.ads"


def make_producer():
    # retries + acks='all' => the producer won't silently drop a message
    # on a transient broker hiccup (part of the "retry handling" requirement).
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
    )


def stream_events(rate_per_sec: float):
    views = pd.read_csv("data_silver/views_silver.csv").to_dict("records")
    ads = pd.read_csv("data_silver/ads_silver.csv").to_dict("records")
    random.shuffle(views)
    random.shuffle(ads)

    producer = make_producer()
    delay = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    sent, errors = 0, 0

    def on_error(topic, event_id):
        def _cb(exc):
            nonlocal errors
            errors += 1
            print(f"  [ERROR] failed to deliver {topic} event {event_id}: {exc}")
        return _cb

    print(f"Streaming {len(views)} view events + {len(ads)} ad events "
          f"to {BOOTSTRAP_SERVERS} at ~{rate_per_sec}/sec. Ctrl+C to stop.\n")

    all_events = [("view", e) for e in views] + [("ad", e) for e in ads]
    random.shuffle(all_events)

    try:
        for kind, event in all_events:
            now = datetime.now(timezone.utc).isoformat()
            if kind == "view":
                event["timestamp"] = now
                topic = VIEWS_TOPIC
                key = str(event["event_id"])
            else:
                event["timestamp"] = now
                topic = ADS_TOPIC
                key = str(event["ad_event_id"])

            future = producer.send(topic, key=key.encode("utf-8"), value=event)
            future.add_errback(on_error(topic, key))
            sent += 1

            if sent % 100 == 0:
                producer.flush()
                print(f"  ...{sent} events sent so far ({errors} errors)")

            if delay:
                time.sleep(delay)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        producer.close()
        print(f"\nDone. Total sent: {sent}, errors: {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=2.0,
                         help="events per second to simulate (default 2)")
    args = parser.parse_args()
    stream_events(args.rate)
