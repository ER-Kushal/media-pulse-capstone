"""
MediaPulse - Kafka Consumer (streams events into the warehouse)
=================================================================
Consumes from mediapulse.views / mediapulse.ads and inserts each event
directly into fact_view / fact_ad in Postgres, in real time.

Duplicate-event strategy:
    fact_view.event_id and fact_ad.ad_event_id are UNIQUE. Inserts use
    "ON CONFLICT ... DO NOTHING", so re-processing the same event (e.g.
    after a consumer restart re-reads uncommitted messages) is a no-op
    instead of a duplicate row. Combined with manual offset commits
    (only after a successful DB write), this gives effectively-once
    behavior even though Kafka itself only guarantees at-least-once.

Retry handling:
    Any DB error during a single message's processing is retried with
    exponential backoff (see `with_retry`). If it still fails after
    MAX_RETRIES, the event is logged to a local dead-letter file
    instead of crashing the whole consumer.

Basic monitoring:
    Every MONITOR_EVERY messages, prints a summary (processed, errors,
    dead-lettered, current rate).

Usage:
    pip install kafka-python sqlalchemy psycopg2-binary
    python kafka/consumer.py
"""

import json
import os
import time
import traceback
from datetime import datetime, timezone

from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = ["mediapulse.views", "mediapulse.ads"]
GROUP_ID = "mediapulse-warehouse-loader"

PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "mediapulse")
PGUSER = os.environ.get("PGUSER", "mediapulse")
PGPASSWORD = os.environ.get("PGPASSWORD", "mediapulse_dev_pw")

engine = create_engine(
    f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
)

MAX_RETRIES = 4
MONITOR_EVERY = 25
DEAD_LETTER_FILE = "kafka/dead_letter_events.jsonl"

# in-memory caches so we don't hit the DB for every single message
_cache = {"user": {}, "content": {}, "device": {}, "geography": {}, "campaign": {}}


def load_caches(conn):
    _cache["user"] = {
        r.user_id: (r.user_key, r.device, r.region)
        for r in conn.execute(text("SELECT user_id, user_key, device, region FROM dim_user"))
    }
    _cache["content"] = {
        r.content_id: r.content_key
        for r in conn.execute(text("SELECT content_id, content_key FROM dim_content"))
    }
    _cache["device"] = {
        r.device_name: r.device_key
        for r in conn.execute(text("SELECT device_name, device_key FROM dim_device"))
    }
    _cache["geography"] = {
        r.region: r.geography_key
        for r in conn.execute(text("SELECT region, geography_key FROM dim_geography"))
    }
    _cache["campaign"] = {
        r.campaign_id: r.campaign_key
        for r in conn.execute(text("SELECT campaign_id, campaign_key FROM dim_campaign"))
    }
    print(f"Loaded dimension caches: {sum(len(v) for v in _cache.values())} total keys")


def ensure_date_key(conn, dt):
    date_key = int(dt.strftime("%Y%m%d"))
    conn.execute(text("""
        INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name,
                               day, day_of_week, day_name, is_weekend)
        VALUES (:dk, :fd, :yr, :q, :mo, :mn, :d, :dow, :dn, :we)
        ON CONFLICT (date_key) DO NOTHING
    """), {
        "dk": date_key, "fd": dt.date(), "yr": dt.year, "q": (dt.month - 1) // 3 + 1,
        "mo": dt.month, "mn": dt.strftime("%B"), "d": dt.day,
        "dow": dt.weekday(), "dn": dt.strftime("%A"), "we": dt.weekday() >= 5,
    })
    return date_key


def process_view_event(conn, event):
    user_key, device, region = _cache["user"].get(event["user_id"], (None, None, None))
    content_key = _cache["content"].get(event["content_id"])
    if user_key is None or content_key is None:
        raise ValueError(f"unknown user_id/content_id in view event {event.get('event_id')}")
    device_key = _cache["device"].get(device)
    geography_key = _cache["geography"].get(region)
    ts = datetime.fromisoformat(event["timestamp"])
    date_key = ensure_date_key(conn, ts)

    conn.execute(text("""
        INSERT INTO fact_view (event_id, user_key, content_key, device_key, geography_key,
                                date_key, event_timestamp, event_type, watch_seconds, source_file)
        VALUES (:eid, :uk, :ck, :dk, :gk, :dtk, :ts, :et, :ws, 'kafka_stream')
        ON CONFLICT (event_id) DO NOTHING
    """), {
        "eid": event["event_id"], "uk": user_key, "ck": content_key,
        "dk": device_key, "gk": geography_key, "dtk": date_key,
        "ts": ts, "et": event.get("event_type"), "ws": event.get("watch_seconds"),
    })


def process_ad_event(conn, event):
    user_key, _, region = _cache["user"].get(event["user_id"], (None, None, None))
    content_key = _cache["content"].get(event["content_id"])
    campaign_key = _cache["campaign"].get(event["campaign_id"])
    if user_key is None or content_key is None or campaign_key is None:
        raise ValueError(f"unknown FK in ad event {event.get('ad_event_id')}")
    geography_key = _cache["geography"].get(region)
    ts = datetime.fromisoformat(event["timestamp"])
    date_key = ensure_date_key(conn, ts)

    conn.execute(text("""
        INSERT INTO fact_ad (ad_event_id, user_key, content_key, campaign_key, geography_key,
                              date_key, event_timestamp, impression, click, source_file)
        VALUES (:aeid, :uk, :ck, :cak, :gk, :dtk, :ts, :imp, :clk, 'kafka_stream')
        ON CONFLICT (ad_event_id) DO NOTHING
    """), {
        "aeid": event["ad_event_id"], "uk": user_key, "ck": content_key, "cak": campaign_key,
        "gk": geography_key, "dtk": date_key, "ts": ts,
        "imp": event.get("impression"), "clk": event.get("click"),
    })


def with_retry(fn, *args):
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            fn(*args)
            return True
        except Exception as exc:
            last_exc = exc
            wait = 0.5 * (2 ** (attempt - 1))
            print(f"  [RETRY {attempt}/{MAX_RETRIES}] {exc} -- waiting {wait:.1f}s")
            time.sleep(wait)
    dead_letter(args[-1] if args else None, last_exc)
    return False


def dead_letter(event, exc):
    os.makedirs(os.path.dirname(DEAD_LETTER_FILE), exist_ok=True)
    with open(DEAD_LETTER_FILE, "a") as f:
        f.write(json.dumps({"event": event, "error": str(exc),
                             "failed_at": datetime.now(timezone.utc).isoformat()}) + "\n")


def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    with engine.begin() as conn:
        load_caches(conn)

    processed, errors, dead_lettered = 0, 0, 0
    start = time.time()
    print(f"Listening on {TOPICS}... Ctrl+C to stop.\n")

    try:
        for msg in consumer:
            event = msg.value
            handler = process_view_event if msg.topic == TOPICS[0] else process_ad_event

            def run():
                with engine.begin() as conn:
                    handler(conn, event)

            ok = with_retry(lambda: run())
            if ok:
                processed += 1
                consumer.commit()
            else:
                errors += 1
                dead_lettered += 1
                consumer.commit()  # move past the poison message; it's saved to dead-letter file

            if (processed + errors) % MONITOR_EVERY == 0:
                elapsed = time.time() - start
                rate = (processed + errors) / elapsed if elapsed else 0
                print(f"[monitor] processed={processed} errors={errors} "
                      f"dead_lettered={dead_lettered} rate={rate:.1f}/s")
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        print(f"\nFinal: processed={processed} errors={errors} dead_lettered={dead_lettered}")
        consumer.close()


if __name__ == "__main__":
    main()
