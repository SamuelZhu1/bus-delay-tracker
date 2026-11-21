"""Polls TransLink's GTFS-realtime feeds and produces to Kafka.

Positions are keyed by vehicle_id so every fix from one bus lands on the same partition
in order; predictions are keyed by trip_id. Optionally saves every raw fetch to a
recording for replay tests.
"""
import argparse
import gzip
import json
import logging
import time

import requests
from confluent_kafka import Producer

from .config import TOPIC_POSITIONS, TOPIC_PREDICTIONS, Config
from .feed import POSITIONS_URL, TRIP_UPDATES_URL, DedupFilter, parse_positions, parse_predictions, write_recording_line

log = logging.getLogger("poller")


def make_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,  # broker drops producer retries, so a retry can't duplicate a fix
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )


def produce(producer: Producer, topic: str, key: bytes, value: bytes) -> None:
    """Produce, waiting for the local queue to drain instead of failing when it's full
    (a replay or a big trip-updates poll can queue tens of thousands of messages)."""
    while True:
        try:
            producer.produce(topic, key=key, value=value)
            return
        except BufferError:
            producer.poll(0.5)


def publish(producer: Producer, dedup: DedupFilter, positions_pb: bytes, trips_pb: bytes | None) -> tuple[int, int]:
    positions = dedup.new_only(parse_positions(positions_pb))
    for p in positions:
        produce(producer, TOPIC_POSITIONS, p["vehicle_id"].encode(), json.dumps(p).encode())
    preds = parse_predictions(trips_pb) if trips_pb else []
    for p in preds:
        produce(producer, TOPIC_PREDICTIONS, p["trip_id"].encode(), json.dumps(p).encode())
    producer.poll(0)
    return len(positions), len(preds)


def fetch(url: str, key: str, session: requests.Session) -> bytes:
    r = session.get(url, params={"apikey": key}, timeout=10)
    r.raise_for_status()
    return r.content


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--record", help="also save every raw fetch to this .jsonl.gz file")
    ap.add_argument("--no-kafka", action="store_true", help="record only")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    cfg = Config()
    if not cfg.translink_api_key:
        raise SystemExit("set TRANSLINK_API_KEY (free at developer.translink.ca)")

    producer = None if args.no_kafka else make_producer(cfg.kafka_bootstrap)
    dedup = DedupFilter()
    rec = gzip.open(args.record, "at", encoding="utf-8") if args.record else None
    session = requests.Session()
    while True:
        started = time.time()
        try:
            pos_pb = fetch(POSITIONS_URL, cfg.translink_api_key, session)
            trips_pb = fetch(TRIP_UPDATES_URL, cfg.translink_api_key, session)
            if rec:
                write_recording_line(rec, started, pos_pb, trips_pb)
                rec.flush()
            if producer:
                n_pos, n_pred = publish(producer, dedup, pos_pb, trips_pb)
                log.info("produced %d new positions, %d predictions", n_pos, n_pred)
        except requests.RequestException as e:
            # A failed poll just means a 15 s gap; the detector interpolates over it.
            log.warning("fetch failed: %s", e)
        time.sleep(max(0.0, cfg.poll_interval_s - (time.time() - started)))


if __name__ == "__main__":
    main()
