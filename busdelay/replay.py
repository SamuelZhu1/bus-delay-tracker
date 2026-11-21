"""Produce a recording back into Kafka, through the same parsing and dedup as the live poller."""
import argparse
import time

from .config import Config
from .feed import DedupFilter, read_recording
from .poller import make_producer, publish


def replay(recording: str, bootstrap: str, speed: float = 0.0) -> dict:
    """speed=0 means as fast as possible; speed=10 means 10x real time."""
    producer = make_producer(bootstrap)
    dedup = DedupFilter()
    n_pos = n_pred = 0
    prev = None
    for fetched_at, pos_pb, trips_pb in read_recording(recording):
        if speed and prev is not None:
            time.sleep(max(0.0, (fetched_at - prev) / speed))
        prev = fetched_at
        a, b = publish(producer, dedup, pos_pb, trips_pb)
        n_pos += a
        n_pred += b
    producer.flush(60)
    return {"positions": n_pos, "predictions": n_pred, "duplicates_dropped": dedup.dropped}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("recording")
    ap.add_argument("--speed", type=float, default=0.0)
    a = ap.parse_args()
    print(replay(a.recording, Config().kafka_bootstrap, a.speed))


if __name__ == "__main__":
    main()
