"""Keeps TransLink's own arrival predictions so we can score them against real arrivals.

For each (trip, stop) we keep a sorted set of predictions by the time they were made,
so the writer can ask "what did TransLink predict 5 minutes before the bus got there?".
"""
import argparse
import json
import logging

import redis
from confluent_kafka import Consumer

from .config import TOPIC_PREDICTIONS, Config
from .messages import BadMessage, Prediction, decode_prediction

PRED_TTL_S = 3 * 3600
log = logging.getLogger("predictions")


def record(r, p: Prediction, prefix: str = "") -> None:
    key = f"{prefix}pred:{p.trip_id}:{p.stop_seq}"
    pipe = r.pipeline(transaction=False)
    pipe.zadd(key, {f"{p.feed_ts}:{p.predicted_arrival_ts}": p.feed_ts})
    pipe.expire(key, PRED_TTL_S)
    pipe.hset(
        f"{prefix}stoppred:{p.stop_id}",
        p.trip_id,
        json.dumps({"route_id": p.route_id, "stop_seq": p.stop_seq, "predicted_ts": p.predicted_arrival_ts, "feed_ts": p.feed_ts}),
    )
    pipe.expire(f"{prefix}stoppred:{p.stop_id}", PRED_TTL_S)
    pipe.execute()


def prediction_made_before(r, trip_id: str, stop_seq: int, before_ts: int, prefix: str = "") -> int | None:
    """Latest predicted arrival time that TransLink published at or before `before_ts`."""
    got = r.zrevrangebyscore(f"{prefix}pred:{trip_id}:{stop_seq}", before_ts, "-inf", start=0, num=1)
    if not got:
        return None
    member = got[0].decode() if isinstance(got[0], bytes) else got[0]
    return int(member.split(":")[1])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-prefix", default="")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    cfg = Config()
    r = redis.Redis.from_url(cfg.redis_url)
    c = Consumer({"bootstrap.servers": cfg.kafka_bootstrap, "group.id": "prediction-recorder", "auto.offset.reset": "earliest"})
    c.subscribe([TOPIC_PREDICTIONS])
    while True:
        for msg in c.consume(num_messages=500, timeout=1.0):
            if msg.error():
                log.warning("%s", msg.error())
                continue
            try:
                record(r, decode_prediction(msg.value()), args.redis_prefix)
            except BadMessage as e:
                log.warning("bad prediction: %s", e)


if __name__ == "__main__":
    main()
