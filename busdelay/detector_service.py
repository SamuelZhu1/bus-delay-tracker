"""Kafka consumer that runs the arrival detector.

Delivery is at-least-once: offsets are committed only after the arrivals produced from
those positions are acknowledged by the broker. If this process dies in between, the
restarted consumer reprocesses a few positions and may re-emit arrivals it already sent.
That's fine because every arrival has a deterministic arrival_id and the DynamoDB writer
rejects repeats with a conditional put.
"""
import argparse
import json
import logging
import signal
import time

import redis
from confluent_kafka import Consumer, KafkaException, Producer

from .config import TOPIC_ARRIVALS, TOPIC_DEAD_LETTER, TOPIC_POSITIONS, Config
from .detector import ArrivalDetector
from .messages import BadMessage, decode_position, encode
from .poller import produce
from .state import RedisStore

log = logging.getLogger("detector")


def dead_letter(producer: Producer, msg, err: str) -> None:
    producer.produce(
        TOPIC_DEAD_LETTER,
        value=json.dumps(
            {
                "source_topic": msg.topic(),
                "raw": msg.value().decode("utf-8", "replace"),
                "error": err,
                "ts": int(time.time()),
            }
        ).encode(),
    )


def run(cfg: Config, group: str = "arrival-detector", redis_prefix: str = "", batch: int = 500, idle_exit_s: float | None = None) -> None:
    store = RedisStore(redis.Redis.from_url(cfg.redis_url), prefix=redis_prefix)
    det = ArrivalDetector(store)
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            # A crashed consumer holds its partitions until this expires; keep recovery quick.
            "session.timeout.ms": 10000,
        }
    )
    producer = Producer({"bootstrap.servers": cfg.kafka_bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 20})
    consumer.subscribe([TOPIC_POSITIONS])
    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    last_msg = None  # idle timer starts after the first message, not while joining the group
    n_pos = n_arr = n_bad = 0
    try:
        while not stop:
            msgs = consumer.consume(num_messages=batch, timeout=1.0)
            if not msgs:
                if idle_exit_s is not None and last_msg is not None and time.time() - last_msg > idle_exit_s:
                    break
                continue
            last_msg = time.time()
            for msg in msgs:
                if msg.error():
                    raise KafkaException(msg.error())
                try:
                    p = decode_position(msg.value())
                except BadMessage as e:
                    dead_letter(producer, msg, str(e))
                    n_bad += 1
                    continue
                for a in det.process(p):
                    produce(producer, TOPIC_ARRIVALS, a.key(), encode(a))
                    n_arr += 1
                n_pos += 1
            # Commit only once everything produced from this batch is on the broker.
            if producer.flush(30) != 0:
                raise RuntimeError("arrivals not acknowledged; not committing offsets")
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        log.info("positions=%d arrivals=%d dead_letter=%d stale=%d", n_pos, n_arr, n_bad, det.stale_dropped)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", default="arrival-detector")
    ap.add_argument("--redis-prefix", default="")
    ap.add_argument("--idle-exit", type=float, default=None, help="exit after this many idle seconds (tests)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run(Config(), group=args.group, redis_prefix=args.redis_prefix, idle_exit_s=args.idle_exit)


if __name__ == "__main__":
    main()
