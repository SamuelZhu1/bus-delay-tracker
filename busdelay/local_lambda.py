"""Runs the writer Lambda locally against Kafka, packing records into the same event shape
AWS sends for a self-managed Kafka trigger. Offsets are committed after the handler returns,
which is also how the real trigger behaves (at-least-once)."""
import argparse
import base64
import time

from confluent_kafka import Consumer

from .config import TOPIC_ARRIVALS, Config
from .lambdas import writer


def to_event(msgs) -> dict:
    records: dict[str, list] = {}
    for m in msgs:
        records.setdefault(f"{m.topic()}-{m.partition()}", []).append(
            {
                "topic": m.topic(),
                "partition": m.partition(),
                "offset": m.offset(),
                "timestamp": m.timestamp()[1],
                "key": base64.b64encode(m.key() or b"").decode(),
                "value": base64.b64encode(m.value()).decode(),
            }
        )
    return {"eventSource": "SelfManagedKafka", "records": records}


def run(cfg: Config, group: str = "arrival-writer", batch: int = 100, idle_exit_s: float | None = None) -> dict:
    c = Consumer({"bootstrap.servers": cfg.kafka_bootstrap, "group.id": group, "enable.auto.commit": False, "auto.offset.reset": "earliest", "session.timeout.ms": 10000})
    c.subscribe([TOPIC_ARRIVALS])
    totals = {"written": 0, "duplicates_rejected": 0, "bad": 0}
    last = None
    try:
        while True:
            msgs = [m for m in c.consume(num_messages=batch, timeout=1.0) if not m.error()]
            if not msgs:
                if idle_exit_s is not None and last is not None and time.time() - last > idle_exit_s:
                    break
                continue
            last = None
            res = writer.handler(to_event(msgs))
            for k in totals:
                totals[k] += res[k]
            c.commit(asynchronous=False)
    finally:
        c.close()
    return totals


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", default="arrival-writer")
    ap.add_argument("--idle-exit", type=float, default=None)
    a = ap.parse_args()
    print(run(Config(), group=a.group, idle_exit_s=a.idle_exit))


if __name__ == "__main__":
    main()
