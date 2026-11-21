"""End-to-end crash test against a real Kafka broker, Redis and DynamoDB (moto server or DynamoDB Local).

1. Compute ground-truth arrivals offline from a recording (no Kafka, no crashes).
2. Replay the recording into Kafka through the poller's parsing and dedup.
3. Run the detector and kill -9 it N times at random points, restarting it each time.
4. Run the writer Lambda locally over stop.arrivals.
5. Compare DynamoDB with the ground truth: missing, extra, and duplicates.

Usage: python scripts/crash_test.py --recording data/day.jsonl.gz --gtfs data/gtfs --kills 20
Needs KAFKA_BOOTSTRAP, REDIS_URL and DYNAMODB_ENDPOINT.
"""
import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

import boto3
import redis
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from busdelay.batch import ground_truth  # noqa: E402
from busdelay.config import TABLE_ARRIVALS, TOPIC_ARRIVALS, TOPIC_DEAD_LETTER, TOPIC_POSITIONS, TOPIC_PREDICTIONS, Config  # noqa: E402
from busdelay.gtfs_static import load_schedule  # noqa: E402
from busdelay.local_lambda import run as run_writer  # noqa: E402
from busdelay.replay import replay  # noqa: E402
from busdelay.state import RedisStore  # noqa: E402
from busdelay.tables import create_tables  # noqa: E402


def reset_topics(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = [TOPIC_POSITIONS, TOPIC_PREDICTIONS, TOPIC_ARRIVALS, TOPIC_DEAD_LETTER]
    existing = set(admin.list_topics(timeout=10).topics)
    to_delete = [t for t in topics if t in existing]
    if to_delete:
        for f in admin.delete_topics(to_delete).values():
            f.result()
        time.sleep(3)
    for f in admin.create_topics([NewTopic(t, num_partitions=6, replication_factor=1) for t in topics]).values():
        f.result()


def reset_tables(ddb) -> None:
    for t in ddb.tables.all():
        t.delete()
        t.wait_until_not_exists()
    create_tables(ddb)


def scan_all(table) -> list[dict]:
    items, kw = [], {}
    while True:
        page = table.scan(**kw)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--recording", required=True)
    ap.add_argument("--gtfs", required=True)
    ap.add_argument("--kills", type=int, default=20)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default="results/crash_test.json")
    a = ap.parse_args()
    rng = random.Random(a.seed)
    cfg = Config()

    t0 = time.time()
    truth = ground_truth(a.recording, a.gtfs)
    expected = set(truth["arrivals"])
    print(f"ground truth: {len(expected)} arrivals from {truth['stats']['positions']} positions ({time.time() - t0:.1f}s)")

    r = redis.Redis.from_url(cfg.redis_url)
    r.flushdb()
    schedule, _ = load_schedule(a.gtfs)
    RedisStore(r).load_schedule(schedule)
    ddb = boto3.resource("dynamodb", endpoint_url=cfg.dynamodb_endpoint, region_name=cfg.aws_region)
    reset_tables(ddb)
    reset_topics(cfg.kafka_bootstrap)

    rep = replay(a.recording, cfg.kafka_bootstrap)
    print(f"replayed into Kafka: {rep}")

    group = f"crash-{int(time.time())}"
    cmd = [sys.executable, "-m", "busdelay.detector_service", "--group", group, "--idle-exit", "8"]
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    kills = 0
    proc = subprocess.Popen(cmd, env=env, stderr=subprocess.DEVNULL)
    while kills < a.kills:
        time.sleep(rng.uniform(4.0, 9.0))  # long enough to rejoin the group and process a few batches
        if proc.poll() is not None:
            break  # finished before we could kill it again
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        kills += 1
        proc = subprocess.Popen(cmd, env=env, stderr=subprocess.DEVNULL)
    proc.wait()
    print(f"detector killed {kills} times, then ran to the end")

    w = run_writer(cfg, group=f"{group}-writer", idle_exit_s=8)
    items = scan_all(ddb.Table(TABLE_ARRIVALS))
    ids = [i["arrival_id"] for i in items]
    got = set(ids)
    result = {
        "expected_arrivals": len(expected),
        "stored_items": len(items),
        "missing": len(expected - got),
        "extra": len(got - expected),
        "duplicate_items": len(ids) - len(got),
        "duplicates_rejected_by_conditional_put": w["duplicates_rejected"],
        "detector_kills": kills,
        "positions": truth["stats"]["positions"],
        "peak_vehicles": truth["stats"]["vehicles_peak"],
        "missing_examples": sorted(expected - got)[:5],
    }
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["missing"] == 0 and result["extra"] == 0 and result["duplicate_items"] == 0 else 1)


if __name__ == "__main__":
    main()
