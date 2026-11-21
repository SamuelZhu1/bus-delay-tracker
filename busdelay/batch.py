"""Offline ground truth: run the same detector over a recording with no Kafka, no Redis
and no crashes. The streaming pipeline must produce exactly this set of arrivals."""
import argparse
import json

from .detector import ArrivalDetector
from .feed import DedupFilter, parse_positions, read_recording
from .gtfs_static import load_schedule
from .messages import BadMessage, decode_position
from .state import DictStore


def ground_truth(recording: str, gtfs: str) -> dict:
    schedule, _ = load_schedule(gtfs)
    det = ArrivalDetector(DictStore(schedule))
    dedup = DedupFilter()
    arrivals = {}
    stats = {"positions": 0, "bad": 0, "vehicles_peak": 0, "polls": 0}
    for _, pos_pb, _ in read_recording(recording):
        raw = parse_positions(pos_pb)
        stats["polls"] += 1
        stats["vehicles_peak"] = max(stats["vehicles_peak"], len(raw))
        for d in dedup.new_only(raw):
            try:
                p = decode_position(json.dumps(d).encode())
            except BadMessage:
                stats["bad"] += 1
                continue
            stats["positions"] += 1
            for a in det.process(p):
                arrivals[a.arrival_id] = a
    stats["arrivals"] = len(arrivals)
    stats["duplicate_polls_dropped"] = dedup.dropped
    return {"arrivals": arrivals, "stats": stats}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("recording")
    ap.add_argument("gtfs")
    a = ap.parse_args()
    print(json.dumps(ground_truth(a.recording, a.gtfs)["stats"], indent=2))


if __name__ == "__main__":
    main()
