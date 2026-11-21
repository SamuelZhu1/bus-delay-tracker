"""GTFS-realtime parsing, duplicate filtering and the recorded-feed file format.

A recording is JSONL(.gz): one line per poll with the raw protobuf bytes of both feeds,
base64 encoded, so a replay goes through exactly the same parsing code as live polling.
"""
import base64
import gzip
import json
from collections.abc import Iterator
from pathlib import Path

from google.transit import gtfs_realtime_pb2

from . import messages as m

POSITIONS_URL = "https://gtfsapi.translink.ca/v3/gtfsposition"
TRIP_UPDATES_URL = "https://gtfsapi.translink.ca/v3/gtfsrealtime"


def parse_positions(raw: bytes) -> list[dict]:
    """Returns plain dicts (not validated yet: the detector sends bad ones to the dead letter topic)."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)
    out = []
    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle
        out.append(
            {
                "v": m.SCHEMA_VERSION,
                "vehicle_id": v.vehicle.id or e.id,
                "trip_id": v.trip.trip_id,
                "route_id": v.trip.route_id,
                "lat": v.position.latitude,
                "lon": v.position.longitude,
                "bearing": v.position.bearing if v.position.HasField("bearing") else None,
                "speed_mps": v.position.speed if v.position.HasField("speed") else None,
                "ts": int(v.timestamp or feed.header.timestamp),
            }
        )
    return out


def parse_predictions(raw: bytes) -> list[dict]:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)
    feed_ts = int(feed.header.timestamp)
    out = []
    for e in feed.entity:
        if not e.HasField("trip_update"):
            continue
        tu = e.trip_update
        for u in tu.stop_time_update:
            if not u.HasField("arrival") or not u.arrival.time:
                continue
            out.append(
                {
                    "v": m.SCHEMA_VERSION,
                    "trip_id": tu.trip.trip_id,
                    "route_id": tu.trip.route_id,
                    "stop_id": u.stop_id,
                    "stop_seq": int(u.stop_sequence),
                    "predicted_arrival_ts": int(u.arrival.time),
                    "feed_ts": feed_ts,
                }
            )
    return out


class DedupFilter:
    """TransLink refreshes positions about every 30 s and we poll every 15 s, so roughly
    half of each poll is the same report again. Drop a bus's fix unless it is newer."""

    def __init__(self):
        self.last_ts: dict[str, int] = {}
        self.dropped = 0

    def new_only(self, positions: list[dict]) -> list[dict]:
        out = []
        for p in positions:
            vid, ts = p["vehicle_id"], p["ts"]
            if ts <= self.last_ts.get(vid, -1):
                self.dropped += 1
                continue
            self.last_ts[vid] = ts
            out.append(p)
        return out


def write_recording_line(f, fetched_at: float, positions_pb: bytes, trips_pb: bytes | None) -> None:
    f.write(
        json.dumps(
            {
                "fetched_at": fetched_at,
                "positions": base64.b64encode(positions_pb).decode(),
                "trips": base64.b64encode(trips_pb).decode() if trips_pb else None,
            }
        )
        + "\n"
    )


def read_recording(path: str | Path) -> Iterator[tuple[float, bytes, bytes | None]]:
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            d = json.loads(line)
            trips = base64.b64decode(d["trips"]) if d.get("trips") else None
            yield d["fetched_at"], base64.b64decode(d["positions"]), trips
