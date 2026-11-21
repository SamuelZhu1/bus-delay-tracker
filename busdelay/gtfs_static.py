"""Loads the static GTFS schedule (stops, trips, stop_times) the detector needs.

TransLink publishes it as a zip; we only need, for each trip, its ordered stops with
coordinates and scheduled arrival times (seconds after local midnight, may pass 24:00).
"""
import csv
import io
import json
import zipfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TripStop:
    stop_seq: int
    stop_id: str
    lat: float
    lon: float
    sched_s: int  # seconds after local midnight of the service date


def parse_gtfs_time(s: str) -> int:
    h, m, sec = (int(x) for x in s.strip().split(":"))
    return h * 3600 + m * 60 + sec


def _read(source: Path, name: str):
    if source.is_dir():
        with open(source / name, newline="", encoding="utf-8-sig") as f:
            yield from csv.DictReader(f)
    else:
        with zipfile.ZipFile(source) as z, z.open(name) as raw:
            yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))


def load_schedule(source: str | Path) -> tuple[dict[str, list[TripStop]], dict[str, str]]:
    """Returns ({trip_id: [TripStop...] in stop order}, {trip_id: route_id})."""
    source = Path(source)
    stops = {r["stop_id"]: (float(r["stop_lat"]), float(r["stop_lon"])) for r in _read(source, "stops.txt")}
    trip_route = {r["trip_id"]: r["route_id"] for r in _read(source, "trips.txt")}
    trips: dict[str, list[TripStop]] = {}
    for r in _read(source, "stop_times.txt"):
        lat, lon = stops[r["stop_id"]]
        t = r.get("arrival_time") or r.get("departure_time")
        trips.setdefault(r["trip_id"], []).append(
            TripStop(int(r["stop_sequence"]), r["stop_id"], lat, lon, parse_gtfs_time(t))
        )
    for lst in trips.values():
        lst.sort(key=lambda s: s.stop_seq)
    return trips, trip_route


def to_json(stops: list[TripStop]) -> str:
    return json.dumps([[s.stop_seq, s.stop_id, s.lat, s.lon, s.sched_s] for s in stops])


def from_json(raw: str | bytes) -> list[TripStop]:
    return [TripStop(int(a), str(b), float(c), float(d), int(e)) for a, b, c, d, e in json.loads(raw)]
