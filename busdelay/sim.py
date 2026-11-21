"""Synthetic TransLink-like day, for tests and for trying the pipeline without an API key.

Writes a small static GTFS folder and a recording in the same format the poller saves,
built from real GTFS-realtime protobuf messages. Buses drive their routes with drifting
delays, GPS noise, 30 s reporting, occasional report gaps, and a few broken messages.
The real measurements come from a recorded TransLink day, not from this.
"""
import argparse
import csv
import gzip
import math
import random
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2

from .feed import write_recording_line

TZ = ZoneInfo("America/Vancouver")
CENTER = (49.2600, -123.1150)


def _offset(lat, lon, dx_m, dy_m):
    return lat + dy_m / 111_320.0, lon + dx_m / (111_320.0 * math.cos(math.radians(lat)))


def build_network(rng: random.Random, n_routes: int, stops_per_route: int = 20):
    """Straight-ish routes radiating across the city, stops about 350 m apart."""
    routes = {}
    for ri in range(n_routes):
        ang = rng.uniform(0, 2 * math.pi)
        start = _offset(*CENTER, rng.uniform(-6000, 6000), rng.uniform(-5000, 5000))
        stops = []
        lat, lon = start
        for si in range(stops_per_route):
            stops.append((f"{ri:03d}{si:02d}", lat, lon))
            ang += rng.uniform(-0.25, 0.25)
            step = rng.uniform(280, 420)
            lat, lon = _offset(lat, lon, step * math.cos(ang), step * math.sin(ang))
        routes[f"R{ri:03d}"] = stops
    return routes


def write_gtfs(out: Path, routes, trips):
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "stops.txt", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon"])
        for stops in routes.values():
            for sid, lat, lon in stops:
                w.writerow([sid, f"Stop {sid}", f"{lat:.6f}", f"{lon:.6f}"])
    with open(out / "trips.txt", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["route_id", "service_id", "trip_id"])
        for t in trips:
            w.writerow([t["route_id"], "WKDY", t["trip_id"]])
    with open(out / "stop_times.txt", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])
        for t in trips:
            for seq, (sid, s) in enumerate(zip(t["stop_ids"], t["sched"]), start=1):
                hhmmss = f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"
                w.writerow([t["trip_id"], hhmmss, hhmmss, sid, seq])


def simulate(out_dir: str, n_routes: int = 40, headway_min: int = 12, start_h: int = 5, end_h: int = 25,
             day: str = "2026-11-17", seed: int = 7, poll_s: int = 15, report_s: int = 30) -> dict:
    rng = random.Random(seed)
    out = Path(out_dir)
    routes = build_network(rng, n_routes)
    trips = []
    for rid, stops in routes.items():
        seg_s = [rng.randint(55, 85) for _ in stops[1:]]
        for k, dep in enumerate(range(start_h * 3600 + rng.randint(0, headway_min * 60), end_h * 3600, headway_min * 60)):
            sched = [dep]
            for s in seg_s:
                sched.append(sched[-1] + s)
            trips.append({"trip_id": f"{rid}-{k:03d}", "route_id": rid, "vehicle_id": f"{rid[1:]}{k % 9}",
                          "stop_ids": [s[0] for s in stops], "coords": [(s[1], s[2]) for s in stops], "sched": sched})
    write_gtfs(out / "gtfs", routes, trips)

    d = date.fromisoformat(day)
    midnight = int(datetime(d.year, d.month, d.day, tzinfo=TZ).timestamp())

    # Drive each trip: delay drifts like a random walk (traffic), bus dwells ~20 s at stops.
    tracks = {}
    for t in trips:
        delay = rng.gauss(30, 60)
        times = []
        for i, s in enumerate(t["sched"]):
            delay = max(-60.0, delay + rng.gauss(4, 18))
            times.append(midnight + s + delay)
        tracks[t["trip_id"]] = times

    def position_at(t, ts):
        times = tracks[t["trip_id"]]
        if ts < times[0] - 60 or ts > times[-1]:
            return None
        c = t["coords"]
        if ts <= times[0]:
            return c[0]
        for i in range(len(times) - 1):
            arrive, nxt = times[i], times[i + 1]
            leave = min(arrive + 20, nxt)
            if ts <= leave:
                return c[i]
            if ts <= nxt:
                f = (ts - leave) / max(1.0, nxt - leave)
                return c[i][0] + f * (c[i + 1][0] - c[i][0]), c[i][1] + f * (c[i + 1][1] - c[i][1])
        return c[-1]

    # Each bus reports every ~30 s with jitter; the feed shows its latest report.
    report_offset = {t["trip_id"]: rng.uniform(0, report_s) for t in trips}
    gaps = {t["trip_id"]: (rng.randint(0, len(t["sched"])) if rng.random() < 0.08 else None) for t in trips}
    rec_path = out / "day.jsonl.gz"
    n_polls = 0
    first, last = midnight + start_h * 3600 - 120, midnight + end_h * 3600 + 3600
    with gzip.open(rec_path, "wt", encoding="utf-8") as rec:
        for now in range(first, last, poll_s):
            pos = gtfs_realtime_pb2.FeedMessage()
            pos.header.gtfs_realtime_version = "2.0"
            pos.header.timestamp = now
            tu = gtfs_realtime_pb2.FeedMessage()
            tu.header.gtfs_realtime_version = "2.0"
            tu.header.timestamp = now
            any_active = False
            for t in trips:
                times = tracks[t["trip_id"]]
                if now < times[0] - 90 or now > times[-1] + 30:
                    continue
                any_active = True
                k = math.floor((now - report_offset[t["trip_id"]]) / report_s)
                rep_ts = int(k * report_s + report_offset[t["trip_id"]])
                g = gaps[t["trip_id"]]
                if g is not None and times[min(g, len(times) - 1)] <= rep_ts <= times[min(g, len(times) - 1)] + 150:
                    rep_ts = int(times[min(g, len(times) - 1)])  # radio gap: report stuck for ~2.5 min
                p = position_at(t, rep_ts)
                if p is None:
                    continue
                lat, lon = _offset(p[0], p[1], rng.gauss(0, 8), rng.gauss(0, 8))
                e = pos.entity.add()
                e.id = t["vehicle_id"]
                v = e.vehicle
                v.vehicle.id = t["vehicle_id"]
                v.trip.trip_id = "" if rng.random() < 0.001 else t["trip_id"]
                v.trip.route_id = t["route_id"]
                v.position.latitude, v.position.longitude = lat, lon
                v.timestamp = rep_ts
                # TransLink-style predictions: our future delay guess drifts with lead time.
                cur_delay = rep_ts - midnight - t["sched"][0] if rep_ts < times[0] else None
                ue = tu.entity.add()
                ue.id = t["trip_id"]
                ue.trip_update.trip.trip_id = t["trip_id"]
                ue.trip_update.trip.route_id = t["route_id"]
                passed = sum(1 for x in times if x <= now)
                known_delay = (times[passed - 1] - midnight - t["sched"][passed - 1]) if passed else 0
                for i in range(passed, min(passed + 8, len(times))):
                    stu = ue.trip_update.stop_time_update.add()
                    stu.stop_sequence = i + 1
                    stu.stop_id = t["stop_ids"][i]
                    stu.arrival.time = int(midnight + t["sched"][i] + known_delay + rng.gauss(0, 20 * (i - passed + 1) ** 0.5))
            if any_active:
                write_recording_line(rec, now, pos.SerializeToString(), tu.SerializeToString())
                n_polls += 1
    return {"trips": len(trips), "routes": n_routes, "polls": n_polls, "recording": str(rec_path), "gtfs": str(out / "gtfs")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--routes", type=int, default=40)
    ap.add_argument("--start-h", type=int, default=5)
    ap.add_argument("--end-h", type=int, default=25)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    print(simulate(a.out, n_routes=a.routes, start_h=a.start_h, end_h=a.end_h, seed=a.seed))


if __name__ == "__main__":
    main()
