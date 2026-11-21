"""Turns a stream of bus positions into stop arrivals.

TransLink buses report about every 30 seconds, so a bus is rarely caught standing at a
stop. For each new fix we look at the straight segment from the previous fix and ask
which upcoming stops it passed within ARRIVAL_RADIUS_M of. Stops the bus skipped past
without a close pass (GPS off the road, a long gap in reports) get an interpolated time.
"""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from .config import ARRIVAL_RADIUS_M
from .geo import closest_approach, haversine_m
from .messages import Arrival, Position

TZ = ZoneInfo("America/Vancouver")
LOOKAHEAD = 15  # how many upcoming stops to check per segment


def midnight_ts(service_date: str) -> int:
    d = date.fromisoformat(service_date)
    return int(datetime(d.year, d.month, d.day, tzinfo=TZ).timestamp())


def pick_service_date(first_sched_s: int, fix_ts: int) -> str:
    """A trip that starts at 25:10 belongs to yesterday's service date; pick the date whose
    schedule lines up best with when we actually saw the bus."""
    local = datetime.fromtimestamp(fix_ts, TZ).date()
    best = min(
        (local - timedelta(days=1), local),
        key=lambda d: abs(midnight_ts(d.isoformat()) + first_sched_s - fix_ts),
    )
    return best.isoformat()


class ArrivalDetector:
    def __init__(self, store, radius_m: float = ARRIVAL_RADIUS_M):
        self.store = store
        self.radius = radius_m
        self.stale_dropped = 0
        self.unknown_trip = 0

    def process(self, p: Position) -> list[Arrival]:
        stops = self.store.trip_stops(p.trip_id)
        if not stops:
            self.unknown_trip += 1
            return []
        st = self.store.get_bus(p.vehicle_id)

        if st is None or st["trip_id"] != p.trip_id:
            return self._start_trip(p, stops)

        if p.ts <= st["last_ts"]:
            # Same fix re-sent, or an older fix arriving late: never move a bus backwards.
            self.stale_dropped += 1
            return []

        arrivals = self._scan(p, st, stops)
        st.update(last_lat=p.lat, last_lon=p.lon, last_ts=p.ts)
        self.store.put_bus(p.vehicle_id, st)
        return arrivals

    def _start_trip(self, p: Position, stops) -> list[Arrival]:
        # A bus can appear mid-trip (recording started late, or it just got a trip id).
        # Start from the nearest stop and only emit it if the bus is actually there.
        dists = [haversine_m(p.lat, p.lon, s.lat, s.lon) for s in stops]
        i = min(range(len(stops)), key=dists.__getitem__)
        st = {
            "trip_id": p.trip_id,
            "service_date": pick_service_date(stops[0].sched_s, p.ts),
            "next_idx": i,
            "last_lat": p.lat,
            "last_lon": p.lon,
            "last_ts": p.ts,
            "last_arrival_ts": 0,
        }
        out = []
        if dists[i] <= self.radius:
            out.append(self._arrival(p, st, stops[i], p.ts, "geofence"))
            st["next_idx"] = i + 1
            st["last_arrival_ts"] = p.ts
        self.store.put_bus(p.vehicle_id, st)
        return out

    def _scan(self, p: Position, st: dict, stops) -> list[Arrival]:
        i0 = st["next_idx"]
        if i0 >= len(stops):
            return []
        a = (st["last_lat"], st["last_lon"])
        b = (p.lat, p.lon)
        window = stops[i0 : i0 + LOOKAHEAD]
        hits = [closest_approach(a, b, (s.lat, s.lon)) for s in window]
        # Furthest stop in the window the bus passed close to on this segment.
        last_hit = max((k for k, (d, _) in enumerate(hits) if d <= self.radius), default=None)
        if last_hit is None:
            return []
        out = []
        dt = p.ts - st["last_ts"]
        prev_t = st["last_arrival_ts"]
        for k in range(last_hit + 1):
            d, t = hits[k]
            when = int(round(st["last_ts"] + t * dt))
            when = max(when, prev_t)  # arrival times along a trip never go backwards
            if d <= self.radius:
                end_d = haversine_m(p.lat, p.lon, window[k].lat, window[k].lon)
                method = "geofence" if end_d <= self.radius else "passed"
            else:
                method = "interpolated"
            out.append(self._arrival(p, st, window[k], when, method))
            prev_t = when
        st["next_idx"] = i0 + last_hit + 1
        st["last_arrival_ts"] = prev_t
        return out

    def _arrival(self, p: Position, st: dict, stop, when: int, method: str) -> Arrival:
        return Arrival(
            trip_id=p.trip_id,
            route_id=p.route_id,
            stop_id=stop.stop_id,
            stop_seq=stop.stop_seq,
            service_date=st["service_date"],
            scheduled_ts=midnight_ts(st["service_date"]) + stop.sched_s,
            actual_ts=when,
            method=method,
        )
