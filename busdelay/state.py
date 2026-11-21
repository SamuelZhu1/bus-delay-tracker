"""Where the detector keeps per-bus state and the trip schedule.

Redis in production (every position needs the bus's last state, thousands of times a
minute); a plain dict for the offline ground truth and unit tests.
"""
import json

from .gtfs_static import TripStop, from_json, to_json

BUS_TTL_S = 30 * 60


class DictStore:
    def __init__(self, schedule: dict[str, list[TripStop]] | None = None):
        self.buses: dict[str, dict] = {}
        self.schedule = schedule or {}

    def get_bus(self, vehicle_id: str) -> dict | None:
        return self.buses.get(vehicle_id)

    def put_bus(self, vehicle_id: str, state: dict) -> None:
        self.buses[vehicle_id] = dict(state)

    def trip_stops(self, trip_id: str) -> list[TripStop] | None:
        return self.schedule.get(trip_id)


class RedisStore:
    def __init__(self, r, prefix: str = ""):
        self.r = r
        self.p = prefix
        self._cache: dict[str, list[TripStop] | None] = {}

    def get_bus(self, vehicle_id: str) -> dict | None:
        raw = self.r.get(f"{self.p}bus:{vehicle_id}")
        return json.loads(raw) if raw else None

    def put_bus(self, vehicle_id: str, state: dict) -> None:
        self.r.set(f"{self.p}bus:{vehicle_id}", json.dumps(state), ex=BUS_TTL_S)

    def trip_stops(self, trip_id: str) -> list[TripStop] | None:
        # Schedules don't change during the day, so cache them in the process.
        if trip_id not in self._cache:
            raw = self.r.get(f"{self.p}stops:{trip_id}")
            self._cache[trip_id] = from_json(raw) if raw else None
        return self._cache[trip_id]

    def load_schedule(self, schedule: dict[str, list[TripStop]]) -> int:
        pipe = self.r.pipeline(transaction=False)
        for trip_id, stops in schedule.items():
            pipe.set(f"{self.p}stops:{trip_id}", to_json(stops))
        pipe.execute()
        return len(schedule)
