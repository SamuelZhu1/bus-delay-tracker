"""Message schemas for the Kafka topics. Every message carries a schema version."""
import json
from dataclasses import asdict, dataclass

from .config import BBOX

SCHEMA_VERSION = 1


class BadMessage(ValueError):
    pass


@dataclass(frozen=True)
class Position:
    vehicle_id: str
    trip_id: str
    route_id: str
    lat: float
    lon: float
    ts: int  # unix seconds from the bus's own report
    bearing: float | None = None
    speed_mps: float | None = None

    def key(self) -> bytes:
        return self.vehicle_id.encode()


@dataclass(frozen=True)
class Prediction:
    trip_id: str
    route_id: str
    stop_id: str
    stop_seq: int
    predicted_arrival_ts: int
    feed_ts: int

    def key(self) -> bytes:
        return self.trip_id.encode()


@dataclass(frozen=True)
class Arrival:
    trip_id: str
    route_id: str
    stop_id: str
    stop_seq: int
    service_date: str
    scheduled_ts: int
    actual_ts: int
    method: str  # "geofence" | "passed" | "interpolated"

    @property
    def arrival_id(self) -> str:
        return f"{self.trip_id}:{self.stop_seq}:{self.service_date}"

    @property
    def delay_s(self) -> int:
        return self.actual_ts - self.scheduled_ts

    def key(self) -> bytes:
        return self.trip_id.encode()


def encode(msg) -> bytes:
    d = asdict(msg)
    d["v"] = SCHEMA_VERSION
    if isinstance(msg, Arrival):
        d["arrival_id"] = msg.arrival_id
    return json.dumps(d, separators=(",", ":")).encode()


def _load(raw: bytes) -> dict:
    try:
        d = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as e:
        raise BadMessage(f"not json: {e}") from e
    if not isinstance(d, dict) or d.get("v") != SCHEMA_VERSION:
        raise BadMessage(f"unknown schema version: {d.get('v') if isinstance(d, dict) else d!r}")
    return d


def decode_position(raw: bytes) -> Position:
    d = _load(raw)
    try:
        p = Position(
            vehicle_id=str(d["vehicle_id"]),
            trip_id=str(d["trip_id"]),
            route_id=str(d["route_id"]),
            lat=float(d["lat"]),
            lon=float(d["lon"]),
            ts=int(d["ts"]),
            bearing=d.get("bearing"),
            speed_mps=d.get("speed_mps"),
        )
    except (KeyError, TypeError, ValueError) as e:
        raise BadMessage(f"bad position: {e}") from e
    if not p.trip_id:
        raise BadMessage("position has no trip_id")
    min_lat, min_lon, max_lat, max_lon = BBOX
    if not (min_lat <= p.lat <= max_lat and min_lon <= p.lon <= max_lon):
        raise BadMessage(f"position outside Metro Vancouver: {p.lat},{p.lon}")
    return p


def decode_prediction(raw: bytes) -> Prediction:
    d = _load(raw)
    try:
        return Prediction(
            trip_id=str(d["trip_id"]),
            route_id=str(d["route_id"]),
            stop_id=str(d["stop_id"]),
            stop_seq=int(d["stop_seq"]),
            predicted_arrival_ts=int(d["predicted_arrival_ts"]),
            feed_ts=int(d["feed_ts"]),
        )
    except (KeyError, TypeError, ValueError) as e:
        raise BadMessage(f"bad prediction: {e}") from e


def decode_arrival(raw: bytes) -> Arrival:
    d = _load(raw)
    try:
        return Arrival(
            trip_id=str(d["trip_id"]),
            route_id=str(d["route_id"]),
            stop_id=str(d["stop_id"]),
            stop_seq=int(d["stop_seq"]),
            service_date=str(d["service_date"]),
            scheduled_ts=int(d["scheduled_ts"]),
            actual_ts=int(d["actual_ts"]),
            method=str(d["method"]),
        )
    except (KeyError, TypeError, ValueError) as e:
        raise BadMessage(f"bad arrival: {e}") from e
