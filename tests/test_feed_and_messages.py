import json

import pytest
from google.transit import gtfs_realtime_pb2

from busdelay.feed import DedupFilter, parse_positions, parse_predictions
from busdelay.messages import Arrival, BadMessage, decode_arrival, decode_position, encode


def _feed(entities):
    f = gtfs_realtime_pb2.FeedMessage()
    f.header.gtfs_realtime_version = "2.0"
    f.header.timestamp = 1000
    for vid, trip, ts in entities:
        e = f.entity.add()
        e.id = vid
        e.vehicle.vehicle.id = vid
        e.vehicle.trip.trip_id = trip
        e.vehicle.trip.route_id = "99"
        e.vehicle.position.latitude = 49.26
        e.vehicle.position.longitude = -123.1
        e.vehicle.timestamp = ts
    return f.SerializeToString()


def test_parse_positions_reads_vehicle_entities():
    out = parse_positions(_feed([("V1", "T1", 990), ("V2", "T2", 995)]))
    assert [(p["vehicle_id"], p["trip_id"], p["ts"]) for p in out] == [("V1", "T1", 990), ("V2", "T2", 995)]


def test_dedup_drops_repeated_reports_between_polls():
    d = DedupFilter()
    assert len(d.new_only(parse_positions(_feed([("V1", "T1", 990)])))) == 1
    assert d.new_only(parse_positions(_feed([("V1", "T1", 990)]))) == []
    assert len(d.new_only(parse_positions(_feed([("V1", "T1", 1020)])))) == 1
    assert d.dropped == 1


def test_parse_predictions():
    f = gtfs_realtime_pb2.FeedMessage()
    f.header.gtfs_realtime_version = "2.0"
    f.header.timestamp = 1000
    e = f.entity.add()
    e.id = "T1"
    e.trip_update.trip.trip_id = "T1"
    e.trip_update.trip.route_id = "99"
    u = e.trip_update.stop_time_update.add()
    u.stop_sequence, u.stop_id, u.arrival.time = 3, "S3", 1300
    assert parse_predictions(f.SerializeToString()) == [
        {"v": 1, "trip_id": "T1", "route_id": "99", "stop_id": "S3", "stop_seq": 3, "predicted_arrival_ts": 1300, "feed_ts": 1000}
    ]


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        json.dumps({"v": 2, "vehicle_id": "V1"}).encode(),
        json.dumps({"v": 1, "vehicle_id": "V1", "trip_id": "", "route_id": "99", "lat": 49.2, "lon": -123.1, "ts": 1}).encode(),
        json.dumps({"v": 1, "vehicle_id": "V1", "trip_id": "T", "route_id": "99", "lat": 0.0, "lon": 0.0, "ts": 1}).encode(),
        json.dumps({"v": 1, "vehicle_id": "V1", "trip_id": "T"}).encode(),
    ],
)
def test_bad_positions_are_rejected(raw):
    with pytest.raises(BadMessage):
        decode_position(raw)


def test_arrival_round_trip_keeps_id():
    a = Arrival("T1", "99", "S1", 1, "2026-11-17", 100, 160, "geofence")
    raw = encode(a)
    assert json.loads(raw)["arrival_id"] == "T1:1:2026-11-17"
    assert decode_arrival(raw) == a
