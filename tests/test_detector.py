from busdelay.detector import ArrivalDetector, midnight_ts, pick_service_date
from busdelay.gtfs_static import TripStop
from busdelay.messages import Position
from busdelay.state import DictStore

DAY = "2026-11-17"
M = midnight_ts(DAY)
# Four stops about 350 m apart going east along 49.26 N, scheduled 8:00:00 + 70 s each.
STOPS = [TripStop(i + 1, f"S{i + 1}", 49.26, -123.12 + i * 0.0048, 8 * 3600 + i * 70) for i in range(4)]


def det():
    return ArrivalDetector(DictStore({"T1": STOPS}))


def pos(lon, ts, lat=49.26, trip="T1", vid="V1"):
    return Position(vehicle_id=vid, trip_id=trip, route_id="99", lat=lat, lon=lon, ts=M + ts)


def test_bus_waiting_at_first_stop_arrives_there():
    d = det()
    out = d.process(pos(-123.12, 8 * 3600 - 30))
    assert [a.stop_id for a in out] == ["S1"]
    assert out[0].method == "geofence"
    assert out[0].delay_s == -30


def test_stop_passed_between_two_fixes_is_timed_by_interpolation_along_segment():
    d = det()
    d.process(pos(-123.12, 8 * 3600))  # at S1
    out = d.process(pos(-123.12 + 0.0072, 8 * 3600 + 60))  # halfway between S2 and S3
    assert [a.stop_id for a in out] == ["S2"]
    assert out[0].method == "passed"
    assert abs(out[0].actual_ts - (M + 8 * 3600 + 40)) <= 1  # 2/3 of the way, at 40 s


def test_gps_offset_from_stop_still_counts_as_interpolated_when_a_later_stop_is_hit():
    d = det()
    d.process(pos(-123.12, 8 * 3600))
    # Previous fix was 150 m off the road (bad GPS), so the straight line to S3 misses S2.
    d.store.buses["V1"]["last_lat"] = 49.26135
    out = d.process(pos(-123.12 + 2 * 0.0048, 8 * 3600 + 150))
    assert [(a.stop_id, a.method) for a in out] == [("S2", "interpolated"), ("S3", "geofence")]


def test_old_or_repeated_fix_is_dropped_and_never_moves_the_bus_back():
    d = det()
    d.process(pos(-123.12, 8 * 3600))
    d.process(pos(-123.12 + 0.0048, 8 * 3600 + 70))
    assert d.process(pos(-123.12, 8 * 3600 + 30)) == []
    assert d.process(pos(-123.12 + 0.0048, 8 * 3600 + 70)) == []
    assert d.stale_dropped == 2


def test_bus_first_seen_mid_trip_does_not_invent_earlier_arrivals():
    d = det()
    out = d.process(pos(-123.12 + 0.0060, 8 * 3600 + 90))  # between S2 and S3
    assert out == []
    out = d.process(pos(-123.12 + 3 * 0.0048, 8 * 3600 + 200))
    assert [a.stop_id for a in out] == ["S3", "S4"]


def test_each_stop_reported_once_per_trip():
    d = det()
    seen = []
    for k in range(0, 300, 10):
        seen += d.process(pos(-123.12 + k * 0.0048 / 70, 8 * 3600 + k))
    ids = [a.stop_id for a in seen]
    assert ids == ["S1", "S2", "S3", "S4"]


def test_unknown_trip_is_counted_not_crashed():
    d = det()
    assert d.process(pos(-123.12, 8 * 3600, trip="NOPE")) == []
    assert d.unknown_trip == 1


def test_after_midnight_trip_belongs_to_previous_service_date():
    # A trip scheduled at 25:10 (1:10 AM next day) belongs to the earlier service date.
    ts = M + 25 * 3600 + 600
    assert pick_service_date(25 * 3600 + 600, ts) == DAY


def test_arrival_ids_are_stable_for_idempotent_writes():
    a = det().process(pos(-123.12, 8 * 3600))[0]
    b = det().process(pos(-123.12, 8 * 3600))[0]
    assert a.arrival_id == b.arrival_id == f"T1:1:{DAY}"
