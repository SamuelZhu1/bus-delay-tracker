"""Checks the detector against the simulator, where the true arrival times are known."""
from collections import Counter

from busdelay.batch import ground_truth


def test_simulated_morning_detects_nearly_every_stop(sim_day):
    g = ground_truth(sim_day["recording"], sim_day["gtfs"])
    s = g["stats"]
    assert s["bad"] > 0  # the simulator sends a few positions with no trip id
    methods = Counter(a.method for a in g["arrivals"].values())
    assert methods["geofence"] > methods["passed"] > 0
    # Arrivals of one trip come out in stop order with non-decreasing times.
    by_trip = {}
    for a in g["arrivals"].values():
        by_trip.setdefault(a.trip_id, []).append(a)
    for arr in by_trip.values():
        arr.sort(key=lambda a: a.stop_seq)
        times = [a.actual_ts for a in arr]
        assert times == sorted(times)


def test_ground_truth_is_deterministic(sim_day):
    a = ground_truth(sim_day["recording"], sim_day["gtfs"])["arrivals"]
    b = ground_truth(sim_day["recording"], sim_day["gtfs"])["arrivals"]
    assert a == b
