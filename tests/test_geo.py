import pytest

from busdelay.geo import closest_approach, haversine_m


def test_haversine_one_degree_latitude():
    assert haversine_m(49.0, -123.0, 50.0, -123.0) == pytest.approx(111_195, rel=1e-3)


def test_closest_approach_catches_a_stop_passed_between_fixes():
    # Bus drives 400 m east along 49.26 N; the stop sits on the road halfway along.
    a, b = (49.26, -123.12), (49.26, -123.1145)
    stop = (49.26, -123.11725)
    d, t = closest_approach(a, b, stop)
    assert d < 1.0
    assert t == pytest.approx(0.5, abs=0.01)


def test_closest_approach_clamps_to_segment_ends():
    a, b = (49.26, -123.12), (49.26, -123.119)
    d, t = closest_approach(a, b, (49.26, -123.125))
    assert t == 0.0
    assert d == pytest.approx(haversine_m(49.26, -123.12, 49.26, -123.125), rel=1e-3)


def test_closest_approach_zero_length_segment():
    d, t = closest_approach((49.26, -123.12), (49.26, -123.12), (49.2601, -123.12))
    assert t == 0.0 and d == pytest.approx(11.1, abs=0.5)
