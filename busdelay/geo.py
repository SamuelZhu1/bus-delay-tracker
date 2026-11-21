import math

EARTH_R_M = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_R_M * math.asin(math.sqrt(a))


def _to_xy(lat: float, lon: float, lat0: float) -> tuple[float, float]:
    """Local flat projection in metres; fine over the few hundred metres between two fixes."""
    x = math.radians(lon) * EARTH_R_M * math.cos(math.radians(lat0))
    y = math.radians(lat) * EARTH_R_M
    return x, y


def closest_approach(a: tuple[float, float], b: tuple[float, float], stop: tuple[float, float]) -> tuple[float, float]:
    """Closest point to `stop` on the straight segment a->b.

    Returns (distance in metres, fraction t along the segment in [0, 1]).
    Used to catch a bus that drove past a stop between two 30-second fixes.
    """
    lat0 = stop[0]
    ax, ay = _to_xy(*a, lat0)
    bx, by = _to_xy(*b, lat0)
    sx, sy = _to_xy(*stop, lat0)
    dx, dy = bx - ax, by - ay
    seg2 = dx * dx + dy * dy
    if seg2 == 0:
        return math.hypot(sx - ax, sy - ay), 0.0
    t = max(0.0, min(1.0, ((sx - ax) * dx + (sy - ay) * dy) / seg2))
    px, py = ax + t * dx, ay + t * dy
    return math.hypot(sx - px, sy - py), t
