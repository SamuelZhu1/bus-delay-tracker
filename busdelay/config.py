import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    dynamodb_endpoint: str | None = os.getenv("DYNAMODB_ENDPOINT") or None
    aws_region: str = os.getenv("AWS_REGION", "ca-central-1")
    translink_api_key: str = os.getenv("TRANSLINK_API_KEY", "")
    poll_interval_s: float = float(os.getenv("POLL_INTERVAL_S", "15"))


TOPIC_POSITIONS = "vehicle.positions"
TOPIC_PREDICTIONS = "trip.predictions"
TOPIC_ARRIVALS = "stop.arrivals"
TOPIC_DEAD_LETTER = "dead.letter"

TABLE_ARRIVALS = os.getenv("TABLE_ARRIVALS", "arrivals")
TABLE_ROUTE_STATS = os.getenv("TABLE_ROUTE_STATS", "route_stats")

# A bus counts as arriving when it comes within this many metres of the stop.
ARRIVAL_RADIUS_M = 30.0
# Metro Vancouver bounding box; anything outside goes to the dead letter topic.
BBOX = (48.9, -123.5, 49.6, -122.2)  # min_lat, min_lon, max_lat, max_lon
