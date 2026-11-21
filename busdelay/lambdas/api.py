"""AWS Lambda behind API Gateway.

GET /stops/{stop_id}/next?route=99   next buses at a stop: TransLink's prediction next to
                                     ours (schedule + the delay we last measured on that trip)
GET /routes/{route_id}/accuracy      how far off TransLink's 5-minute predictions run, by hour
"""
import json
import os
import time

import boto3
import redis
from boto3.dynamodb.conditions import Key

from ..config import TABLE_ROUTE_STATS
from ..gtfs_static import from_json
from ..detector import midnight_ts, pick_service_date

_ddb = None
_redis = None


def _clients():
    global _ddb, _redis
    if _ddb is None:
        _ddb = boto3.resource("dynamodb", endpoint_url=os.getenv("DYNAMODB_ENDPOINT") or None, region_name=os.getenv("AWS_REGION", "ca-central-1"))
    if _redis is None:
        _redis = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    return _ddb, _redis


def _resp(code: int, body) -> dict:
    return {"statusCode": code, "headers": {"content-type": "application/json"}, "body": json.dumps(body)}


def next_buses(r, stop_id: str, route: str | None, now: int, prefix: str = "") -> list[dict]:
    out = []
    for trip_id, raw in r.hgetall(f"{prefix}stoppred:{stop_id}").items():
        trip_id = trip_id.decode() if isinstance(trip_id, bytes) else trip_id
        p = json.loads(raw)
        if route and p["route_id"] != route:
            continue
        if p["predicted_ts"] < now - 60:
            continue  # already gone
        ours = None
        stops_raw = r.get(f"{prefix}stops:{trip_id}")
        delay = r.get(f"{prefix}tripdelay:{trip_id}")
        if stops_raw and delay is not None:
            stops = from_json(stops_raw)
            match = [s for s in stops if s.stop_seq == p["stop_seq"]]
            if match:
                sd = pick_service_date(stops[0].sched_s, now)
                ours = midnight_ts(sd) + match[0].sched_s + int(delay)
        out.append({"trip_id": trip_id, "route_id": p["route_id"], "translink_eta_ts": p["predicted_ts"], "our_eta_ts": ours})
    return sorted(out, key=lambda b: b["translink_eta_ts"])[:5]


def route_accuracy(stats_table, route_id: str) -> list[dict]:
    items = stats_table.query(KeyConditionExpression=Key("route_id").eq(route_id))["Items"]
    return [
        {"hour": int(i["hour"]), "n": int(i["n"]), "mean_abs_err_5m_s": round(float(i["sum_abs_err_5m"]) / int(i["n"]), 1)}
        for i in sorted(items, key=lambda i: int(i["hour"]))
    ]


def handler(event, context=None):
    ddb, r = _clients()
    path = event.get("rawPath") or event.get("path", "")
    params = event.get("pathParameters") or {}
    query = event.get("queryStringParameters") or {}
    prefix = os.getenv("REDIS_PREFIX", "")
    if path.startswith("/stops/") and path.endswith("/next"):
        return _resp(200, {"stop_id": params["stop_id"], "buses": next_buses(r, params["stop_id"], query.get("route"), int(time.time()), prefix)})
    if path.startswith("/routes/") and path.endswith("/accuracy"):
        return _resp(200, {"route_id": params["route_id"], "by_hour": route_accuracy(ddb.Table(TABLE_ROUTE_STATS), params["route_id"])})
    return _resp(404, {"error": f"no route for {path}"})
