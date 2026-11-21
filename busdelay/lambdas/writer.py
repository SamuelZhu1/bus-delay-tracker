"""AWS Lambda triggered by the self-managed Kafka topic `stop.arrivals`.

Each arrival is written once with a conditional put, so an arrival the detector re-sends
after a crash (at-least-once delivery) is rejected instead of stored twice. Only a write
that actually created the item updates the per-route accuracy stats.
"""
import base64
import logging
import os
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import boto3
import redis
from botocore.exceptions import ClientError

from ..config import TABLE_ARRIVALS, TABLE_ROUTE_STATS
from ..messages import Arrival, BadMessage, decode_arrival
from ..predictions import prediction_made_before

log = logging.getLogger()
TZ = ZoneInfo("America/Vancouver")
LEAD_TIMES_S = (300, 600)

_ddb = None
_redis = None


def _clients():
    global _ddb, _redis
    if _ddb is None:
        _ddb = boto3.resource("dynamodb", endpoint_url=os.getenv("DYNAMODB_ENDPOINT") or None, region_name=os.getenv("AWS_REGION", "ca-central-1"))
    if _redis is None and os.getenv("REDIS_URL"):
        _redis = redis.Redis.from_url(os.environ["REDIS_URL"])
    return _ddb, _redis


def item_for(a: Arrival, errors: dict[int, int | None]) -> dict:
    item = {
        "pk": f"{a.route_id}#{a.stop_id}#{a.service_date}",
        "sk": f"{a.scheduled_ts}#{a.trip_id}",
        "arrival_id": a.arrival_id,
        "trip_id": a.trip_id,
        "stop_seq": a.stop_seq,
        "scheduled_ts": a.scheduled_ts,
        "actual_ts": a.actual_ts,
        "delay_s": a.delay_s,
        "method": a.method,
    }
    for lead, err in errors.items():
        if err is not None:
            item[f"pred_err_{lead // 60}m_s"] = err
    return item


def write_arrival(table, stats_table, r, a: Arrival, prefix: str = "") -> bool:
    """Returns True if this call stored the arrival, False if it was already there."""
    errors = {}
    for lead in LEAD_TIMES_S:
        pred = prediction_made_before(r, a.trip_id, a.stop_seq, a.actual_ts - lead, prefix) if r else None
        errors[lead] = (a.actual_ts - pred) if pred is not None else None
    try:
        table.put_item(Item=item_for(a, errors), ConditionExpression="attribute_not_exists(pk)")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise
    if r is not None:
        r.set(f"{prefix}tripdelay:{a.trip_id}", a.delay_s, ex=3 * 3600)
    if errors[300] is not None:
        hour = datetime.fromtimestamp(a.actual_ts, TZ).hour
        stats_table.update_item(
            Key={"route_id": a.route_id, "hour": hour},
            UpdateExpression="ADD n :one, sum_abs_err_5m :e",
            ExpressionAttributeValues={":one": 1, ":e": Decimal(abs(errors[300]))},
        )
    return True


def handler(event, context=None):
    ddb, r = _clients()
    table = ddb.Table(TABLE_ARRIVALS)
    stats = ddb.Table(TABLE_ROUTE_STATS)
    prefix = os.getenv("REDIS_PREFIX", "")
    written = dupes = bad = 0
    for records in event.get("records", {}).values():
        for rec in records:
            try:
                a = decode_arrival(base64.b64decode(rec["value"]))
            except BadMessage as e:
                log.warning("skipping bad arrival at offset %s: %s", rec.get("offset"), e)
                bad += 1
                continue
            if write_arrival(table, stats, r, a, prefix):
                written += 1
            else:
                dupes += 1
    return {"written": written, "duplicates_rejected": dupes, "bad": bad}
