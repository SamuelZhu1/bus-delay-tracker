import base64
import json

import boto3
import fakeredis
import pytest
from moto import mock_aws

from busdelay.gtfs_static import TripStop, to_json
from busdelay.lambdas import api, writer
from busdelay.messages import Arrival, Prediction, encode
from busdelay.predictions import prediction_made_before, record
from busdelay.tables import create_tables


@pytest.fixture
def env():
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name="ca-central-1")
        create_tables(ddb)
        r = fakeredis.FakeRedis()
        writer._ddb, writer._redis = ddb, r
        api._ddb, api._redis = ddb, r
        yield ddb, r
        writer._ddb = writer._redis = api._ddb = api._redis = None


def event(arrivals):
    return {"eventSource": "SelfManagedKafka", "records": {"stop.arrivals-0": [
        {"offset": i, "value": base64.b64encode(encode(a)).decode()} for i, a in enumerate(arrivals)]}}


A = Arrival("T1", "99", "S5", 5, "2026-11-17", 1_000_000, 1_000_120, "geofence")


def test_redelivered_arrival_is_stored_once(env):
    ddb, _ = env
    assert writer.handler(event([A]))["written"] == 1
    # Kafka redelivers the same batch after a consumer crash.
    res = writer.handler(event([A, A]))
    assert res == {"written": 0, "duplicates_rejected": 2, "bad": 0}
    assert ddb.Table("arrivals").scan()["Count"] == 1


def test_prediction_error_uses_prediction_made_5_minutes_before(env):
    ddb, r = env
    # TransLink predicted 1_000_000 at 6 min before, then 1_000_060 at 4 min before.
    record(r, Prediction("T1", "99", "S5", 5, 1_000_000, A.actual_ts - 360))
    record(r, Prediction("T1", "99", "S5", 5, 1_000_060, A.actual_ts - 240))
    assert prediction_made_before(r, "T1", 5, A.actual_ts - 300) == 1_000_000
    writer.handler(event([A]))
    item = ddb.Table("arrivals").scan()["Items"][0]
    assert int(item["pred_err_5m_s"]) == 120
    assert int(item["delay_s"]) == 120
    stats = ddb.Table("route_stats").scan()["Items"][0]
    assert int(stats["n"]) == 1 and int(stats["sum_abs_err_5m"]) == 120


def test_duplicate_does_not_double_count_route_stats(env):
    ddb, r = env
    record(r, Prediction("T1", "99", "S5", 5, 1_000_000, A.actual_ts - 400))
    writer.handler(event([A]))
    writer.handler(event([A]))
    assert int(ddb.Table("route_stats").scan()["Items"][0]["n"]) == 1


def test_bad_record_is_skipped(env):
    ev = {"records": {"stop.arrivals-0": [{"offset": 0, "value": base64.b64encode(b"{}").decode()}]}}
    assert writer.handler(ev) == {"written": 0, "duplicates_rejected": 0, "bad": 1}


def test_next_bus_api_shows_translink_and_our_eta(env, monkeypatch):
    _, r = env
    stops = [TripStop(5, "S5", 49.26, -123.1, 8 * 3600)]
    r.set("stops:T1", to_json(stops))
    r.set("tripdelay:T1", 180)
    record(r, Prediction("T1", "99", "S5", 5, 2_000_000_000, 1_999_999_000))
    monkeypatch.setattr(api, "_now", lambda: 1_999_999_500)
    resp = api.handler({"rawPath": "/stops/S5/next", "pathParameters": {"stop_id": "S5"}, "queryStringParameters": {"route": "99"}})
    body = json.loads(resp["body"])
    assert resp["statusCode"] == 200
    bus = body["buses"][0]
    assert bus["translink_eta_ts"] == 2_000_000_000
    assert bus["our_eta_ts"] is not None


def test_accuracy_api(env):
    ddb, r = env
    record(r, Prediction("T1", "99", "S5", 5, 1_000_000, A.actual_ts - 400))
    writer.handler(event([A]))
    body = json.loads(api.handler({"rawPath": "/routes/99/accuracy", "pathParameters": {"route_id": "99"}})["body"])
    assert body["by_hour"][0]["mean_abs_err_5m_s"] == 120.0


def test_unknown_path_404(env):
    assert api.handler({"rawPath": "/nope"})["statusCode"] == 404
