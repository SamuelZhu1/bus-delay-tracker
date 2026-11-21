# bus-delay-tracker

My bus app kept telling me the bus was a few minutes away when it had already gone, so I
built a pipeline on TransLink's raw real-time feed to see when buses actually reach each
stop and how far off the predictions run.

## How it works

```
TransLink GTFS-realtime (positions + trip updates, ~30 s refresh)
   -> poller (Python, every 15 s, drops repeated reports)
   -> Kafka  vehicle.positions (key = vehicle_id)   trip.predictions (key = trip_id)
        -> arrival detector (consumer group, per-bus state in Redis) -> Kafka stop.arrivals
        -> prediction recorder (TransLink predictions per trip/stop in Redis)
   -> AWS Lambda writer (self-managed Kafka trigger) -> DynamoDB arrivals, route_stats
   -> AWS Lambda API (API Gateway): next bus at a stop, route accuracy by hour
```

- **Arrival detection.** Buses report about every 30 s, so one is rarely caught at a stop.
  For each new fix the detector checks the straight segment from the previous fix and finds
  the upcoming stops it passed within 30 m of, timing each by where along the segment it
  passed. Stops skipped without a close pass get an interpolated time.
- **Delivery.** At-least-once: the detector commits offsets only after its arrivals are
  acknowledged by Kafka. Every arrival has a deterministic id (`trip:stop_seq:service_date`)
  and the writer stores it with a DynamoDB conditional put, so a re-sent arrival is rejected.
- **Prediction error.** For each arrival the writer looks up what TransLink predicted 5 and
  10 minutes earlier and stores the difference.

## Run it

```
cp .env.example .env          # add a free key from developer.translink.ca
docker compose up -d          # Kafka, Redis, DynamoDB Local
pip install -e ".[dev]"
python -m busdelay.poller --record data/day.jsonl.gz
python -m busdelay.detector_service
python -m busdelay.predictions
python -m busdelay.local_lambda    # or deploy template.yaml with AWS SAM
```

No key? `python -m busdelay.sim data/sim` writes a simulated day in the same format.

## Tests

- `pytest` runs the unit tests (detector cases, feed parsing, message validation, the writer
  and API against moto and fakeredis).
- `scripts/crash_test.py` replays a recording through real Kafka, Redis and DynamoDB,
  kills the detector with `kill -9` N times, and compares DynamoDB with an offline ground
  truth computed from the same recording (missing, extra, duplicates). CI runs a short version.

See `docs/bugs.md` for bugs found while building.
