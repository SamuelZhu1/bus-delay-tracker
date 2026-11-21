# Bugs found while building

1. **Bus first seen mid-trip got a fake "interpolated" arrival at the stop behind it.**
   The detector started a new trip at the nearest stop, even when the bus had already
   passed it, so the next segment marked that stop as skipped. Fix: if the bus is closer to
   the following stop than the nearest stop is, start from the following stop
   (`test_bus_first_seen_mid_trip_does_not_invent_earlier_arrivals`).
2. **Replay crashed with `BufferError: Local: Queue full`.** A fast replay (or a big trip
   updates poll) queues more than librdkafka's 100k local buffer. Fix: wait on `poll()` and
   retry instead of failing.
3. **Restarted detector exited before it got its partitions back.** After a `kill -9`, the
   dead consumer kept its partitions until the session timeout (45 s default), and the idle
   timer had started at launch. Fix: 10 s session timeout, idle timer starts at the first message.

Open: detector state is written to Redis per message, before offsets are committed. A crash
after the Redis write but before the arrivals are flushed could drop an arrival on restart
(the replayed fix looks stale). The crash test is there to measure this; planned fix is to
buffer state writes per batch and write them after the producer flush.
