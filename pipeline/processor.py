"""Consumer 2 (group `wiki-edits-analytics`): stream processing.

1. 1-minute tumbling windows per wiki (event time, 20 s allowed lateness)
   -> upserted into `edit_stats_minute` (edits, bot/anon edits, new pages, bytes added/removed).
2. Sequential anomaly model per wiki: an exponentially weighted moving average (EWMA)
   of edits/minute forecasts the next window. When a window closes we compare the
   actual count with the forecast made *before* seeing it:
       z = (actual - expected) / max(std, sqrt(expected), 1)
   z >= SPIKE_Z (after a warm-up) -> `edit_spike` alert.
3. Per-event rule: a single edit that adds or removes >= LARGE_EDIT_BYTES bytes
   -> `large_addition` / `large_removal` alert.

Alerts go to the Kafka topic `wikipedia.edit-alerts` *and* the `edit_alerts` table.
On start the EWMA state is rebuilt from recent rows in `edit_stats_minute`.
"""
import json
import math
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, KafkaError, Producer
from psycopg2.extras import execute_values

from common import (KAFKA_BOOTSTRAP, TOPIC_ALERTS, TOPIC_EDITS, connect_db, ensure_schema,
                    setup_logging)

GROUP_ID = "wiki-edits-analytics"
WINDOW = timedelta(minutes=1)
LATENESS = timedelta(seconds=20)
ALPHA = float(os.getenv("EWMA_ALPHA", "0.2"))
WARMUP_WINDOWS = int(os.getenv("WARMUP_WINDOWS", "10"))
SPIKE_Z = float(os.getenv("SPIKE_Z", "3.5"))
MIN_SPIKE_EDITS = int(os.getenv("MIN_SPIKE_EDITS", "15"))     # ignore "spikes" on tiny wikis
LARGE_EDIT_BYTES = int(os.getenv("LARGE_EDIT_BYTES", "20000"))
STATS_RETENTION_HOURS = 48
ALL = "_all"  # pseudo-wiki holding the global totals

log = setup_logging("processor")
running = True
signal.signal(signal.SIGINT, lambda *_: globals().update(running=False))
signal.signal(signal.SIGTERM, lambda *_: globals().update(running=False))


class Ewma:
    """Online EWMA mean/variance of a sequence (edits per minute)."""

    def __init__(self):
        self.mean, self.var, self.n = 0.0, 0.0, 0

    def forecast(self):
        return self.mean if self.n else None

    def score(self, x):
        if self.n == 0:
            return None
        std = math.sqrt(self.var)
        return (x - self.mean) / max(std, math.sqrt(max(self.mean, 0.0)), 1.0)

    def update(self, x):
        if self.n == 0:
            self.mean, self.var = float(x), 0.0
        else:
            diff = x - self.mean
            incr = ALPHA * diff
            self.mean += incr
            self.var = (1 - ALPHA) * (self.var + diff * incr)
        self.n += 1


def new_window():
    return {"edits": 0, "bot_edits": 0, "anon_edits": 0, "new_pages": 0,
            "bytes_added": 0, "bytes_removed": 0}


def floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def main():
    conn = connect_db(log)
    ensure_schema(conn)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "client.id": "wiki-alerts-producer"})
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_EDITS])

    models = defaultdict(Ewma)
    # warm start: replay the last hour of closed windows into the EWMA models
    with conn.cursor() as cur:
        cur.execute("""SELECT wiki, edits FROM edit_stats_minute
                       WHERE window_start > now() - interval '60 minutes' ORDER BY window_start""")
        for wiki, edits in cur.fetchall():
            models[wiki].update(edits)
    log.info("warm start: %s wiki models restored", len(models))

    windows = defaultdict(new_window)       # (window_start, wiki) -> aggregates
    closed_until = None                     # windows starting before this are already flushed
    watermark = datetime.min.replace(tzinfo=timezone.utc)
    pending_alerts, last_cleanup = [], 0.0

    def alert(alert_type, wiki, event_time, value, expected=None, z=None, detail=""):
        a = {"alert_type": alert_type, "wiki": wiki, "event_time": event_time.isoformat(),
             "value": value, "expected": expected, "zscore": z, "detail": detail}
        producer.produce(TOPIC_ALERTS, key=wiki, value=json.dumps(a).encode())
        pending_alerts.append((alert_type, wiki, event_time, value, expected, z, detail))
        log.info("ALERT %s %s value=%s expected=%s z=%s %s", alert_type, wiki, value,
                 None if expected is None else round(expected, 1),
                 None if z is None else round(z, 2), detail)

    def flush_closed():
        nonlocal closed_until
        # a window is final once the watermark (max event time seen) passes its end + lateness
        ready = sorted({ws for (ws, _) in windows if ws + WINDOW <= watermark - LATENESS})
        if not ready:
            return
        rows = []
        for ws in ready:
            wikis = [w for (s, w) in list(windows) if s == ws]
            for wiki in wikis:
                agg = windows.pop((ws, wiki))
                m = models[wiki]
                expected, z = m.forecast(), m.score(agg["edits"])
                anomaly = (m.n >= WARMUP_WINDOWS and z is not None and z >= SPIKE_Z
                           and agg["edits"] >= MIN_SPIKE_EDITS)
                if anomaly:
                    alert("edit_spike", wiki, ws, agg["edits"], expected, z,
                          f"{agg['edits']} edits in the minute starting {ws:%H:%M} UTC "
                          f"(forecast {expected:.1f})")
                m.update(agg["edits"])
                rows.append((ws, wiki, agg["edits"], agg["bot_edits"], agg["anon_edits"],
                             agg["new_pages"], agg["bytes_added"], agg["bytes_removed"],
                             expected, z, anomaly))
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO edit_stats_minute (window_start, wiki, edits, bot_edits, anon_edits,
                    new_pages, bytes_added, bytes_removed, expected_edits, zscore, is_anomaly)
                VALUES %s
                ON CONFLICT (window_start, wiki) DO UPDATE SET
                    edits = EXCLUDED.edits, bot_edits = EXCLUDED.bot_edits,
                    anon_edits = EXCLUDED.anon_edits, new_pages = EXCLUDED.new_pages,
                    bytes_added = EXCLUDED.bytes_added, bytes_removed = EXCLUDED.bytes_removed,
                    expected_edits = EXCLUDED.expected_edits, zscore = EXCLUDED.zscore,
                    is_anomaly = EXCLUDED.is_anomaly, updated_at = now()""", rows)
        conn.commit()
        closed_until = ready[-1] + WINDOW
        log.info("closed %s window(s) up to %s: %s rows", len(ready), closed_until, len(rows))

    log.info("consuming %s as group %s", TOPIC_EDITS, GROUP_ID)
    while running:
        for m in consumer.consume(num_messages=500, timeout=1.0):
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", m.error())
                continue
            e = json.loads(m.value())
            ts = datetime.fromisoformat(e["event_time"])
            ws = floor_minute(ts)
            if closed_until and ws < closed_until:
                continue  # too late, window already emitted
            watermark = max(watermark, ts)
            change = e["bytes_change"]
            for wiki in (e["wiki"], ALL):
                agg = windows[(ws, wiki)]
                agg["edits"] += 1
                agg["bot_edits"] += e["is_bot"]
                agg["anon_edits"] += e["is_anonymous"]
                agg["new_pages"] += e["edit_type"] == "new"
                agg["bytes_added"] += max(change, 0)
                agg["bytes_removed"] += max(-change, 0)
            if abs(change) >= LARGE_EDIT_BYTES:
                kind = "large_addition" if change > 0 else "large_removal"
                alert(kind, e["wiki"], ts, change,
                      detail=f"{'+' if change > 0 else ''}{change:,} bytes on \"{e['title']}\""
                             f"{' (bot)' if e['is_bot'] else ''}")

        flush_closed()
        if pending_alerts:
            with conn.cursor() as cur:
                execute_values(cur, """INSERT INTO edit_alerts
                    (alert_type, wiki, event_time, value, expected, zscore, detail) VALUES %s""",
                               pending_alerts)
            conn.commit()
            pending_alerts.clear()
        producer.poll(0)

        if time.time() - last_cleanup > 600:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM edit_stats_minute WHERE window_start < now() - %s * interval '1 hour'",
                            (STATS_RETENTION_HOURS,))
                cur.execute("DELETE FROM edit_alerts WHERE event_time < now() - interval '7 days'")
            conn.commit()
            last_cleanup = time.time()

    producer.flush(5)
    consumer.close()
    conn.close()


if __name__ == "__main__":
    main()
