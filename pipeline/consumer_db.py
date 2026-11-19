"""Consumer 1 (group `wiki-edits-db-writer`): Kafka -> Postgres table `wiki_edits`.

Reads batches, inserts them with ON CONFLICT DO NOTHING (idempotent on event_id) and
commits Kafka offsets only after the database transaction commits, so a crash can
replay a batch but never lose or duplicate a row.
"""
import json
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError
from psycopg2.extras import execute_values

from common import KAFKA_BOOTSTRAP, TOPIC_EDITS, connect_db, ensure_schema, setup_logging

GROUP_ID = "wiki-edits-db-writer"
BATCH_SIZE = 500
RAW_RETENTION_HOURS = float(os.getenv("RAW_RETENTION_HOURS", "6"))  # keep the cloud DB small

log = setup_logging("consumer_db")
running = True
signal.signal(signal.SIGINT, lambda *_: globals().update(running=False))
signal.signal(signal.SIGTERM, lambda *_: globals().update(running=False))

COLUMNS = ["event_id", "rc_id", "event_time", "wiki", "language", "edit_type", "namespace", "title",
           "is_bot", "is_minor", "is_anonymous", "old_length", "new_length", "bytes_change"]
INSERT_SQL = (f"INSERT INTO wiki_edits ({', '.join(COLUMNS)}) VALUES %s "
              "ON CONFLICT (event_id) DO NOTHING")


def main():
    conn = connect_db(log)
    ensure_schema(conn)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_EDITS])
    log.info("consuming %s as group %s", TOPIC_EDITS, GROUP_ID)

    total, last_report, last_cleanup = 0, time.time(), 0.0
    while running:
        msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
        rows = []
        for m in msgs:
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", m.error())
                continue
            e = json.loads(m.value())
            rows.append(tuple(e.get(c) for c in COLUMNS))
        if rows:
            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, rows, page_size=BATCH_SIZE)
            conn.commit()
            consumer.commit(asynchronous=False)  # offsets after the DB commit
            total += len(rows)

        now = time.time()
        if now - last_cleanup > 300:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM wiki_edits WHERE event_time < now() - %s * interval '1 hour'",
                            (RAW_RETENTION_HOURS,))
                deleted = cur.rowcount
            conn.commit()
            last_cleanup = now
            if deleted:
                log.info("retention: deleted %s rows older than %sh", deleted, RAW_RETENTION_HOURS)
        if now - last_report >= 30:
            log.info("inserted %s rows so far", total)
            last_report = now

    consumer.close()
    conn.close()


if __name__ == "__main__":
    main()
