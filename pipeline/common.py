"""Shared configuration and helpers for the producer and consumers."""
import logging
import os
import pathlib
import time

import psycopg2

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_EDITS = os.getenv("TOPIC_EDITS", "wikipedia.edits")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "wikipedia.edit-alerts")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://wiki:wiki@localhost:5432/wikistream")

SCHEMA_FILE = pathlib.Path(__file__).resolve().parent.parent / "db" / "schema.sql"
if not SCHEMA_FILE.exists():  # inside the container the schema is copied next to the code
    SCHEMA_FILE = pathlib.Path(__file__).resolve().parent / "schema.sql"


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    return logging.getLogger(name)


def connect_db(log: logging.Logger, retries: int = 30):
    """Connect to Postgres, retrying while the database container starts."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("database not ready (%s/%s): %s", attempt, retries, str(exc).strip())
            time.sleep(2)
    raise SystemExit("could not connect to the database")


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        # both consumers start at the same time; serialize the CREATE statements
        cur.execute("SELECT pg_advisory_xact_lock(706013)")
        cur.execute(SCHEMA_FILE.read_text())
    conn.commit()
