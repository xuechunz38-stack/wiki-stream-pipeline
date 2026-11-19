"""Producer: Wikimedia EventStreams (live) -> Kafka topic `wikipedia.edits`.

Reads the public Server-Sent-Events feed of every change on every Wikimedia wiki,
keeps edits and page creations on Wikipedia language editions, reshapes each one
into a flat event and publishes it with the wiki name (e.g. "enwiki") as the key.
Reconnects automatically and resumes from the last seen event id.
"""
import ipaddress
import json
import os
import signal
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from common import KAFKA_BOOTSTRAP, TOPIC_EDITS, setup_logging

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia asks API clients to send a descriptive User-Agent.
USER_AGENT = os.getenv("USER_AGENT", "ids706-kafka-student-pipeline/1.0 (course project)")
KEEP_TYPES = {"edit", "new"}

log = setup_logging("producer")
running = True


def _stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def is_anonymous(user: str) -> bool:
    """Logged-out editor: shown as an IP address, or (since 2025) as a temporary
    account whose name starts with '~', e.g. '~2026-12345-67'."""
    if user.startswith("~"):
        return True
    try:
        ipaddress.ip_address(user)
        return True
    except ValueError:
        return False


def to_event(raw: dict):
    """Return the flat event we publish, or None if the change is filtered out."""
    server = raw.get("server_name", "")
    if raw.get("type") not in KEEP_TYPES or not server.endswith(".wikipedia.org"):
        return None
    length = raw.get("length") or {}
    old_len, new_len = length.get("old") or 0, length.get("new") or 0
    return {
        "event_id": raw["meta"]["id"],
        "rc_id": raw.get("id"),
        "event_time": datetime.fromtimestamp(raw["timestamp"], tz=timezone.utc).isoformat(),
        "wiki": raw["wiki"],
        "language": server.split(".")[0],
        "edit_type": raw["type"],
        "namespace": raw.get("namespace"),
        "title": raw.get("title"),
        "is_bot": bool(raw.get("bot")),
        "is_minor": bool(raw.get("minor")),
        "is_anonymous": is_anonymous(raw.get("user", "")),
        "old_length": old_len,
        "new_length": new_len,
        "bytes_change": new_len - old_len,
    }


def sse_events(last_event_id=None):
    """Yield (event_id, data_dict) from the SSE stream."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as resp:
        resp.raise_for_status()
        event_id, data_lines = None, []
        for line in resp.iter_lines(decode_unicode=True):
            if not running:
                return
            if line is None:
                continue
            if line == "":  # blank line = end of one SSE event
                if data_lines:
                    try:
                        yield event_id, json.loads("\n".join(data_lines))
                    except json.JSONDecodeError:
                        pass
                event_id, data_lines = None, []
            elif line.startswith("id:"):
                event_id = line[3:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].strip())


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "wiki-edits-producer",
        "enable.idempotence": True,     # no duplicates on producer retries
        "acks": "all",
        "linger.ms": 50,                # small batching window
        "compression.type": "lz4",
    })
    stats = {"seen": 0, "sent": 0, "failed": 0}

    def on_delivery(err, msg):
        if err:
            stats["failed"] += 1
            log.error("delivery failed: %s", err)

    last_id, last_report, backoff = None, time.time(), 1
    log.info("streaming %s -> kafka %s topic %s", STREAM_URL, KAFKA_BOOTSTRAP, TOPIC_EDITS)
    while running:
        try:
            for sse_id, raw in sse_events(last_id):
                last_id = sse_id or last_id
                stats["seen"] += 1
                event = to_event(raw)
                if event is None:
                    continue
                producer.produce(
                    TOPIC_EDITS,
                    key=event["wiki"],
                    value=json.dumps(event).encode(),
                    on_delivery=on_delivery,
                )
                stats["sent"] += 1
                producer.poll(0)
                if time.time() - last_report >= 30:
                    log.info("seen=%(seen)s sent=%(sent)s failed=%(failed)s", stats)
                    last_report = time.time()
            backoff = 1
        except (requests.RequestException, ConnectionError) as exc:
            log.warning("stream disconnected (%s); reconnecting in %ss", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except BufferError:
            producer.poll(1)
    log.info("flushing producer ...")
    producer.flush(10)


if __name__ == "__main__":
    main()
