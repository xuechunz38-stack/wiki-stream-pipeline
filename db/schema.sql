-- Raw events: one row per Wikipedia edit that went through Kafka (written by consumer_db.py)
CREATE TABLE IF NOT EXISTS wiki_edits (
    event_id      UUID PRIMARY KEY,              -- Wikimedia meta.id (globally unique)
    rc_id         BIGINT,                        -- recent-changes id inside the wiki
    event_time    TIMESTAMPTZ NOT NULL,          -- when the edit happened
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    wiki          TEXT NOT NULL,                 -- category: enwiki, dewiki, ...
    language      TEXT NOT NULL,                 -- en, de, ...
    edit_type     TEXT NOT NULL,                 -- category: edit | new
    namespace     INTEGER,                       -- 0 = article, 1 = talk, 2 = user ...
    title         TEXT,
    is_bot        BOOLEAN NOT NULL,
    is_minor      BOOLEAN NOT NULL,
    is_anonymous  BOOLEAN NOT NULL,              -- edit made from an IP address (no account)
    old_length    INTEGER,
    new_length    INTEGER,
    bytes_change  INTEGER NOT NULL               -- numeric: new_length - old_length
);
CREATE INDEX IF NOT EXISTS wiki_edits_time_idx ON wiki_edits (event_time DESC);
CREATE INDEX IF NOT EXISTS wiki_edits_wiki_time_idx ON wiki_edits (wiki, event_time DESC);

-- Derived: 1-minute tumbling-window aggregates per wiki (written by processor.py)
CREATE TABLE IF NOT EXISTS edit_stats_minute (
    window_start    TIMESTAMPTZ NOT NULL,
    wiki            TEXT NOT NULL,
    edits           INTEGER NOT NULL,
    bot_edits       INTEGER NOT NULL,
    anon_edits      INTEGER NOT NULL,
    new_pages       INTEGER NOT NULL,
    bytes_added     BIGINT NOT NULL,
    bytes_removed   BIGINT NOT NULL,
    expected_edits  DOUBLE PRECISION,            -- EWMA forecast made *before* this window closed
    zscore          DOUBLE PRECISION,
    is_anomaly      BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, wiki)
);

-- Derived: alerts raised by the stream processor (also published to Kafka topic wikipedia.edit-alerts)
CREATE TABLE IF NOT EXISTS edit_alerts (
    alert_id      BIGSERIAL PRIMARY KEY,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    alert_type    TEXT NOT NULL,                 -- edit_spike | large_addition | large_removal
    wiki          TEXT NOT NULL,
    event_time    TIMESTAMPTZ NOT NULL,
    value         DOUBLE PRECISION NOT NULL,
    expected      DOUBLE PRECISION,
    zscore        DOUBLE PRECISION,
    detail        TEXT
);
CREATE INDEX IF NOT EXISTS edit_alerts_time_idx ON edit_alerts (event_time DESC);
