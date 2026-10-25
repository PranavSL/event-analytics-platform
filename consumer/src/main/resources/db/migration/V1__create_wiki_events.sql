CREATE TABLE wiki_events (
    event_id        UUID        PRIMARY KEY,
    event_time      TIMESTAMPTZ NOT NULL,
    wiki            TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    namespace       INT,
    title           TEXT,
    username        TEXT,
    is_bot          BOOLEAN     NOT NULL,
    is_minor        BOOLEAN,
    length_old      INT,
    length_new      INT,
    kafka_partition INT         NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_events_time      ON wiki_events (event_time);
CREATE INDEX idx_events_wiki_time ON wiki_events (wiki, event_time);