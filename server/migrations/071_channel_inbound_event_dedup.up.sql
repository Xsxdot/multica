CREATE TABLE channel_inbound_event_dedup (
    provider       TEXT         NOT NULL CHECK (provider IN ('feishu')),
    event_id       TEXT         NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, event_id)
);

CREATE INDEX idx_channel_inbound_event_dedup_processed_at
    ON channel_inbound_event_dedup (processed_at);
