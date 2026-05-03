-- Idempotency table for inbound platform events. The Feishu SDK replays
-- buffered events on reconnect (PRD AC2.1); we de-dup by (provider,
-- event_id) before letting the inbound pipeline process them. Rows live
-- ~7 days; an application-side worker prunes older rows hourly (DESIGN section 6
-- risk 2 / Q3 above). pg_cron is intentionally NOT used -- the project does
-- not enable it.
CREATE TABLE channel_inbound_event_dedup (
    provider       TEXT         NOT NULL CHECK (provider IN ('feishu')),
    event_id       TEXT         NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, event_id)
);

-- GC worker hot path: DELETE WHERE processed_at < now() - interval '7 days'.
-- A composite index (processed_at) gives the worker a cheap range scan;
-- BRIN would be lighter but requires sustained sequential inserts which we
-- can't guarantee under retries, so plain BTREE is the safe default.
CREATE INDEX idx_channel_inbound_event_dedup_processed_at
    ON channel_inbound_event_dedup (processed_at);
