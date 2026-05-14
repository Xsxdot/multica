ALTER TABLE channel_inbound_event_dedup
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'processed',
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE channel_inbound_event_dedup
    DROP CONSTRAINT IF EXISTS channel_inbound_event_dedup_status_check,
    ADD CONSTRAINT channel_inbound_event_dedup_status_check
        CHECK (status IN ('processing', 'processed', 'failed'));

CREATE INDEX IF NOT EXISTS idx_channel_inbound_event_dedup_retryable
    ON channel_inbound_event_dedup (status, updated_at)
    WHERE status IN ('processing', 'failed');
