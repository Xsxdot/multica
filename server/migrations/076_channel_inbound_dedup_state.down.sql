DROP INDEX IF EXISTS idx_channel_inbound_event_dedup_retryable;

ALTER TABLE channel_inbound_event_dedup
    DROP CONSTRAINT IF EXISTS channel_inbound_event_dedup_status_check,
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS status;
