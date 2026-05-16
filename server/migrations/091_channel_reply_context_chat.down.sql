DROP INDEX IF EXISTS idx_channel_reply_context_chat;

ALTER TABLE channel_reply_context
    DROP COLUMN IF EXISTS chat_id,
    DROP COLUMN IF EXISTS thread_id;
