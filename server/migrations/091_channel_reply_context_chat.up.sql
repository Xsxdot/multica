ALTER TABLE channel_reply_context
    ADD COLUMN chat_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN thread_id TEXT NOT NULL DEFAULT '';

CREATE INDEX idx_channel_reply_context_chat
    ON channel_reply_context (connection_id, external_user_id, chat_id, thread_id);
