ALTER TABLE channel_reply_context
    ADD COLUMN chat_id TEXT NOT NULL DEFAULT '';

ALTER TABLE channel_reply_context
    DROP CONSTRAINT channel_reply_context_pkey;

ALTER TABLE channel_reply_context
    ADD PRIMARY KEY (connection_id, external_user_id, chat_id);

CREATE INDEX idx_channel_reply_context_chat
    ON channel_reply_context (connection_id, external_user_id, chat_id);
