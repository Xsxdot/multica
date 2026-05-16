DROP INDEX IF EXISTS idx_channel_reply_context_chat;

ALTER TABLE channel_reply_context
    DROP CONSTRAINT channel_reply_context_pkey;

ALTER TABLE channel_reply_context
    ADD PRIMARY KEY (connection_id, external_user_id);

ALTER TABLE channel_reply_context
    DROP COLUMN IF EXISTS chat_id;
