ALTER TABLE channel_bind_token
    ADD COLUMN IF NOT EXISTS purpose TEXT NOT NULL DEFAULT 'user_identity',
    ADD COLUMN IF NOT EXISTS external_chat_id TEXT,
    ADD COLUMN IF NOT EXISTS external_chat_type TEXT,
    ADD COLUMN IF NOT EXISTS external_chat_name TEXT;

ALTER TABLE channel_bind_token
    DROP CONSTRAINT IF EXISTS channel_bind_token_purpose_check,
    ADD CONSTRAINT channel_bind_token_purpose_check
        CHECK (purpose IN ('user_identity', 'chat_workspace'));

ALTER TABLE channel_bind_token
    DROP CONSTRAINT IF EXISTS channel_bind_token_chat_workspace_check,
    ADD CONSTRAINT channel_bind_token_chat_workspace_check
        CHECK (
            purpose = 'user_identity'
            OR (
                external_chat_id IS NOT NULL
                AND external_chat_type IS NOT NULL
            )
        );
