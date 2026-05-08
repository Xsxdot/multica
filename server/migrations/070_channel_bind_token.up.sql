CREATE TABLE channel_bind_token (
    token_hash         BYTEA        PRIMARY KEY,
    purpose            TEXT         NOT NULL DEFAULT 'user_identity' CHECK (purpose IN ('user_identity', 'chat_workspace')),
    provider           TEXT         NOT NULL CHECK (provider IN ('feishu')),
    external_user_id   TEXT         NOT NULL,
    external_chat_id   TEXT,
    external_chat_type TEXT,
    external_chat_name TEXT,
    expires_at         TIMESTAMPTZ  NOT NULL,
    consumed_at        TIMESTAMPTZ,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK (
        purpose = 'user_identity'
        OR (
            external_chat_id IS NOT NULL
            AND external_chat_type IS NOT NULL
        )
    )
);

CREATE INDEX idx_channel_bind_token_unconsumed
    ON channel_bind_token (expires_at)
    WHERE consumed_at IS NULL;
