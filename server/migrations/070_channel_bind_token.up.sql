CREATE TABLE channel_bind_token (
    token_hash         BYTEA        PRIMARY KEY,
    provider           TEXT         NOT NULL CHECK (provider IN ('feishu')),
    external_user_id   TEXT         NOT NULL,
    expires_at         TIMESTAMPTZ  NOT NULL,
    consumed_at        TIMESTAMPTZ,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_channel_bind_token_unconsumed
    ON channel_bind_token (expires_at)
    WHERE consumed_at IS NULL;
