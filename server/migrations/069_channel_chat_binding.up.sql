CREATE TABLE channel_chat_binding (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    provider            TEXT         NOT NULL CHECK (provider IN ('feishu')),
    external_chat_id    TEXT         NOT NULL,
    chat_type           TEXT         NOT NULL CHECK (chat_type IN ('group', 'dm')),
    workspace_id        UUID         NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    is_primary          BOOLEAN      NOT NULL DEFAULT TRUE,
    bound_by_user_id    UUID         REFERENCES "user"(id) ON DELETE SET NULL,
    external_chat_name  TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (provider, external_chat_id)
);

CREATE UNIQUE INDEX idx_channel_chat_binding_primary_per_ws
    ON channel_chat_binding (provider, workspace_id)
    WHERE is_primary;

CREATE INDEX idx_channel_chat_binding_workspace
    ON channel_chat_binding (workspace_id, provider);
