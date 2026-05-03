-- Association table between an external IM chat (e.g. a Feishu group chat_id)
-- and a Multica workspace. PRD F4: MVP is 1:1, but the schema is many-to-many
-- ready (no field added on workspace). is_primary marks the canonical chat
-- per workspace once multi-binding lands in P1; under MVP every row is
-- primary by definition.
--
-- Uniqueness rules:
--   - (provider, external_chat_id) is globally unique -> one Feishu group can
--     only belong to one workspace at a time (PRD AC4.2).
--   - At most one is_primary=TRUE row per (provider, workspace_id) -- enforced
--     via partial unique index below; under MVP this collapses to "1:1".
CREATE TABLE channel_chat_binding (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    provider            TEXT         NOT NULL CHECK (provider IN ('feishu')),
    external_chat_id    TEXT         NOT NULL,
    chat_type           TEXT         NOT NULL CHECK (chat_type IN ('group', 'dm')),
    workspace_id        UUID         NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    is_primary          BOOLEAN      NOT NULL DEFAULT TRUE,
    bound_by_user_id    UUID         REFERENCES "user"(id) ON DELETE SET NULL,
    external_chat_name  TEXT,         -- snapshot for diagnostics
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (provider, external_chat_id)   -- AC4.2: a chat can't be claimed twice
);

-- Enforce "at most one primary chat per (provider, workspace)" without
-- blocking future many-to-many: secondary chats will have is_primary=FALSE
-- and may coexist freely.
CREATE UNIQUE INDEX idx_channel_chat_binding_primary_per_ws
    ON channel_chat_binding (provider, workspace_id)
    WHERE is_primary;

-- Hot path: outbound subscriber resolves "which chat to send to?" given
-- (provider, workspace_id). Covered by the partial unique index above for
-- the primary case; for non-primary lookups (P1) add a regular index now to
-- avoid an extra migration later.
CREATE INDEX idx_channel_chat_binding_workspace
    ON channel_chat_binding (workspace_id, provider);
