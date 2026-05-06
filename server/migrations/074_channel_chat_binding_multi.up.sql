-- Migration 074: Add index for workspace-level channel binding lookups
-- Supports multi-binding (one workspace -> many chats) while keeping
-- the UNIQUE (provider, external_chat_id) constraint (one chat -> one workspace).

CREATE INDEX IF NOT EXISTS idx_channel_chat_binding_ws_provider
    ON channel_chat_binding (workspace_id, provider);
