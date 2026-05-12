-- Per-bound chat settings: listen scope and optional fixed agent for channel intent.

ALTER TABLE channel_chat_binding
    ADD COLUMN IF NOT EXISTS listen_mode TEXT NOT NULL DEFAULT 'mentions'
        CONSTRAINT channel_chat_binding_listen_mode_check CHECK (listen_mode IN ('mentions', 'all'));

ALTER TABLE channel_chat_binding
    ADD COLUMN IF NOT EXISTS agent_id UUID REFERENCES agent(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_channel_chat_binding_agent
    ON channel_chat_binding (agent_id)
    WHERE agent_id IS NOT NULL;
