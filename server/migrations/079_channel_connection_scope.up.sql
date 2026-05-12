CREATE TABLE channel_connection (
    id            TEXT        PRIMARY KEY,
    provider      TEXT        NOT NULL,
    display_name  TEXT        NOT NULL,
    enabled       BOOLEAN     NOT NULL DEFAULT TRUE,
    is_default    BOOLEAN     NOT NULL DEFAULT FALSE,
    config        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status        TEXT        NOT NULL DEFAULT 'configured'
                                CHECK (status IN ('configured', 'connected', 'disabled', 'error')),
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_channel_connection_provider_default
    ON channel_connection(provider)
    WHERE is_default;

INSERT INTO channel_connection (id, provider, display_name, enabled, is_default)
VALUES ('feishu', 'feishu', 'Feishu', TRUE, TRUE)
ON CONFLICT (id) DO NOTHING;

ALTER TABLE channel_user_binding
    DROP CONSTRAINT IF EXISTS channel_user_binding_provider_check,
    ADD COLUMN connection_id TEXT;
UPDATE channel_user_binding SET connection_id = provider WHERE connection_id IS NULL;
ALTER TABLE channel_user_binding
    ALTER COLUMN connection_id SET NOT NULL,
    ADD CONSTRAINT channel_user_binding_connection_fk
        FOREIGN KEY (connection_id) REFERENCES channel_connection(id) ON DELETE CASCADE,
    DROP CONSTRAINT IF EXISTS channel_user_binding_provider_external_user_id_key,
    DROP CONSTRAINT IF EXISTS channel_user_binding_provider_user_id_key,
    ADD CONSTRAINT channel_user_binding_connection_external_user_id_key
        UNIQUE (connection_id, external_user_id),
    ADD CONSTRAINT channel_user_binding_connection_user_id_key
        UNIQUE (connection_id, user_id);

ALTER TABLE channel_chat_binding
    DROP CONSTRAINT IF EXISTS channel_chat_binding_provider_check,
    ADD COLUMN connection_id TEXT;
UPDATE channel_chat_binding SET connection_id = provider WHERE connection_id IS NULL;
DROP INDEX IF EXISTS idx_channel_chat_binding_primary_per_ws;
ALTER TABLE channel_chat_binding
    ALTER COLUMN connection_id SET NOT NULL,
    ADD CONSTRAINT channel_chat_binding_connection_fk
        FOREIGN KEY (connection_id) REFERENCES channel_connection(id) ON DELETE CASCADE,
    DROP CONSTRAINT IF EXISTS channel_chat_binding_provider_external_chat_id_key,
    ADD CONSTRAINT channel_chat_binding_connection_external_chat_id_key
        UNIQUE (connection_id, external_chat_id);
CREATE UNIQUE INDEX idx_channel_chat_binding_primary_per_connection_ws
    ON channel_chat_binding (connection_id, workspace_id)
    WHERE is_primary;
CREATE INDEX idx_channel_chat_binding_workspace_connection
    ON channel_chat_binding (workspace_id, connection_id);

ALTER TABLE channel_bind_token
    DROP CONSTRAINT IF EXISTS channel_bind_token_provider_check,
    ADD COLUMN connection_id TEXT;
UPDATE channel_bind_token SET connection_id = provider WHERE connection_id IS NULL;
ALTER TABLE channel_bind_token
    ALTER COLUMN connection_id SET NOT NULL,
    ADD CONSTRAINT channel_bind_token_connection_fk
        FOREIGN KEY (connection_id) REFERENCES channel_connection(id) ON DELETE CASCADE;
CREATE INDEX idx_channel_bind_token_connection
    ON channel_bind_token (connection_id, external_user_id);

ALTER TABLE channel_inbound_event_dedup
    DROP CONSTRAINT IF EXISTS channel_inbound_event_dedup_provider_check,
    ADD COLUMN connection_id TEXT;
UPDATE channel_inbound_event_dedup SET connection_id = provider WHERE connection_id IS NULL;
ALTER TABLE channel_inbound_event_dedup
    ALTER COLUMN connection_id SET NOT NULL,
    ADD CONSTRAINT channel_inbound_event_dedup_connection_fk
        FOREIGN KEY (connection_id) REFERENCES channel_connection(id) ON DELETE CASCADE,
    DROP CONSTRAINT IF EXISTS channel_inbound_event_dedup_pkey,
    ADD PRIMARY KEY (connection_id, event_id);

ALTER TABLE channel_outbound_failure
    DROP CONSTRAINT IF EXISTS channel_outbound_failure_provider_check,
    ADD COLUMN connection_id TEXT;
UPDATE channel_outbound_failure SET connection_id = provider WHERE connection_id IS NULL;
ALTER TABLE channel_outbound_failure
    ALTER COLUMN connection_id SET NOT NULL,
    ADD CONSTRAINT channel_outbound_failure_connection_fk
        FOREIGN KEY (connection_id) REFERENCES channel_connection(id) ON DELETE CASCADE;
CREATE INDEX idx_channel_outbound_failure_connection
    ON channel_outbound_failure (connection_id, next_retry_at);

ALTER TABLE channel_outbound_notification
    DROP CONSTRAINT IF EXISTS channel_outbound_notification_provider_check,
    ADD COLUMN connection_id TEXT;
UPDATE channel_outbound_notification SET connection_id = provider WHERE connection_id IS NULL;
ALTER TABLE channel_outbound_notification
    ALTER COLUMN connection_id SET NOT NULL,
    ADD CONSTRAINT channel_outbound_notification_connection_fk
        FOREIGN KEY (connection_id) REFERENCES channel_connection(id) ON DELETE CASCADE;
CREATE INDEX idx_channel_outbound_notification_connection
    ON channel_outbound_notification (connection_id, aggregation_due_at, next_attempt_at);

ALTER TABLE channel_conversation
    DROP CONSTRAINT IF EXISTS channel_conversation_provider_check,
    ADD COLUMN connection_id TEXT;
UPDATE channel_conversation SET connection_id = provider WHERE connection_id IS NULL;
ALTER TABLE channel_conversation
    ALTER COLUMN connection_id SET NOT NULL,
    ADD CONSTRAINT channel_conversation_connection_fk
        FOREIGN KEY (connection_id) REFERENCES channel_connection(id) ON DELETE CASCADE,
    DROP CONSTRAINT IF EXISTS channel_conversation_pkey,
    ADD PRIMARY KEY (connection_id, conversation_key);

ALTER TABLE channel_inbound_event
    DROP CONSTRAINT IF EXISTS channel_inbound_event_provider_check,
    ADD COLUMN connection_id TEXT;
UPDATE channel_inbound_event SET connection_id = provider WHERE connection_id IS NULL;
ALTER TABLE channel_inbound_event
    ALTER COLUMN connection_id SET NOT NULL,
    ADD CONSTRAINT channel_inbound_event_connection_fk
        FOREIGN KEY (connection_id) REFERENCES channel_connection(id) ON DELETE CASCADE,
    DROP CONSTRAINT IF EXISTS channel_inbound_event_provider_event_id_key,
    ADD CONSTRAINT channel_inbound_event_connection_event_id_key
        UNIQUE (connection_id, event_id);
CREATE INDEX idx_channel_inbound_event_connection_conversation
    ON channel_inbound_event(connection_id, conversation_key, status, created_at);
