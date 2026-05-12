DROP INDEX IF EXISTS idx_channel_inbound_event_connection_conversation;
ALTER TABLE channel_inbound_event
    DROP CONSTRAINT IF EXISTS channel_inbound_event_connection_event_id_key,
    ADD CONSTRAINT channel_inbound_event_provider_event_id_key UNIQUE (provider, event_id),
    DROP CONSTRAINT IF EXISTS channel_inbound_event_connection_fk,
    DROP COLUMN IF EXISTS connection_id,
    ADD CONSTRAINT channel_inbound_event_provider_check CHECK (provider IN ('feishu'));

ALTER TABLE channel_conversation
    DROP CONSTRAINT IF EXISTS channel_conversation_pkey,
    ADD PRIMARY KEY (provider, conversation_key),
    DROP CONSTRAINT IF EXISTS channel_conversation_connection_fk,
    DROP COLUMN IF EXISTS connection_id,
    ADD CONSTRAINT channel_conversation_provider_check CHECK (provider IN ('feishu'));

DROP INDEX IF EXISTS idx_channel_outbound_notification_connection;
ALTER TABLE channel_outbound_notification
    DROP CONSTRAINT IF EXISTS channel_outbound_notification_connection_fk,
    DROP COLUMN IF EXISTS connection_id,
    ADD CONSTRAINT channel_outbound_notification_provider_check CHECK (provider IN ('feishu'));

DROP INDEX IF EXISTS idx_channel_outbound_failure_connection;
ALTER TABLE channel_outbound_failure
    DROP CONSTRAINT IF EXISTS channel_outbound_failure_connection_fk,
    DROP COLUMN IF EXISTS connection_id,
    ADD CONSTRAINT channel_outbound_failure_provider_check CHECK (provider IN ('feishu'));

ALTER TABLE channel_inbound_event_dedup
    DROP CONSTRAINT IF EXISTS channel_inbound_event_dedup_pkey,
    ADD PRIMARY KEY (provider, event_id),
    DROP CONSTRAINT IF EXISTS channel_inbound_event_dedup_connection_fk,
    DROP COLUMN IF EXISTS connection_id,
    ADD CONSTRAINT channel_inbound_event_dedup_provider_check CHECK (provider IN ('feishu'));

DROP INDEX IF EXISTS idx_channel_bind_token_connection;
ALTER TABLE channel_bind_token
    DROP CONSTRAINT IF EXISTS channel_bind_token_connection_fk,
    DROP COLUMN IF EXISTS connection_id,
    ADD CONSTRAINT channel_bind_token_provider_check CHECK (provider IN ('feishu'));

DROP INDEX IF EXISTS idx_channel_chat_binding_workspace_connection;
DROP INDEX IF EXISTS idx_channel_chat_binding_primary_per_connection_ws;
ALTER TABLE channel_chat_binding
    DROP CONSTRAINT IF EXISTS channel_chat_binding_connection_external_chat_id_key,
    ADD CONSTRAINT channel_chat_binding_provider_external_chat_id_key UNIQUE (provider, external_chat_id),
    DROP CONSTRAINT IF EXISTS channel_chat_binding_connection_fk,
    DROP COLUMN IF EXISTS connection_id,
    ADD CONSTRAINT channel_chat_binding_provider_check CHECK (provider IN ('feishu'));
CREATE UNIQUE INDEX idx_channel_chat_binding_primary_per_ws
    ON channel_chat_binding (provider, workspace_id)
    WHERE is_primary;

ALTER TABLE channel_user_binding
    DROP CONSTRAINT IF EXISTS channel_user_binding_connection_user_id_key,
    DROP CONSTRAINT IF EXISTS channel_user_binding_connection_external_user_id_key,
    ADD CONSTRAINT channel_user_binding_provider_external_user_id_key UNIQUE (provider, external_user_id),
    ADD CONSTRAINT channel_user_binding_provider_user_id_key UNIQUE (provider, user_id),
    DROP CONSTRAINT IF EXISTS channel_user_binding_connection_fk,
    DROP COLUMN IF EXISTS connection_id,
    ADD CONSTRAINT channel_user_binding_provider_check CHECK (provider IN ('feishu'));

DROP INDEX IF EXISTS idx_channel_connection_provider_default;
DROP TABLE IF EXISTS channel_connection;
