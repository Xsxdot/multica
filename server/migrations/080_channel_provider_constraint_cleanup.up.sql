ALTER TABLE channel_user_binding
    DROP CONSTRAINT IF EXISTS channel_user_binding_provider_check;

ALTER TABLE channel_chat_binding
    DROP CONSTRAINT IF EXISTS channel_chat_binding_provider_check;

ALTER TABLE channel_bind_token
    DROP CONSTRAINT IF EXISTS channel_bind_token_provider_check;

ALTER TABLE channel_inbound_event_dedup
    DROP CONSTRAINT IF EXISTS channel_inbound_event_dedup_provider_check;

ALTER TABLE channel_outbound_failure
    DROP CONSTRAINT IF EXISTS channel_outbound_failure_provider_check;

ALTER TABLE channel_outbound_notification
    DROP CONSTRAINT IF EXISTS channel_outbound_notification_provider_check;

ALTER TABLE channel_conversation
    DROP CONSTRAINT IF EXISTS channel_conversation_provider_check;

ALTER TABLE channel_inbound_event
    DROP CONSTRAINT IF EXISTS channel_inbound_event_provider_check;
