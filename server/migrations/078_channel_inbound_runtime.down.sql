DROP INDEX IF EXISTS idx_channel_action_result_event;
DROP TABLE IF EXISTS channel_action_result;
DROP INDEX IF EXISTS idx_channel_inbound_event_conversation;
DROP INDEX IF EXISTS idx_channel_inbound_event_waiting_user_expiry;
DROP INDEX IF EXISTS idx_channel_inbound_event_waiting_agent;
DROP INDEX IF EXISTS idx_channel_inbound_event_processing;
DROP INDEX IF EXISTS idx_channel_inbound_event_claim;
DROP TABLE IF EXISTS channel_inbound_event;
DROP TABLE IF EXISTS channel_conversation;

DROP INDEX IF EXISTS idx_channel_chat_binding_default_project;
ALTER TABLE channel_chat_binding
    DROP COLUMN IF EXISTS default_project_id;
