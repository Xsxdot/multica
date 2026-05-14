DROP INDEX IF EXISTS idx_channel_chat_binding_agent;

ALTER TABLE channel_chat_binding DROP CONSTRAINT IF EXISTS channel_chat_binding_listen_mode_check;

ALTER TABLE channel_chat_binding DROP COLUMN IF EXISTS agent_id;

ALTER TABLE channel_chat_binding DROP COLUMN IF EXISTS listen_mode;
