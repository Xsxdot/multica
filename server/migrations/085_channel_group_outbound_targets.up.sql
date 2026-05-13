ALTER TABLE channel_outbound_notification
    ADD COLUMN target_type TEXT NOT NULL DEFAULT 'user'
        CHECK (target_type IN ('user', 'chat')),
    ADD COLUMN target_chat_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN mention_external_user_id TEXT NOT NULL DEFAULT '';

UPDATE channel_outbound_notification
SET mention_external_user_id = target_external_user_id
WHERE target_external_user_id IS NOT NULL
  AND mention_external_user_id = '';

ALTER TABLE channel_outbound_notification
    ALTER COLUMN target_external_user_id DROP NOT NULL;

