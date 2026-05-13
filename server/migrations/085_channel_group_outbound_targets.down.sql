UPDATE channel_outbound_notification
SET target_external_user_id = COALESCE(NULLIF(mention_external_user_id, ''), target_chat_id)
WHERE target_external_user_id IS NULL;

ALTER TABLE channel_outbound_notification
    ALTER COLUMN target_external_user_id SET NOT NULL,
    DROP COLUMN IF EXISTS mention_external_user_id,
    DROP COLUMN IF EXISTS target_chat_id,
    DROP COLUMN IF EXISTS target_type;
