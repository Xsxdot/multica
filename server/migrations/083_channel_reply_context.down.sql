DROP TABLE IF EXISTS channel_reply_context;

ALTER TABLE channel_outbound_notification
    DROP COLUMN IF EXISTS replyable,
    DROP COLUMN IF EXISTS inbox_item_id,
    DROP COLUMN IF EXISTS issue_title,
    DROP COLUMN IF EXISTS issue_identifier,
    DROP COLUMN IF EXISTS issue_id,
    DROP COLUMN IF EXISTS workspace_id;
