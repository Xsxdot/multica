ALTER TABLE channel_outbound_notification
    ADD COLUMN workspace_id UUID REFERENCES workspace(id) ON DELETE SET NULL,
    ADD COLUMN issue_id UUID REFERENCES issue(id) ON DELETE SET NULL,
    ADD COLUMN issue_identifier TEXT NOT NULL DEFAULT '',
    ADD COLUMN issue_title TEXT NOT NULL DEFAULT '',
    ADD COLUMN inbox_item_id UUID REFERENCES inbox_item(id) ON DELETE SET NULL,
    ADD COLUMN replyable BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE channel_reply_context (
    connection_id       TEXT        NOT NULL REFERENCES channel_connection(id) ON DELETE CASCADE,
    external_user_id    TEXT        NOT NULL,
    workspace_id        UUID        NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    issue_id            UUID        NOT NULL REFERENCES issue(id) ON DELETE CASCADE,
    issue_identifier    TEXT        NOT NULL DEFAULT '',
    issue_title         TEXT        NOT NULL DEFAULT '',
    inbox_item_id       UUID        REFERENCES inbox_item(id) ON DELETE SET NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (connection_id, external_user_id)
);

CREATE INDEX idx_channel_reply_context_expiry
    ON channel_reply_context (expires_at);
