CREATE TABLE channel_action_proposal (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    code                TEXT        NOT NULL,
    connection_id       TEXT        NOT NULL REFERENCES channel_connection(id) ON DELETE CASCADE,
    chat_id             TEXT        NOT NULL,
    sender_external_id  TEXT        NOT NULL,
    workspace_id        UUID        NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    inbound_event_id    UUID        NOT NULL REFERENCES channel_inbound_event(id) ON DELETE CASCADE,
    action_kind         TEXT        NOT NULL CHECK (action_kind IN (
                                      'CreateIssue',
                                      'AddComment',
                                      'SetStatus',
                                      'SetAssignee',
                                      'SetPriority',
                                      'SetLabel'
                                    )),
    intent_payload      JSONB       NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'pending'
                                      CHECK (status IN ('pending', 'confirmed', 'cancelled', 'expired')),
    expires_at          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (inbound_event_id, action_kind)
);

CREATE INDEX idx_channel_action_proposal_lookup
    ON channel_action_proposal(connection_id, chat_id, sender_external_id, (upper(code)), created_at DESC);

CREATE INDEX idx_channel_action_proposal_expiry
    ON channel_action_proposal(status, expires_at)
    WHERE status = 'pending';
