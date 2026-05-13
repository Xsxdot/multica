ALTER TABLE channel_chat_binding
    ADD COLUMN IF NOT EXISTS default_project_id UUID REFERENCES project(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_channel_chat_binding_default_project
    ON channel_chat_binding(default_project_id)
    WHERE default_project_id IS NOT NULL;

CREATE TABLE channel_conversation (
    provider            TEXT         NOT NULL CHECK (provider IN ('feishu')),
    conversation_key    TEXT         NOT NULL,
    chat_id             TEXT         NOT NULL,
    chat_type           TEXT         NOT NULL CHECK (chat_type IN ('group', 'direct')),
    sender_external_id  TEXT         NOT NULL,
    active_event_id     UUID,
    active_since        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, conversation_key)
);

CREATE TABLE channel_inbound_event (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    provider            TEXT         NOT NULL CHECK (provider IN ('feishu')),
    event_id            TEXT         NOT NULL,
    event_type          TEXT         NOT NULL,
    conversation_key    TEXT         NOT NULL,
    chat_id             TEXT         NOT NULL,
    chat_type           TEXT         NOT NULL CHECK (chat_type IN ('group', 'direct')),
    sender_external_id  TEXT         NOT NULL,
    sender_name         TEXT         NOT NULL DEFAULT '',
    message_id          TEXT         NOT NULL DEFAULT '',
    text                TEXT         NOT NULL DEFAULT '',
    canonical_event     JSONB        NOT NULL,
    raw_payload         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status              TEXT         NOT NULL DEFAULT 'queued'
                                      CHECK (status IN (
                                          'queued',
                                          'processing',
                                          'processed',
                                          'waiting_agent',
                                          'waiting_user',
                                          'failed',
                                          'dead',
                                          'rejected_backpressure'
                                      )),
    phase               TEXT         NOT NULL DEFAULT 'pre'
                                      CHECK (phase IN ('pre', 'intent', 'post', 'done')),
    wait_kind           TEXT         CHECK (wait_kind IN ('intent', 'action', 'channel_turn', 'user_clarification')),
    wait_task_id        UUID         REFERENCES agent_task_queue(id) ON DELETE SET NULL,
    wait_expires_at     TIMESTAMPTZ,
    workspace_id        UUID         REFERENCES workspace(id) ON DELETE SET NULL,
    default_project_id  UUID         REFERENCES project(id) ON DELETE SET NULL,
    intent_payload      JSONB,
    dispatch_completed_at TIMESTAMPTZ,
    dispatch_reply_text TEXT         NOT NULL DEFAULT '',
    attempts            INTEGER      NOT NULL DEFAULT 0,
    max_attempts        INTEGER      NOT NULL DEFAULT 3,
    next_attempt_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    locked_at           TIMESTAMPTZ,
    locked_by           TEXT,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    last_error          TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (provider, event_id)
);

CREATE INDEX idx_channel_inbound_event_claim
    ON channel_inbound_event(status, next_attempt_at, created_at)
    WHERE status = 'queued';

CREATE INDEX idx_channel_inbound_event_processing
    ON channel_inbound_event(status, updated_at)
    WHERE status = 'processing';

CREATE INDEX idx_channel_inbound_event_waiting_agent
    ON channel_inbound_event(status, wait_task_id, updated_at)
    WHERE status = 'waiting_agent';

CREATE INDEX idx_channel_inbound_event_waiting_user_expiry
    ON channel_inbound_event(status, wait_expires_at)
    WHERE status = 'waiting_user';

CREATE INDEX idx_channel_inbound_event_conversation
    ON channel_inbound_event(provider, conversation_key, status, created_at);

CREATE TABLE channel_action_result (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    inbound_event_id    UUID        NOT NULL REFERENCES channel_inbound_event(id) ON DELETE CASCADE,
    action_kind         TEXT        NOT NULL CHECK (action_kind IN (
                                      'create_issue',
                                      'add_comment',
                                      'set_status',
                                      'set_assignee',
                                      'set_priority',
                                      'add_label',
                                      'remove_label'
                                    )),
    status              TEXT        NOT NULL DEFAULT 'processing'
                                      CHECK (status IN ('processing', 'completed')),
    result_payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    last_error          TEXT,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (inbound_event_id, action_kind)
);

CREATE INDEX idx_channel_action_result_event
    ON channel_action_result(inbound_event_id);
