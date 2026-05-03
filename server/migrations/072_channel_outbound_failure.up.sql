CREATE TABLE channel_outbound_failure (
    id                        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    provider                  TEXT         NOT NULL CHECK (provider IN ('feishu')),
    event_kind                TEXT         NOT NULL,
    target_user_id            UUID         REFERENCES "user"(id) ON DELETE CASCADE,
    target_external_user_id   TEXT,
    payload                   JSONB        NOT NULL,
    status                    TEXT         NOT NULL DEFAULT 'pending'
                                  CHECK (status IN ('pending', 'dead')),
    attempts                  INTEGER      NOT NULL DEFAULT 0,
    max_attempts              INTEGER      NOT NULL DEFAULT 3,
    next_retry_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_error                TEXT,
    last_attempted_at         TIMESTAMPTZ,
    created_at                TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_channel_outbound_failure_pending
    ON channel_outbound_failure (next_retry_at)
    WHERE status = 'pending';

CREATE INDEX idx_channel_outbound_failure_target_user
    ON channel_outbound_failure (target_user_id, created_at DESC);

CREATE INDEX idx_channel_outbound_failure_dead
    ON channel_outbound_failure (created_at DESC)
    WHERE status = 'dead';
