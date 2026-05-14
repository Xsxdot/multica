CREATE TABLE channel_outbound_notification (
    id                        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    provider                  TEXT         NOT NULL CHECK (provider IN ('feishu')),
    event_kind                TEXT         NOT NULL,
    target_user_id            UUID         REFERENCES "user"(id) ON DELETE CASCADE,
    target_external_user_id   TEXT         NOT NULL,
    title                     TEXT         NOT NULL,
    body                      TEXT         NOT NULL DEFAULT '',
    status                    TEXT         NOT NULL DEFAULT 'pending'
                                      CHECK (status IN ('pending', 'processing', 'sent', 'dead')),
    attempts                  INTEGER      NOT NULL DEFAULT 0,
    max_attempts              INTEGER      NOT NULL DEFAULT 3,
    aggregation_due_at        TIMESTAMPTZ  NOT NULL,
    next_attempt_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_error                TEXT,
    created_at                TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_channel_outbound_notification_due
    ON channel_outbound_notification (aggregation_due_at, next_attempt_at)
    WHERE status = 'pending';

CREATE INDEX idx_channel_outbound_notification_processing
    ON channel_outbound_notification (updated_at)
    WHERE status = 'processing';

CREATE INDEX idx_channel_outbound_notification_cleanup
    ON channel_outbound_notification (updated_at)
    WHERE status IN ('sent', 'dead');
