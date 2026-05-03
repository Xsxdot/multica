-- Persistent retry queue for failed outbound notifications. DESIGN section 6 risk 5
-- mandates that Issue/Comment writes are the source of truth and channel
-- failures NEVER block the web write path; this table is the asynchronous
-- catch basin.
--
-- Lifecycle:
--   pending -> (worker picks up at next_retry_at) -> succeeded (row DELETEd)
--                                                -> failed (attempts++, exp backoff)
--                                                -> dead (attempts >= max, kept 30d for audit)
CREATE TABLE channel_outbound_failure (
    id                        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    provider                  TEXT         NOT NULL CHECK (provider IN ('feishu')),
    event_kind                TEXT         NOT NULL,            -- e.g. 'comment_mention', 'issue_assigned'
    target_user_id            UUID         REFERENCES "user"(id) ON DELETE CASCADE,
    target_external_user_id   TEXT,                              -- snapshot, survives unbinding
    payload                   JSONB        NOT NULL,             -- full OutboundCardMessage serialization
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

-- Worker claim hot path: SELECT ... WHERE status='pending' AND
-- next_retry_at <= now() ORDER BY next_retry_at LIMIT N FOR UPDATE SKIP
-- LOCKED. Partial index keeps the working set tiny -- dead rows accumulate
-- but stay out of the index, mirroring 067_task_queue_claim_candidate_index.
CREATE INDEX idx_channel_outbound_failure_pending
    ON channel_outbound_failure (next_retry_at)
    WHERE status = 'pending';

-- Audit / observability: list dead-letter queue for human triage,
-- list-by-target-user for support tickets ("did user X get notified?").
CREATE INDEX idx_channel_outbound_failure_target_user
    ON channel_outbound_failure (target_user_id, created_at DESC);
CREATE INDEX idx_channel_outbound_failure_dead
    ON channel_outbound_failure (created_at DESC)
    WHERE status = 'dead';
