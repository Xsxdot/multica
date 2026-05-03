-- One-shot binding tokens. PRD AC3.4 requires a 10-minute TTL and one-time
-- consumption. We only ever store the SHA-256 hash; the plaintext token is
-- delivered to the user via Feishu DM and never persisted server-side
-- (DESIGN section 6 risk 3).
CREATE TABLE channel_bind_token (
    token_hash         BYTEA        PRIMARY KEY,    -- 32 bytes SHA-256, natural key
    provider           TEXT         NOT NULL CHECK (provider IN ('feishu')),
    external_user_id   TEXT         NOT NULL,        -- whose binding this token grants
    expires_at         TIMESTAMPTZ  NOT NULL,
    consumed_at        TIMESTAMPTZ,                  -- NULL = still valid for one consume
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Validation hot path: SELECT ... WHERE token_hash = $1 AND consumed_at IS
-- NULL AND expires_at > now(). The PK already serves the equality lookup;
-- the partial index below skips long-tail consumed rows for an even tighter
-- working set on the validation path.
CREATE INDEX idx_channel_bind_token_unconsumed
    ON channel_bind_token (expires_at)
    WHERE consumed_at IS NULL;

-- GC worker hot path: DELETE WHERE expires_at < now() - interval '1 day'.
-- The partial index above also accelerates this scan since unconsumed
-- expired rows show up there first; no extra index required.
