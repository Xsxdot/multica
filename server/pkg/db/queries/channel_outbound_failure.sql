-- name: InsertOutboundFailure :one
INSERT INTO channel_outbound_failure (
    provider, connection_id, event_kind, target_user_id, target_external_user_id,
    payload, status, max_attempts
) VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7)
RETURNING *;

-- name: ClaimPendingOutboundFailures :many
-- Atomically claim up to N pending failures: the UPDATE locks rows via
-- the FOR UPDATE SKIP LOCKED subquery so two replicas never process the
-- same failure concurrently.
--
-- Claimed rows get a 5-minute cooldown on next_retry_at to prevent
-- re-claim before IncrementAttempts/MarkDead overwrites it with the
-- real backoff. 5 minutes is chosen as ~2× the worst-case adapter call
-- timeout (Feishu OpenAPI default ≈ 60s, generous headroom for slow
-- networks). If a worker hangs longer than 5 min on a single SendCard,
-- another replica may legitimately re-claim the row — that's a tradeoff
-- between availability (stuck worker shouldn't permanently lock a row)
-- and exactly-once (we accept rare double-send during a multi-minute
-- stall). Bump this if you raise SendCard timeouts.
UPDATE channel_outbound_failure SET
    next_retry_at = now() + interval '5 minutes',
    updated_at = now()
WHERE id IN (
    SELECT id FROM channel_outbound_failure
    WHERE status = 'pending'
      AND next_retry_at <= now()
    ORDER BY next_retry_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: ClaimPendingOutboundFailuresForConnections :many
UPDATE channel_outbound_failure SET
    next_retry_at = now() + interval '5 minutes',
    updated_at = now()
WHERE id IN (
    SELECT id FROM channel_outbound_failure
    WHERE status = 'pending'
      AND next_retry_at <= now()
      AND connection_id = ANY(sqlc.arg(connection_ids)::text[])
    ORDER BY next_retry_at ASC
    LIMIT sqlc.arg(claim_limit)
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: IncrementOutboundFailureAttempts :one
-- Record a retry attempt: bump attempts, set last_error, last_attempted_at,
-- and compute next_retry_at using the caller-supplied backoff duration.
UPDATE channel_outbound_failure SET
    attempts = attempts + 1,
    last_error = $2,
    last_attempted_at = now(),
    next_retry_at = now() + $3::interval,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: MarkOutboundFailureDead :one
-- Permanently mark a failure as dead (non-retryable error or max attempts exceeded).
UPDATE channel_outbound_failure SET
    status = 'dead',
    last_error = $2,
    last_attempted_at = now(),
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: DeleteOutboundFailure :exec
-- Delete a failure record (used when retry succeeds).
DELETE FROM channel_outbound_failure WHERE id = $1;

-- name: DeleteOldDeadOutboundFailures :exec
-- Cleanup: remove dead entries older than 7 days.
DELETE FROM channel_outbound_failure
WHERE status = 'dead'
  AND created_at < now() - interval '7 days';

-- name: CleanupOldInboundEventDedup :exec
-- Cleanup: remove dedup entries older than 7 days (TC-out-4 / DESIGN §8 T6).
DELETE FROM channel_inbound_event_dedup
WHERE processed_at < now() - interval '7 days';

-- name: CleanupExpiredBindTokens :exec
-- Cleanup: remove consumed or expired bind tokens older than 1 day.
DELETE FROM channel_bind_token
WHERE (consumed_at IS NOT NULL AND consumed_at < now() - interval '1 day')
   OR (consumed_at IS NULL AND expires_at < now() - interval '1 day');
