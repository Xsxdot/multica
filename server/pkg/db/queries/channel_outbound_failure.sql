-- name: InsertOutboundFailure :one
INSERT INTO channel_outbound_failure (
    provider, event_kind, target_user_id, target_external_user_id,
    payload, status, max_attempts
) VALUES ($1, $2, $3, $4, $5, 'pending', $6)
RETURNING *;

-- name: ClaimPendingOutboundFailures :many
-- Claim up to N pending failures whose next_retry_at <= now().
-- FOR UPDATE SKIP LOCKED prevents contention across multiple worker
-- instances and avoids deadlocks on the same rows.
SELECT * FROM channel_outbound_failure
WHERE status = 'pending'
  AND next_retry_at <= now()
ORDER BY next_retry_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;

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
