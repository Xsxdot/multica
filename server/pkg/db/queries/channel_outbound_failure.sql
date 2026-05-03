-- name: EnqueueOutboundFailure :one
INSERT INTO channel_outbound_failure (
    provider, event_kind, target_user_id, target_external_user_id,
    payload, status, max_attempts, next_retry_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING *;

-- name: ClaimOutboundFailures :many
SELECT * FROM channel_outbound_failure
WHERE status = 'pending' AND next_retry_at <= now()
ORDER BY next_retry_at
LIMIT sqlc.arg('limit')
FOR UPDATE SKIP LOCKED;

-- name: MarkOutboundFailureRetry :exec
UPDATE channel_outbound_failure SET
    attempts = attempts + 1,
    next_retry_at = $2,
    last_error = $3,
    last_attempted_at = now(),
    updated_at = now()
WHERE id = $1;

-- name: MarkOutboundFailureDead :exec
UPDATE channel_outbound_failure SET
    status = 'dead',
    last_error = $2,
    last_attempted_at = now(),
    updated_at = now()
WHERE id = $1;

-- name: DeleteOutboundFailure :exec
DELETE FROM channel_outbound_failure
WHERE id = $1;

-- name: DeleteOldDeadOutboundFailures :exec
DELETE FROM channel_outbound_failure
WHERE status = 'dead' AND created_at < now() - interval '30 days';

-- name: ListOutboundFailuresByTargetUser :many
SELECT * FROM channel_outbound_failure
WHERE target_user_id = $1
ORDER BY created_at DESC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');
