-- name: CreateChannelBindToken :one
INSERT INTO channel_bind_token (token_hash, provider, external_user_id, expires_at)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: ConsumeChannelBindToken :one
UPDATE channel_bind_token SET
    consumed_at = now()
WHERE token_hash = $1
  AND consumed_at IS NULL
  AND expires_at > now()
RETURNING *;

-- name: GetChannelBindToken :one
SELECT * FROM channel_bind_token
WHERE token_hash = $1;

-- name: DeleteExpiredChannelBindTokens :exec
DELETE FROM channel_bind_token
WHERE expires_at < now() - interval '1 day';
