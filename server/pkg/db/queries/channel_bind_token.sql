-- name: CreateChannelBindToken :one
INSERT INTO channel_bind_token (
    token_hash, purpose, provider, external_user_id,
    external_chat_id, external_chat_type, external_chat_name,
    expires_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
