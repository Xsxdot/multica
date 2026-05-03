-- name: GetChannelUserBindingByExternal :one
SELECT * FROM channel_user_binding
WHERE provider = $1 AND external_user_id = $2;

-- name: GetChannelUserBindingByUser :one
SELECT * FROM channel_user_binding
WHERE provider = $1 AND user_id = $2;

-- name: CreateChannelUserBinding :one
INSERT INTO channel_user_binding (provider, external_user_id, user_id, external_name)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: DeleteChannelUserBindingByUser :exec
DELETE FROM channel_user_binding
WHERE provider = $1 AND user_id = $2;

-- name: DeleteChannelUserBindingByExternal :exec
DELETE FROM channel_user_binding
WHERE provider = $1 AND external_user_id = $2;
