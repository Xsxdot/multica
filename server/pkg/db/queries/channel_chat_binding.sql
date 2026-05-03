-- name: GetChannelChatBindingByExternal :one
SELECT * FROM channel_chat_binding
WHERE provider = $1 AND external_chat_id = $2;

-- name: GetPrimaryChannelChatBindingByWorkspace :one
SELECT * FROM channel_chat_binding
WHERE provider = $1 AND workspace_id = $2 AND is_primary = TRUE;

-- name: ListChannelChatBindingsByWorkspace :many
SELECT * FROM channel_chat_binding
WHERE provider = $1 AND workspace_id = $2
ORDER BY is_primary DESC, created_at ASC;

-- name: CreateChannelChatBinding :one
INSERT INTO channel_chat_binding (provider, external_chat_id, chat_type, workspace_id, is_primary, bound_by_user_id, external_chat_name)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: DeleteChannelChatBindingByExternal :exec
DELETE FROM channel_chat_binding
WHERE provider = $1 AND external_chat_id = $2;

-- name: DeleteChannelChatBindingsByWorkspace :exec
DELETE FROM channel_chat_binding
WHERE provider = $1 AND workspace_id = $2;
