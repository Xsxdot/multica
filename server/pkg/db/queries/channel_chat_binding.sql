-- name: ListChannelChatBindings :many
SELECT * FROM channel_chat_binding
WHERE workspace_id = $1
ORDER BY is_primary DESC, created_at ASC;

-- name: GetChannelChatBinding :one
SELECT * FROM channel_chat_binding
WHERE id = $1;

-- name: GetChannelChatBindingByProviderAndChatID :one
SELECT * FROM channel_chat_binding
WHERE connection_id = $1 AND external_chat_id = $2;

-- name: CreateChannelChatBinding :one
INSERT INTO channel_chat_binding (
    provider, connection_id, external_chat_id, chat_type, workspace_id,
    is_primary, bound_by_user_id, external_chat_name, default_project_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, sqlc.narg('default_project_id'))
RETURNING *;

-- name: UpdateChannelChatBindingDefaultProject :one
UPDATE channel_chat_binding SET
    default_project_id = sqlc.narg('default_project_id'),
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: DeleteChannelChatBinding :exec
DELETE FROM channel_chat_binding WHERE id = $1;

-- name: SetChannelChatBindingPrimary :one
UPDATE channel_chat_binding SET
    is_primary = $2,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: ClearPrimaryBindingsForWorkspaceProvider :exec
UPDATE channel_chat_binding SET
    is_primary = false,
    updated_at = now()
WHERE workspace_id = $1 AND connection_id = $2 AND is_primary = true;

-- name: GetPrimaryChannelChatBinding :one
SELECT * FROM channel_chat_binding
WHERE workspace_id = $1 AND connection_id = $2 AND is_primary = true;
