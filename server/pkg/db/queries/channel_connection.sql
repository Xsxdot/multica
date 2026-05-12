-- name: ListEnabledChannelConnections :many
SELECT * FROM channel_connection
WHERE enabled = true
ORDER BY provider ASC, created_at ASC;

-- name: ListChannelConnections :many
SELECT * FROM channel_connection
ORDER BY provider ASC, created_at ASC;
