-- name: TryRecordInboundEvent :one
INSERT INTO channel_inbound_event_dedup (provider, event_id)
VALUES ($1, $2)
ON CONFLICT (provider, event_id) DO NOTHING
RETURNING provider, event_id, processed_at;

-- name: DeleteOldInboundEvents :exec
DELETE FROM channel_inbound_event_dedup
WHERE processed_at < now() - interval '7 days';
