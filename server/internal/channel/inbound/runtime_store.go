package inbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/channel/port"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const (
	InboundStatusQueued               = "queued"
	InboundStatusProcessing           = "processing"
	InboundStatusProcessed            = "processed"
	InboundStatusWaitingAgent         = "waiting_agent"
	InboundStatusWaitingUser          = "waiting_user"
	InboundStatusFailed               = "failed"
	InboundStatusDead                 = "dead"
	InboundStatusRejectedBackpressure = "rejected_backpressure"

	InboundPhasePre    = "pre"
	InboundPhaseIntent = "intent"
	InboundPhasePost   = "post"
	InboundPhaseDone   = "done"

	WaitKindIntent = "intent"
	WaitKindAction = "action"
	WaitKindUser   = "user_clarification"
)

type AcceptOptions struct {
	ConversationLimit int
	GlobalLimit       int
	BypassLimit       bool
}

type AcceptResult struct {
	EventID                  string
	Duplicate                bool
	Accepted                 bool
	RejectedBackpressure     bool
	ClarificationConsumed    bool
	QueueDepth               int
	ActiveWaitingForUserText string
}

type RetryResult struct {
	Dead bool
}

type InboundEventRecord struct {
	ID               string
	Event            port.InboundEvent
	Status           string
	Phase            string
	ConversationKey  string
	WaitKind         string
	WaitTaskID       string
	WorkspaceID      string
	DefaultProjectID string
	Attempts         int
	MaxAttempts      int
	UpdatedAt        time.Time
}

type WaitingAgentEvent struct {
	ID         string
	WaitKind   string
	WaitTaskID string
	UpdatedAt  time.Time
}

type ExpiredWaitingUserEvent struct {
	ID    string
	Event port.InboundEvent
}

type ChatBindingContext struct {
	WorkspaceID      string
	DefaultProjectID string
	ListenMode       string
	AgentID          string
}

type InboundEventStore interface {
	AcceptEvent(ctx context.Context, evt port.InboundEvent, opts AcceptOptions) (AcceptResult, error)
	Load(ctx context.Context, id string) (*InboundEventRecord, error)
	ClaimNext(ctx context.Context, workerID string) (*InboundEventRecord, error)
	SaveEvent(ctx context.Context, id string, evt port.InboundEvent, phase string, chatCtx ChatBindingContext) error
	MarkQueued(ctx context.Context, id string, evt port.InboundEvent, phase string, chatCtx ChatBindingContext) error
	MarkWaitingAgent(ctx context.Context, id string, evt port.InboundEvent, taskID string, chatCtx ChatBindingContext) error
	MarkWaitingUser(ctx context.Context, id string, evt port.InboundEvent, replyText string, chatCtx ChatBindingContext, expiresAt time.Time) error
	MarkProcessed(ctx context.Context, id string) error
	MarkRetry(ctx context.Context, id string, err error) (RetryResult, error)
	MarkDead(ctx context.Context, id string, err error) error
	ListWaitingAgent(ctx context.Context, limit int) ([]WaitingAgentEvent, error)
	LookupChatContext(ctx context.Context, channelName, chatID string) (ChatBindingContext, error)
	RequeueStaleProcessing(ctx context.Context, olderThan time.Duration) (int64, error)
	ExpireWaitingUser(ctx context.Context, limit int) ([]ExpiredWaitingUserEvent, error)
}

type DBInboundEventStore struct {
	pool *pgxpool.Pool
}

func NewDBInboundEventStore(pool *pgxpool.Pool) *DBInboundEventStore {
	return &DBInboundEventStore{pool: pool}
}

func ConversationKey(evt port.InboundEvent) string {
	chatType := normalizedRuntimeChatType(evt)
	if chatType == string(port.ChatTypeDirect) {
		return strings.Join([]string{evt.ConnectionID(), chatType, evt.SenderID}, ":")
	}
	return strings.Join([]string{evt.ConnectionID(), chatType, evt.ChatID, evt.SenderID}, ":")
}

func ControlMessageBypassesBackpressure(evt port.InboundEvent) bool {
	text := strings.TrimSpace(strings.ToLower(evt.Text))
	return text == "/bind" || text == "/help" || text == "help"
}

func (s *DBInboundEventStore) AcceptEvent(ctx context.Context, evt port.InboundEvent, opts AcceptOptions) (AcceptResult, error) {
	if s == nil || s.pool == nil {
		return AcceptResult{}, errors.New("inbound store is not configured")
	}
	if evt.ChannelName == "" || evt.EventID == "" {
		return AcceptResult{}, errors.New("inbound accept: missing channel_name or event_id")
	}
	connectionID := evt.ConnectionID()
	if connectionID == "" {
		return AcceptResult{}, errors.New("inbound accept: missing connection_id")
	}
	key := ConversationKey(evt)
	if key == "" {
		return AcceptResult{}, errors.New("inbound accept: missing conversation key")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AcceptResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var existingID, existingStatus string
	err = tx.QueryRow(ctx, `
SELECT id::text, status FROM channel_inbound_event
WHERE connection_id = $1 AND event_id = $2
`, connectionID, evt.EventID).Scan(&existingID, &existingStatus)
	if err == nil {
		return AcceptResult{EventID: existingID, Duplicate: true}, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return AcceptResult{}, err
	}

	if opts.GlobalLimit > 0 {
		var pending int
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM channel_inbound_event
WHERE status IN ('queued', 'processing', 'waiting_agent', 'waiting_user')
`).Scan(&pending); err != nil {
			return AcceptResult{}, err
		}
		if pending >= opts.GlobalLimit {
			id, err := insertInboundEvent(ctx, tx, evt, key, InboundStatusRejectedBackpressure, InboundPhaseDone)
			if err != nil {
				return AcceptResult{}, err
			}
			if err := tx.Commit(ctx); err != nil {
				return AcceptResult{}, err
			}
			return AcceptResult{EventID: id, RejectedBackpressure: true, QueueDepth: pending}, nil
		}
	}

	if err := upsertConversation(ctx, tx, evt, key); err != nil {
		return AcceptResult{}, err
	}

	var activeID string
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(active_event_id::text, '')
FROM channel_conversation
WHERE connection_id = $1 AND conversation_key = $2
FOR UPDATE
`, connectionID, key).Scan(&activeID); err != nil {
		return AcceptResult{}, err
	}
	if activeID != "" {
		activeStatus, activeText, waitExpiresAt, terminal, err := loadActiveEventState(ctx, tx, activeID)
		if err != nil {
			return AcceptResult{}, err
		}
		if terminal {
			if err := clearConversationActive(ctx, tx, connectionID, key, activeID); err != nil {
				return AcceptResult{}, err
			}
			activeID = ""
		} else if activeStatus == InboundStatusWaitingUser && !waitExpiresAt.IsZero() && !time.Now().Before(waitExpiresAt) {
			if err := markDeadTx(ctx, tx, activeID, "user clarification timed out"); err != nil {
				return AcceptResult{}, err
			}
			if err := clearConversationActive(ctx, tx, connectionID, key, activeID); err != nil {
				return AcceptResult{}, err
			}
			activeID = ""
		} else if activeStatus == InboundStatusWaitingUser && !opts.BypassLimit {
			id, err := insertInboundEvent(ctx, tx, evt, key, InboundStatusProcessed, InboundPhaseDone)
			if err != nil {
				return AcceptResult{}, err
			}
			combined := combineClarification(activeText, evt.Text)
			if _, err := tx.Exec(ctx, `
UPDATE channel_inbound_event
SET text = $2,
    canonical_event = jsonb_set(canonical_event, '{Text}', to_jsonb($2::text), true),
    status = 'queued',
    phase = 'intent',
    wait_kind = NULL,
    wait_task_id = NULL,
    wait_expires_at = NULL,
    next_attempt_at = now(),
    updated_at = now(),
    last_error = NULL
WHERE id = $1
`, activeID, combined); err != nil {
				return AcceptResult{}, err
			}
			if err := tx.Commit(ctx); err != nil {
				return AcceptResult{}, err
			}
			return AcceptResult{
				EventID:                  id,
				ClarificationConsumed:    true,
				ActiveWaitingForUserText: activeText,
			}, nil
		}
	}

	var depth int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM channel_inbound_event
WHERE connection_id = $1
  AND conversation_key = $2
  AND status IN ('queued', 'processing', 'waiting_agent', 'waiting_user')
`, connectionID, key).Scan(&depth); err != nil {
		return AcceptResult{}, err
	}

	if !opts.BypassLimit && opts.ConversationLimit > 0 && depth >= opts.ConversationLimit {
		id, err := insertInboundEvent(ctx, tx, evt, key, InboundStatusRejectedBackpressure, InboundPhaseDone)
		if err != nil {
			return AcceptResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return AcceptResult{}, err
		}
		return AcceptResult{EventID: id, RejectedBackpressure: true, QueueDepth: depth}, nil
	}

	id, err := insertInboundEvent(ctx, tx, evt, key, InboundStatusQueued, InboundPhasePre)
	if err != nil {
		return AcceptResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AcceptResult{}, err
	}
	return AcceptResult{EventID: id, Accepted: true, QueueDepth: depth}, nil
}

func (s *DBInboundEventStore) ClaimNext(ctx context.Context, workerID string) (*InboundEventRecord, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var id string
	err = tx.QueryRow(ctx, `
SELECT e.id::text
FROM channel_inbound_event e
JOIN channel_conversation c
  ON c.connection_id = e.connection_id
 AND c.conversation_key = e.conversation_key
WHERE e.status = 'queued'
  AND e.next_attempt_at <= now()
  AND (c.active_event_id IS NULL OR c.active_event_id = e.id)
ORDER BY e.created_at ASC
LIMIT 1
FOR UPDATE OF e, c SKIP LOCKED
`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
UPDATE channel_conversation c
SET active_event_id = e.id,
    active_since = COALESCE(c.active_since, now()),
    updated_at = now()
FROM channel_inbound_event e
WHERE e.id = $1
  AND c.connection_id = e.connection_id
  AND c.conversation_key = e.conversation_key
`, id); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
UPDATE channel_inbound_event
SET status = 'processing',
    attempts = attempts + 1,
    locked_at = now(),
    locked_by = $2,
    started_at = COALESCE(started_at, now()),
    updated_at = now()
WHERE id = $1
`, id, workerID); err != nil {
		return nil, err
	}

	rec, err := loadInboundEventRecord(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return rec, nil
}

func (s *DBInboundEventStore) Load(ctx context.Context, id string) (*InboundEventRecord, error) {
	return loadInboundEventRecord(ctx, s.pool, id)
}

func (s *DBInboundEventStore) SaveEvent(ctx context.Context, id string, evt port.InboundEvent, phase string, chatCtx ChatBindingContext) error {
	canonical, raw, err := marshalEvent(evt)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
UPDATE channel_inbound_event
SET text = $2,
    sender_external_id = $3,
    sender_name = $4,
    message_id = $5,
    canonical_event = $6,
    raw_payload = $7,
    phase = $8,
    workspace_id = nullif($9, '')::uuid,
    default_project_id = nullif($10, '')::uuid,
    updated_at = now()
WHERE id = $1
`, id, evt.Text, evt.SenderID, evt.SenderName, evt.MessageID, canonical, raw, phase, chatCtx.WorkspaceID, chatCtx.DefaultProjectID)
	return err
}

func (s *DBInboundEventStore) MarkQueued(ctx context.Context, id string, evt port.InboundEvent, phase string, chatCtx ChatBindingContext) error {
	canonical, raw, err := marshalEvent(evt)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
UPDATE channel_inbound_event
SET status = 'queued',
    phase = $2,
    wait_kind = NULL,
    wait_task_id = NULL,
    wait_expires_at = NULL,
    text = $3,
    canonical_event = $4,
    raw_payload = $5,
    workspace_id = nullif($6, '')::uuid,
    default_project_id = nullif($7, '')::uuid,
    next_attempt_at = now(),
    locked_at = NULL,
    locked_by = NULL,
    last_error = NULL,
    updated_at = now()
WHERE id = $1
`, id, phase, evt.Text, canonical, raw, chatCtx.WorkspaceID, chatCtx.DefaultProjectID)
	return err
}

func (s *DBInboundEventStore) MarkWaitingAgent(ctx context.Context, id string, evt port.InboundEvent, taskID string, chatCtx ChatBindingContext) error {
	canonical, raw, err := marshalEvent(evt)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
UPDATE channel_inbound_event
SET status = 'waiting_agent',
    phase = 'intent',
    wait_kind = 'intent',
    wait_task_id = nullif($2, '')::uuid,
    wait_expires_at = NULL,
    text = $3,
    canonical_event = $4,
    raw_payload = $5,
    workspace_id = nullif($6, '')::uuid,
    default_project_id = nullif($7, '')::uuid,
    locked_at = NULL,
    locked_by = NULL,
    updated_at = now()
WHERE id = $1
`, id, taskID, evt.Text, canonical, raw, chatCtx.WorkspaceID, chatCtx.DefaultProjectID)
	return err
}

func (s *DBInboundEventStore) MarkWaitingUser(ctx context.Context, id string, evt port.InboundEvent, replyText string, chatCtx ChatBindingContext, expiresAt time.Time) error {
	canonical, raw, err := marshalEvent(evt)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
UPDATE channel_inbound_event
SET status = 'waiting_user',
    phase = 'intent',
    wait_kind = 'user_clarification',
    wait_task_id = NULL,
    wait_expires_at = $8,
    text = $3,
    canonical_event = $4,
    raw_payload = $5,
    workspace_id = nullif($6, '')::uuid,
    default_project_id = nullif($7, '')::uuid,
    last_error = $2,
    locked_at = NULL,
    locked_by = NULL,
    updated_at = now()
WHERE id = $1
`, id, replyText, evt.Text, canonical, raw, chatCtx.WorkspaceID, chatCtx.DefaultProjectID, expiresAt)
	return err
}

func (s *DBInboundEventStore) MarkProcessed(ctx context.Context, id string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
UPDATE channel_inbound_event
SET status = 'processed',
    phase = 'done',
    completed_at = now(),
    wait_expires_at = NULL,
    locked_at = NULL,
    locked_by = NULL,
    updated_at = now(),
    last_error = NULL
WHERE id = $1
`, id); err != nil {
		return err
	}
	if err := clearConversationActiveForEvent(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *DBInboundEventStore) MarkRetry(ctx context.Context, id string, runErr error) (RetryResult, error) {
	msg := truncateErr(runErr)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RetryResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var attempts, maxAttempts int
	if err := tx.QueryRow(ctx, `
SELECT attempts, max_attempts FROM channel_inbound_event WHERE id = $1 FOR UPDATE
`, id).Scan(&attempts, &maxAttempts); err != nil {
		return RetryResult{}, err
	}
	if attempts >= maxAttempts {
		if err := markDeadTx(ctx, tx, id, msg); err != nil {
			return RetryResult{}, err
		}
		if err := clearConversationActiveForEvent(ctx, tx, id); err != nil {
			return RetryResult{}, err
		}
		return RetryResult{Dead: true}, tx.Commit(ctx)
	}
	delay := time.Duration(attempts) * 30 * time.Second
	if delay < 5*time.Second {
		delay = 5 * time.Second
	}
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	if _, err := tx.Exec(ctx, `
UPDATE channel_inbound_event
SET status = 'queued',
    next_attempt_at = now() + $2::interval,
    locked_at = NULL,
    locked_by = NULL,
    last_error = $3,
    updated_at = now()
WHERE id = $1
`, id, fmt.Sprintf("%f seconds", delay.Seconds()), msg); err != nil {
		return RetryResult{}, err
	}
	return RetryResult{}, tx.Commit(ctx)
}

func (s *DBInboundEventStore) MarkDead(ctx context.Context, id string, runErr error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := markDeadTx(ctx, tx, id, truncateErr(runErr)); err != nil {
		return err
	}
	if err := clearConversationActiveForEvent(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *DBInboundEventStore) ListWaitingAgent(ctx context.Context, limit int) ([]WaitingAgentEvent, error) {
	if limit <= 0 {
		limit = 32
	}
	rows, err := s.pool.Query(ctx, `
SELECT id::text, COALESCE(wait_kind, ''), COALESCE(wait_task_id::text, ''), updated_at
FROM channel_inbound_event
WHERE status = 'waiting_agent'
ORDER BY updated_at ASC
LIMIT $1
`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]WaitingAgentEvent, 0, limit)
	for rows.Next() {
		var item WaitingAgentEvent
		if err := rows.Scan(&item.ID, &item.WaitKind, &item.WaitTaskID, &item.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *DBInboundEventStore) LookupChatContext(ctx context.Context, connectionID, chatID string) (ChatBindingContext, error) {
	q := db.New(s.pool)
	row, err := q.GetChannelChatBindingContextForInbound(ctx, db.GetChannelChatBindingContextForInboundParams{
		ConnectionID:   connectionID,
		ExternalChatID: chatID,
	})
	if err != nil {
		return ChatBindingContext{}, err
	}
	listen := row.ListenMode
	if listen == "" {
		listen = "mentions"
	}
	return ChatBindingContext{
		WorkspaceID:      row.WorkspaceID,
		DefaultProjectID: sqlcOptionalString(row.DefaultProjectID),
		ListenMode:       listen,
		AgentID:          sqlcOptionalString(row.AgentID),
	}, nil
}

func sqlcOptionalString(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

func (s *DBInboundEventStore) RequeueStaleProcessing(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		olderThan = 5 * time.Minute
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE channel_inbound_event
SET status = 'queued',
    next_attempt_at = now(),
    locked_at = NULL,
    locked_by = NULL,
    last_error = COALESCE(last_error, 'processing lease expired'),
    updated_at = now()
WHERE status = 'processing'
  AND updated_at < now() - $1::interval
`, fmt.Sprintf("%f seconds", olderThan.Seconds()))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *DBInboundEventStore) ExpireWaitingUser(ctx context.Context, limit int) ([]ExpiredWaitingUserEvent, error) {
	if limit <= 0 {
		limit = 32
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
SELECT id::text
FROM channel_inbound_event
WHERE status = 'waiting_user'
  AND wait_expires_at IS NOT NULL
  AND wait_expires_at <= now()
ORDER BY wait_expires_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED
`, limit)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	out := make([]ExpiredWaitingUserEvent, 0, len(ids))
	for _, id := range ids {
		rec, err := loadInboundEventRecord(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err := markDeadTx(ctx, tx, id, "user clarification timed out"); err != nil {
			return nil, err
		}
		if err := clearConversationActiveForEvent(ctx, tx, id); err != nil {
			return nil, err
		}
		out = append(out, ExpiredWaitingUserEvent{ID: id, Event: rec.Event})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func upsertConversation(ctx context.Context, tx pgx.Tx, evt port.InboundEvent, key string) error {
	chatType := normalizedRuntimeChatType(evt)
	_, err := tx.Exec(ctx, `
INSERT INTO channel_conversation (provider, connection_id, conversation_key, chat_id, chat_type, sender_external_id)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (connection_id, conversation_key) DO UPDATE SET
    chat_id = EXCLUDED.chat_id,
    chat_type = EXCLUDED.chat_type,
    sender_external_id = EXCLUDED.sender_external_id,
    updated_at = now()
`, evt.ChannelName, evt.ConnectionID(), key, evt.ChatID, chatType, evt.SenderID)
	return err
}

func insertInboundEvent(ctx context.Context, tx pgx.Tx, evt port.InboundEvent, key, status, phase string) (string, error) {
	canonical, raw, err := marshalEvent(evt)
	if err != nil {
		return "", err
	}
	var id string
	err = tx.QueryRow(ctx, `
INSERT INTO channel_inbound_event (
    provider, connection_id, event_id, event_type, conversation_key, chat_id, chat_type,
    sender_external_id, sender_name, message_id, text, canonical_event,
    raw_payload, status, phase
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, $12,
    $13, $14, $15
)
RETURNING id::text
`, evt.ChannelName, evt.ConnectionID(), evt.EventID, string(evt.Type), key, evt.ChatID, normalizedRuntimeChatType(evt),
		evt.SenderID, evt.SenderName, evt.MessageID, evt.Text, canonical, raw, status, phase).Scan(&id)
	return id, err
}

func normalizedRuntimeChatType(evt port.InboundEvent) string {
	if evt.ChatType == port.ChatTypeDirect {
		return string(port.ChatTypeDirect)
	}
	return string(port.ChatTypeGroup)
}

func loadInboundEventRecord(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id string) (*InboundEventRecord, error) {
	var (
		rec              InboundEventRecord
		canonical        []byte
		waitKind         string
		waitTaskID       string
		workspaceID      string
		defaultProjectID string
		connectionID     string
	)
	err := q.QueryRow(ctx, `
SELECT id::text, canonical_event, status, phase, conversation_key,
       COALESCE(wait_kind, ''), COALESCE(wait_task_id::text, ''),
       COALESCE(workspace_id::text, ''), COALESCE(default_project_id::text, ''),
       attempts, max_attempts, updated_at, connection_id
FROM channel_inbound_event
WHERE id = $1
`, id).Scan(
		&rec.ID,
		&canonical,
		&rec.Status,
		&rec.Phase,
		&rec.ConversationKey,
		&waitKind,
		&waitTaskID,
		&workspaceID,
		&defaultProjectID,
		&rec.Attempts,
		&rec.MaxAttempts,
		&rec.UpdatedAt,
		&connectionID,
	)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(canonical, &rec.Event); err != nil {
		return nil, err
	}
	if rec.Event.ChannelConnectionID == "" {
		rec.Event.ChannelConnectionID = connectionID
	}
	rec.Event.RuntimeEventID = rec.ID
	rec.WaitKind = waitKind
	rec.WaitTaskID = waitTaskID
	rec.WorkspaceID = workspaceID
	rec.DefaultProjectID = defaultProjectID
	return &rec, nil
}

func loadActiveEventState(ctx context.Context, tx pgx.Tx, id string) (status, text string, waitExpiresAt time.Time, terminal bool, err error) {
	var waitExpires pgtype.Timestamptz
	err = tx.QueryRow(ctx, `
SELECT status, text, wait_expires_at FROM channel_inbound_event WHERE id = $1 FOR UPDATE
`, id).Scan(&status, &text, &waitExpires)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", time.Time{}, true, nil
	}
	if err != nil {
		return "", "", time.Time{}, false, err
	}
	if waitExpires.Valid {
		waitExpiresAt = waitExpires.Time
	}
	switch status {
	case InboundStatusProcessed, InboundStatusDead, InboundStatusRejectedBackpressure:
		return status, text, waitExpiresAt, true, nil
	default:
		return status, text, waitExpiresAt, false, nil
	}
}

func marshalEvent(evt port.InboundEvent) ([]byte, []byte, error) {
	canonical, err := json.Marshal(evt)
	if err != nil {
		return nil, nil, err
	}
	raw := evt.RawPayload
	if len(raw) == 0 || !json.Valid(raw) {
		raw = json.RawMessage(`{}`)
	}
	return canonical, raw, nil
}

func combineClarification(original, clarification string) string {
	original = strings.TrimSpace(original)
	clarification = strings.TrimSpace(clarification)
	if original == "" {
		return clarification
	}
	if clarification == "" {
		return original
	}
	return original + "\n\n用户补充：" + clarification
}

func clearConversationActive(ctx context.Context, tx pgx.Tx, connectionID, key, activeID string) error {
	_, err := tx.Exec(ctx, `
UPDATE channel_conversation
SET active_event_id = NULL,
    active_since = NULL,
    updated_at = now()
WHERE connection_id = $1
  AND conversation_key = $2
  AND active_event_id = $3
`, connectionID, key, activeID)
	return err
}

func clearConversationActiveForEvent(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `
UPDATE channel_conversation c
SET active_event_id = NULL,
    active_since = NULL,
    updated_at = now()
FROM channel_inbound_event e
WHERE e.id = $1
  AND c.connection_id = e.connection_id
  AND c.conversation_key = e.conversation_key
  AND c.active_event_id = e.id
`, id)
	return err
}

func markDeadTx(ctx context.Context, tx pgx.Tx, id, msg string) error {
	_, err := tx.Exec(ctx, `
UPDATE channel_inbound_event
SET status = 'dead',
    phase = 'done',
    completed_at = now(),
    wait_expires_at = NULL,
    locked_at = NULL,
    locked_by = NULL,
    last_error = $2,
    updated_at = now()
WHERE id = $1
`, id, msg)
	return err
}

func truncateErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > 2000 {
		msg = msg[:2000]
	}
	return msg
}

var _ InboundEventStore = (*DBInboundEventStore)(nil)
