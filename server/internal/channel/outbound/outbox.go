package outbound

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	channelmetrics "github.com/multica-ai/multica/server/internal/channel/metrics"
)

const (
	OutboxAggregationWindow = 60 * time.Second
	OutboxBatchSize         = 100
	OutboxTickInterval      = 10 * time.Second
)

type NotificationEnqueueRequest struct {
	Provider             string
	ConnectionID         string
	EventKind            string
	TargetUserID         pgtype.UUID
	TargetExternalUserID string
	Title                string
	Body                 string
}

type NotificationEnqueuer interface {
	EnqueueNotification(ctx context.Context, req NotificationEnqueueRequest) error
}

type OutboxNotification struct {
	ID                   pgtype.UUID
	Provider             string
	ConnectionID         string
	EventKind            string
	TargetUserID         pgtype.UUID
	TargetExternalUserID string
	Title                string
	Body                 string
	Attempts             int32
	MaxAttempts          int32
}

type outboxDB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type DBNotificationStore struct {
	db     outboxDB
	window time.Duration
}

func NewDBNotificationStore(db outboxDB) *DBNotificationStore {
	return &DBNotificationStore{db: db, window: OutboxAggregationWindow}
}

func (s *DBNotificationStore) EnqueueNotification(ctx context.Context, req NotificationEnqueueRequest) error {
	if strings.TrimSpace(req.Provider) == "" || strings.TrimSpace(req.ConnectionID) == "" ||
		strings.TrimSpace(req.EventKind) == "" ||
		strings.TrimSpace(req.TargetExternalUserID) == "" || !req.TargetUserID.Valid {
		return errors.New("outbox: invalid notification enqueue request")
	}
	window := s.window
	if window <= 0 {
		window = OutboxAggregationWindow
	}
	const q = `
INSERT INTO channel_outbound_notification (
    provider, connection_id, event_kind, target_user_id, target_external_user_id,
    title, body, aggregation_due_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, now() + $8::interval)
`
	_, err := s.db.Exec(ctx, q,
		req.Provider,
		req.ConnectionID,
		req.EventKind,
		req.TargetUserID,
		req.TargetExternalUserID,
		req.Title,
		req.Body,
		pgInterval(window),
	)
	return err
}

func (s *DBNotificationStore) ClaimDue(ctx context.Context, limit int32) ([]OutboxNotification, error) {
	const q = `
UPDATE channel_outbound_notification SET
    status = 'processing',
    next_attempt_at = now() + interval '5 minutes',
    updated_at = now()
WHERE id IN (
    SELECT id FROM channel_outbound_notification
    WHERE status = 'pending'
      AND aggregation_due_at <= now()
      AND next_attempt_at <= now()
    ORDER BY aggregation_due_at ASC, created_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, provider, connection_id, event_kind, target_user_id, target_external_user_id,
          title, body, attempts, max_attempts
`
	return s.queryNotifications(ctx, q, limit)
}

func (s *DBNotificationStore) ReclaimStaleProcessing(ctx context.Context, limit int32, staleAfter time.Duration) ([]OutboxNotification, error) {
	const q = `
UPDATE channel_outbound_notification SET
    status = 'processing',
    next_attempt_at = now() + interval '5 minutes',
    updated_at = now()
WHERE id IN (
    SELECT id FROM channel_outbound_notification
    WHERE status = 'processing'
      AND updated_at < now() - $2::interval
    ORDER BY updated_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, provider, connection_id, event_kind, target_user_id, target_external_user_id,
          title, body, attempts, max_attempts
`
	return s.queryNotifications(ctx, q, limit, pgInterval(staleAfter))
}

func (s *DBNotificationStore) queryNotifications(ctx context.Context, q string, args ...any) ([]OutboxNotification, error) {
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []OutboxNotification
	for rows.Next() {
		var n OutboxNotification
		if err := rows.Scan(
			&n.ID,
			&n.Provider,
			&n.ConnectionID,
			&n.EventKind,
			&n.TargetUserID,
			&n.TargetExternalUserID,
			&n.Title,
			&n.Body,
			&n.Attempts,
			&n.MaxAttempts,
		); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *DBNotificationStore) MarkSent(ctx context.Context, ids []pgtype.UUID) error {
	const q = `
UPDATE channel_outbound_notification SET
    status = 'sent',
    updated_at = now(),
    last_error = NULL
WHERE id = $1
`
	for _, id := range ids {
		if _, err := s.db.Exec(ctx, q, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *DBNotificationStore) ScheduleRetry(ctx context.Context, ids []pgtype.UUID, lastError string, backoff time.Duration) error {
	const q = `
UPDATE channel_outbound_notification SET
    status = 'pending',
    attempts = attempts + 1,
    next_attempt_at = now() + $2::interval,
    updated_at = now(),
    last_error = $3
WHERE id = $1
`
	for _, id := range ids {
		if _, err := s.db.Exec(ctx, q, id, pgInterval(backoff), truncateError(lastError)); err != nil {
			return err
		}
	}
	return nil
}

func (s *DBNotificationStore) MarkDead(ctx context.Context, ids []pgtype.UUID, lastError string) error {
	const q = `
UPDATE channel_outbound_notification SET
    status = 'dead',
    updated_at = now(),
    last_error = $2
WHERE id = $1
`
	for _, id := range ids {
		if _, err := s.db.Exec(ctx, q, id, truncateError(lastError)); err != nil {
			return err
		}
	}
	return nil
}

func (s *DBNotificationStore) Cleanup(ctx context.Context) error {
	const q = `
DELETE FROM channel_outbound_notification
WHERE status IN ('sent', 'dead')
  AND updated_at < now() - interval '7 days'
`
	_, err := s.db.Exec(ctx, q)
	return err
}

type OutboxWorker struct {
	store  NotificationStore
	sender RetrySender
	active func() bool
}

type NotificationStore interface {
	ClaimDue(ctx context.Context, limit int32) ([]OutboxNotification, error)
	ReclaimStaleProcessing(ctx context.Context, limit int32, staleAfter time.Duration) ([]OutboxNotification, error)
	MarkSent(ctx context.Context, ids []pgtype.UUID) error
	ScheduleRetry(ctx context.Context, ids []pgtype.UUID, lastError string, backoff time.Duration) error
	MarkDead(ctx context.Context, ids []pgtype.UUID, lastError string) error
	Cleanup(ctx context.Context) error
}

func NewOutboxWorker(store NotificationStore, sender RetrySender) *OutboxWorker {
	return &OutboxWorker{store: store, sender: sender}
}

func (w *OutboxWorker) SetActiveFunc(active func() bool) {
	w.active = active
}

func (w *OutboxWorker) isActive() bool {
	return w.active == nil || w.active()
}

func (w *OutboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(OutboxTickInterval)
	defer ticker.Stop()
	cleanupTicker := time.NewTicker(CleanupTickInterval)
	defer cleanupTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.isActive() {
				w.processBatch(ctx)
			}
		case <-cleanupTicker.C:
			if err := w.store.Cleanup(ctx); err != nil {
				slog.Error("outbox worker: cleanup failed", "error", err)
			}
		}
	}
}

func (w *OutboxWorker) processBatch(ctx context.Context) {
	reclaimed, err := w.store.ReclaimStaleProcessing(ctx, OutboxBatchSize, 5*time.Minute)
	if err != nil {
		channelmetrics.M.RecordOutboundOutbox("unknown", "reclaim_error", 1)
		slog.Error("outbox worker: reclaim stale processing failed", "error", err)
	}

	rows, err := w.store.ClaimDue(ctx, OutboxBatchSize)
	if err != nil {
		channelmetrics.M.RecordOutboundOutbox("unknown", "claim_error", 1)
		slog.Error("outbox worker: claim failed", "error", err)
		rows = nil
	}
	groups := groupNotifications(append(reclaimed, rows...))
	for _, g := range groups {
		w.processGroup(ctx, g)
	}
}

type notificationGroup struct {
	provider       string
	connectionID   string
	eventKind      string
	externalUserID string
	targetUserID   pgtype.UUID
	items          []OutboxNotification
}

func groupNotifications(rows []OutboxNotification) []notificationGroup {
	byKey := map[string]*notificationGroup{}
	for _, n := range rows {
		key := n.ConnectionID + "\x00" + n.EventKind + "\x00" + n.TargetExternalUserID + "\x00" + uuidStr(n.TargetUserID)
		g := byKey[key]
		if g == nil {
			g = &notificationGroup{
				provider:       n.Provider,
				connectionID:   n.ConnectionID,
				eventKind:      n.EventKind,
				externalUserID: n.TargetExternalUserID,
				targetUserID:   n.TargetUserID,
			}
			byKey[key] = g
		}
		g.items = append(g.items, n)
	}
	out := make([]notificationGroup, 0, len(byKey))
	for _, g := range byKey {
		out = append(out, *g)
	}
	return out
}

func (w *OutboxWorker) processGroup(ctx context.Context, g notificationGroup) {
	if len(g.items) == 0 {
		return
	}
	ids := make([]pgtype.UUID, 0, len(g.items))
	for _, item := range g.items {
		ids = append(ids, item.ID)
	}
	payload := RetryPayload{
		Title: fmt.Sprintf("你有 %d 条新通知", len(g.items)),
		Body:  buildOutboxBody(g.items),
	}
	if len(g.items) == 1 {
		payload.Title = g.items[0].Title
		payload.Body = g.items[0].Body
	}
	err := w.sender.SendCard(ctx, g.connectionID, g.externalUserID, payload)
	if err == nil {
		channelmetrics.M.RecordOutboundOutbox(g.provider, "sent", len(g.items))
		if markErr := w.store.MarkSent(ctx, ids); markErr != nil {
			slog.Error("outbox worker: mark sent failed", "error", markErr)
		}
		return
	}

	var retryIDs, deadIDs []pgtype.UUID
	for _, item := range g.items {
		if !IsRetryable(err) || item.Attempts >= item.MaxAttempts {
			deadIDs = append(deadIDs, item.ID)
		} else {
			retryIDs = append(retryIDs, item.ID)
		}
	}

	if len(retryIDs) > 0 {
		for _, id := range retryIDs {
			backoff := backoffForAttempt(int(itemAttemptsByID(g.items, id)))
			if retryErr := w.store.ScheduleRetry(ctx, []pgtype.UUID{id}, err.Error(), backoff); retryErr != nil {
				slog.Error("outbox worker: schedule retry failed", "error", retryErr)
			}
		}
		channelmetrics.M.RecordOutboundOutbox(g.provider, "scheduled", len(retryIDs))
	}
	if len(deadIDs) > 0 {
		if deadErr := w.store.MarkDead(ctx, deadIDs, err.Error()); deadErr != nil {
			slog.Error("outbox worker: mark dead failed", "error", deadErr)
		}
		channelmetrics.M.RecordOutboundOutbox(g.provider, "dead", len(deadIDs))
	}
}

func itemAttemptsByID(items []OutboxNotification, id pgtype.UUID) int32 {
	for _, item := range items {
		if item.ID == id {
			return item.Attempts
		}
	}
	return 0
}

func buildOutboxBody(items []OutboxNotification) string {
	var b strings.Builder
	for i, item := range items {
		if i > 0 {
			b.WriteByte('\n')
		}
		_, _ = fmt.Fprintf(&b, "[%d] %s", i+1, item.Title)
		if item.Body != "" {
			b.WriteString(": ")
			b.WriteString(item.Body)
		}
	}
	return b.String()
}

func truncateError(s string) string {
	if len(s) > 2000 {
		return s[:2000]
	}
	return s
}
