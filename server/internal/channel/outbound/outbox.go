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
	if strings.TrimSpace(req.Provider) == "" || strings.TrimSpace(req.EventKind) == "" ||
		strings.TrimSpace(req.TargetExternalUserID) == "" || !req.TargetUserID.Valid {
		return errors.New("outbox: invalid notification enqueue request")
	}
	window := s.window
	if window <= 0 {
		window = OutboxAggregationWindow
	}
	const q = `
INSERT INTO channel_outbound_notification (
    provider, event_kind, target_user_id, target_external_user_id,
    title, body, aggregation_due_at
) VALUES ($1, $2, $3, $4, $5, $6, now() + $7::interval)
`
	_, err := s.db.Exec(ctx, q,
		req.Provider,
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
RETURNING id, provider, event_kind, target_user_id, target_external_user_id,
          title, body, attempts, max_attempts
`
	rows, err := s.db.Query(ctx, q, limit)
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
	rows, err := w.store.ClaimDue(ctx, OutboxBatchSize)
	if err != nil {
		channelmetrics.M.RecordOutboundOutbox("unknown", "claim_error", 1)
		slog.Error("outbox worker: claim failed", "error", err)
		return
	}
	groups := groupNotifications(rows)
	for _, g := range groups {
		w.processGroup(ctx, g)
	}
}

type notificationGroup struct {
	provider       string
	eventKind      string
	externalUserID string
	targetUserID   pgtype.UUID
	items          []OutboxNotification
}

func groupNotifications(rows []OutboxNotification) []notificationGroup {
	byKey := map[string]*notificationGroup{}
	for _, n := range rows {
		key := n.Provider + "\x00" + n.EventKind + "\x00" + n.TargetExternalUserID + "\x00" + uuidStr(n.TargetUserID)
		g := byKey[key]
		if g == nil {
			g = &notificationGroup{
				provider:       n.Provider,
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
	maxAttempts := int32(3)
	attempts := int32(0)
	for _, item := range g.items {
		ids = append(ids, item.ID)
		if item.MaxAttempts > maxAttempts {
			maxAttempts = item.MaxAttempts
		}
		if item.Attempts > attempts {
			attempts = item.Attempts
		}
	}
	payload := RetryPayload{
		Title: fmt.Sprintf("你有 %d 条新通知", len(g.items)),
		Body:  buildOutboxBody(g.items),
	}
	if len(g.items) == 1 {
		payload.Title = g.items[0].Title
		payload.Body = g.items[0].Body
	}
	err := w.sender.SendCard(ctx, g.provider, g.externalUserID, payload)
	if err == nil {
		channelmetrics.M.RecordOutboundOutbox(g.provider, "sent", len(g.items))
		if markErr := w.store.MarkSent(ctx, ids); markErr != nil {
			slog.Error("outbox worker: mark sent failed", "error", markErr)
		}
		return
	}
	if IsRetryable(err) && attempts < maxAttempts {
		backoff := backoffForAttempt(int(attempts))
		channelmetrics.M.RecordOutboundOutbox(g.provider, "scheduled", len(g.items))
		if retryErr := w.store.ScheduleRetry(ctx, ids, err.Error(), backoff); retryErr != nil {
			slog.Error("outbox worker: schedule retry failed", "error", retryErr)
		}
		return
	}
	channelmetrics.M.RecordOutboundOutbox(g.provider, "dead", len(g.items))
	if deadErr := w.store.MarkDead(ctx, ids, err.Error()); deadErr != nil {
		slog.Error("outbox worker: mark dead failed", "error", deadErr)
	}
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
