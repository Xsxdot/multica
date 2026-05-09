package outbound

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type fakeNotificationStore struct {
	claimed   []OutboxNotification
	reclaimed []OutboxNotification
	sent      []pgtype.UUID
	retried   []pgtype.UUID
	dead      []pgtype.UUID
}

func (f *fakeNotificationStore) ClaimDue(context.Context, int32) ([]OutboxNotification, error) {
	return f.claimed, nil
}

func (f *fakeNotificationStore) ReclaimStaleProcessing(context.Context, int32, time.Duration) ([]OutboxNotification, error) {
	return f.reclaimed, nil
}

func (f *fakeNotificationStore) MarkSent(_ context.Context, ids []pgtype.UUID) error {
	f.sent = append(f.sent, ids...)
	return nil
}

func (f *fakeNotificationStore) ScheduleRetry(_ context.Context, ids []pgtype.UUID, _ string, _ time.Duration) error {
	f.retried = append(f.retried, ids...)
	return nil
}

func (f *fakeNotificationStore) MarkDead(_ context.Context, ids []pgtype.UUID, _ string) error {
	f.dead = append(f.dead, ids...)
	return nil
}

func (f *fakeNotificationStore) Cleanup(context.Context) error { return nil }

func TestOutboxWorker_MergesDueNotificationsAndMarksSent(t *testing.T) {
	t.Parallel()

	userID := pgtype.UUID{Bytes: [16]byte{0x01}, Valid: true}
	store := &fakeNotificationStore{claimed: []OutboxNotification{
		{ID: pgtype.UUID{Bytes: [16]byte{0x11}, Valid: true}, Provider: "feishu", EventKind: "issue_assigned", TargetUserID: userID, TargetExternalUserID: "ou_1", Title: "A", Body: "body A", MaxAttempts: 3},
		{ID: pgtype.UUID{Bytes: [16]byte{0x12}, Valid: true}, Provider: "feishu", EventKind: "issue_assigned", TargetUserID: userID, TargetExternalUserID: "ou_1", Title: "B", Body: "body B", MaxAttempts: 3},
	}}
	sender := &mockRetrySender{}
	worker := NewOutboxWorker(store, sender)

	worker.processBatch(context.Background())

	if len(sender.calls) != 1 {
		t.Fatalf("send calls = %d, want 1", len(sender.calls))
	}
	if sender.calls[0].Payload.Title != "你有 2 条新通知" {
		t.Fatalf("title = %q", sender.calls[0].Payload.Title)
	}
	if !strings.Contains(sender.calls[0].Payload.Body, "[1] A: body A") ||
		!strings.Contains(sender.calls[0].Payload.Body, "[2] B: body B") {
		t.Fatalf("merged body = %q", sender.calls[0].Payload.Body)
	}
	if len(store.sent) != 2 {
		t.Fatalf("sent ids = %d, want 2", len(store.sent))
	}
}

func TestOutboxWorker_RetryableFailureSchedulesRetry(t *testing.T) {
	t.Parallel()

	store := &fakeNotificationStore{claimed: []OutboxNotification{
		{ID: pgtype.UUID{Bytes: [16]byte{0x21}, Valid: true}, Provider: "feishu", EventKind: "issue_assigned", TargetUserID: pgtype.UUID{Bytes: [16]byte{0x01}, Valid: true}, TargetExternalUserID: "ou_1", Title: "A", MaxAttempts: 3},
	}}
	sender := &mockRetrySender{err: WrapRetryable(errors.New("temporary"))}
	worker := NewOutboxWorker(store, sender)

	worker.processBatch(context.Background())

	if len(store.retried) != 1 {
		t.Fatalf("retried ids = %d, want 1", len(store.retried))
	}
	if len(store.dead) != 0 {
		t.Fatalf("dead ids = %d, want 0", len(store.dead))
	}
}

func TestOutboxWorker_NonRetryableFailureMarksDead(t *testing.T) {
	t.Parallel()

	store := &fakeNotificationStore{claimed: []OutboxNotification{
		{ID: pgtype.UUID{Bytes: [16]byte{0x31}, Valid: true}, Provider: "feishu", EventKind: "issue_assigned", TargetUserID: pgtype.UUID{Bytes: [16]byte{0x01}, Valid: true}, TargetExternalUserID: "ou_1", Title: "A", MaxAttempts: 3},
	}}
	sender := &mockRetrySender{err: errors.New("bad request")}
	worker := NewOutboxWorker(store, sender)

	worker.processBatch(context.Background())

	if len(store.dead) != 1 {
		t.Fatalf("dead ids = %d, want 1", len(store.dead))
	}
	if len(store.retried) != 0 {
		t.Fatalf("retried ids = %d, want 0", len(store.retried))
	}
}

func TestOutboxWorker_ReclaimStaleProcessing(t *testing.T) {
	t.Parallel()

	userID := pgtype.UUID{Bytes: [16]byte{0x01}, Valid: true}
	reclaimed := []OutboxNotification{
		{ID: pgtype.UUID{Bytes: [16]byte{0x41}, Valid: true}, Provider: "feishu", EventKind: "issue_assigned", TargetUserID: userID, TargetExternalUserID: "ou_1", Title: "R", MaxAttempts: 3},
	}
	store := &fakeNotificationStore{reclaimed: reclaimed}
	sender := &mockRetrySender{}
	worker := NewOutboxWorker(store, sender)

	worker.processBatch(context.Background())

	if len(sender.calls) != 1 {
		t.Fatalf("send calls = %d, want 1", len(sender.calls))
	}
	if len(store.sent) != 1 {
		t.Fatalf("sent ids = %d, want 1", len(store.sent))
	}
}

func TestOutboxWorker_MixedAttemptsGroup_SplitsRetryAndDead(t *testing.T) {
	t.Parallel()

	userID := pgtype.UUID{Bytes: [16]byte{0x01}, Valid: true}
	store := &fakeNotificationStore{claimed: []OutboxNotification{
		{ID: pgtype.UUID{Bytes: [16]byte{0x51}, Valid: true}, Provider: "feishu", EventKind: "issue_assigned", TargetUserID: userID, TargetExternalUserID: "ou_1", Title: "Old", Attempts: 3, MaxAttempts: 3},
		{ID: pgtype.UUID{Bytes: [16]byte{0x52}, Valid: true}, Provider: "feishu", EventKind: "issue_assigned", TargetUserID: userID, TargetExternalUserID: "ou_1", Title: "New", Attempts: 0, MaxAttempts: 3},
	}}
	sender := &mockRetrySender{err: WrapRetryable(errors.New("temporary"))}
	worker := NewOutboxWorker(store, sender)

	worker.processBatch(context.Background())

	if len(store.dead) != 1 || store.dead[0].Bytes != [16]byte{0x51} {
		t.Fatalf("dead ids = %v, want [0x51]", store.dead)
	}
	if len(store.retried) != 1 || store.retried[0].Bytes != [16]byte{0x52} {
		t.Fatalf("retried ids = %v, want [0x52]", store.retried)
	}
}
