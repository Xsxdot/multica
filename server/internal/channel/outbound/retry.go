package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const (
	// RetryBatchSize is the max number of failures claimed per tick.
	RetryBatchSize int32 = 50

	// RetryTickInterval is how often the worker polls for pending failures.
	RetryTickInterval = 10 * time.Second

	// Backoff schedule: attempt 0 → 30s, attempt 1 → 2min, attempt 2 → 10min.
	backoff0 = 30 * time.Second
	backoff1 = 2 * time.Minute
	backoff2 = 10 * time.Minute
)

// RetryableError is the sentinel the retry worker uses to classify an error
// as worth retrying. Channel adapters should wrap transient failures
// (5xx, network timeout) in RetryableError so the worker re-enqueues them
// with backoff. Non-wrapped errors are treated as permanent and the failure
// record is marked 'dead' immediately.
type RetryableError struct{ Inner error }

func (e *RetryableError) Error() string { return e.Inner.Error() }
func (e *RetryableError) Unwrap() error { return e.Inner }

// WrapRetryable marks an error as retryable.
func WrapRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Inner: err}
}

// IsRetryable reports whether an error is retryable.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var re *RetryableError
	return errors.As(err, &re)
}

// RetryPayload is the JSON structure stored in channel_outbound_failure.payload.
// It captures enough information to reconstruct the original send call.
type RetryPayload struct {
	ExternalUserID string `json:"external_user_id"`
	Title          string `json:"title"`
	Body           string `json:"body"`
}

// RetrySender abstracts the channel send operation so the worker can be
// tested without a real Feishu adapter. The provider parameter identifies
// which channel adapter to use (e.g. "feishu").
type RetrySender interface {
	SendCard(ctx context.Context, provider string, externalUserID string, card RetryPayload) error
}

// FailureStore abstracts the DB operations needed by RetryWorker.
// *db.Queries satisfies this interface.
type FailureStore interface {
	ClaimPendingOutboundFailures(ctx context.Context, limit int32) ([]db.ChannelOutboundFailure, error)
	IncrementOutboundFailureAttempts(ctx context.Context, arg db.IncrementOutboundFailureAttemptsParams) (db.ChannelOutboundFailure, error)
	MarkOutboundFailureDead(ctx context.Context, arg db.MarkOutboundFailureDeadParams) (db.ChannelOutboundFailure, error)
	DeleteOutboundFailure(ctx context.Context, id pgtype.UUID) error
}

// RetryWorker polls channel_outbound_failure for pending rows and retries
// them. It distinguishes retryable (5xx / network) from non-retryable
// errors: retryable failures get exponential backoff (30s → 2m → 10min)
// and non-retryable failures are marked 'dead' immediately.
//
// Concurrency: the worker runs a single goroutine; multiple instances
// across replicas use atomic UPDATE ... FOR UPDATE SKIP LOCKED in the
// claim query to avoid contention.
type RetryWorker struct {
	store  FailureStore
	sender RetrySender
}

// NewRetryWorker creates a RetryWorker. Call Run to start it.
func NewRetryWorker(pool *pgxpool.Pool, queries *db.Queries, sender RetrySender) *RetryWorker {
	return &RetryWorker{
		store:  queries,
		sender: sender,
	}
}

// NewRetryWorkerWithStore creates a RetryWorker with a custom FailureStore
// (for testing).
func NewRetryWorkerWithStore(store FailureStore, sender RetrySender) *RetryWorker {
	return &RetryWorker{
		store:  store,
		sender: sender,
	}
}

// RetryWorkerEnabled reports whether the retry worker should start.
// Controlled by CHANNEL_RETRY_WORKER_ENABLED env var (default: false).
// Must be explicitly enabled after the real RetrySender is wired.
func RetryWorkerEnabled() bool {
	return os.Getenv("CHANNEL_RETRY_WORKER_ENABLED") == "true"
}

// Run starts the retry loop. It blocks until ctx is cancelled.
func (w *RetryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(RetryTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

// processBatch claims and processes a batch of pending failures.
func (w *RetryWorker) processBatch(ctx context.Context) {
	failures, err := w.store.ClaimPendingOutboundFailures(ctx, RetryBatchSize)
	if err != nil {
		slog.Error("retry worker: claim failed", "error", err)
		return
	}

	for _, f := range failures {
		w.processOne(ctx, f)
	}
}

// processOne handles a single failure record.
func (w *RetryWorker) processOne(ctx context.Context, f db.ChannelOutboundFailure) {
	var payload RetryPayload
	if err := json.Unmarshal(f.Payload, &payload); err != nil {
		slog.Error("retry worker: bad payload, marking dead",
			"id", uuidStr(f.ID), "error", err)
		w.markDead(ctx, f, fmt.Sprintf("unmarshal payload: %s", err))
		return
	}

	// Use target_external_user_id column as single source of truth.
	// If empty, fall back to payload (legacy records).
	externalUserID := f.TargetExternalUserID.String
	if externalUserID == "" {
		externalUserID = payload.ExternalUserID
	}
	if externalUserID == "" {
		slog.Error("retry worker: no external user id, marking dead",
			"id", uuidStr(f.ID))
		w.markDead(ctx, f, "no external_user_id in failure record or payload")
		return
	}

	err := w.sender.SendCard(ctx, f.Provider, externalUserID, payload)

	if err == nil {
		// Success — delete the failure record entirely.
		slog.Info("retry worker: send succeeded, deleting record",
			"id", uuidStr(f.ID), "provider", f.Provider, "attempts", f.Attempts)
		w.deleteRecord(ctx, f.ID)
		return
	}

	// Classify the error.
	if IsRetryable(err) {
		// Retryable — increment attempts with backoff.
		// Use f.Attempts (current) as the backoff index: attempt 0 → 30s,
		// attempt 1 → 2min, attempt 2 → 10min.
		nextAttempt := int(f.Attempts) + 1
		if nextAttempt >= int(f.MaxAttempts) {
			slog.Warn("retry worker: max attempts reached, marking dead",
				"id", uuidStr(f.ID), "attempts", nextAttempt, "error", err)
			w.markDead(ctx, f, fmt.Sprintf("max attempts (%d) exhausted: %s", f.MaxAttempts, err))
			return
		}

		backoff := backoffForAttempt(int(f.Attempts))
		_, updateErr := w.store.IncrementOutboundFailureAttempts(ctx, db.IncrementOutboundFailureAttemptsParams{
			ID:        f.ID,
			LastError: pgText(err.Error()),
			Column3:   pgInterval(backoff),
		})
		if updateErr != nil {
			slog.Error("retry worker: increment attempts failed",
				"id", uuidStr(f.ID), "error", updateErr)
		} else {
			slog.Info("retry worker: scheduled retry",
				"id", uuidStr(f.ID), "next_attempt", nextAttempt, "backoff", backoff, "error", err)
		}
		return
	}

	// Non-retryable — mark dead immediately.
	slog.Warn("retry worker: non-retryable error, marking dead",
		"id", uuidStr(f.ID), "error", err)
	w.markDead(ctx, f, fmt.Sprintf("non-retryable: %s", err))
}

// markDead sets the failure status to 'dead'.
func (w *RetryWorker) markDead(ctx context.Context, f db.ChannelOutboundFailure, reason string) {
	_, err := w.store.MarkOutboundFailureDead(ctx, db.MarkOutboundFailureDeadParams{
		ID:        f.ID,
		LastError: pgText(reason),
	})
	if err != nil {
		slog.Error("retry worker: mark dead failed", "id", uuidStr(f.ID), "error", err)
	}
}

// deleteRecord removes a failure record (used on successful retry).
func (w *RetryWorker) deleteRecord(ctx context.Context, id pgtype.UUID) {
	if err := w.store.DeleteOutboundFailure(ctx, id); err != nil {
		slog.Error("retry worker: delete failed", "id", uuidStr(id), "error", err)
	}
}

// backoffForAttempt returns the backoff duration for the given attempt number.
// Attempt 0 → 30s, 1 → 2min, 2 → 10min. Default is 10min.
func backoffForAttempt(attempt int) time.Duration {
	switch attempt {
	case 0:
		return backoff0
	case 1:
		return backoff1
	case 2:
		return backoff2
	default:
		return backoff2
	}
}

func uuidStr(u pgtype.UUID) string {
	if !u.Valid {
		return "<nil>"
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", u.Bytes[0:4], u.Bytes[4:6], u.Bytes[6:8], u.Bytes[8:10], u.Bytes[10:16])
}

func pgText(s string) pgtype.Text {
	return pgtype.Text{String: s, Valid: true}
}

func pgInterval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{
		Microseconds: d.Microseconds(),
		Valid:        true,
	}
}
