package inbound

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/channel/port"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// DedupStore is the narrow persistence contract the dedup Step depends
// on. Production wires the adapter returned by NewDBDedupStore (which
// translates the sqlc-generated *db.Queries shape into this contract);
// tests pass a fake satisfying DedupStore directly. Keeping the
// interface narrow — strings in, bool out — keeps the dedup Step
// itself free of pgx / sqlc types and mirrors the pattern used by
// binding.TokenStore (T4).
//
// TryRecordInboundEvent attempts to insert (provider, eventID) into the
// dedup table. It returns inserted=true when the row is newly written
// (the event is being seen for the first time) and inserted=false when
// the row already existed (the platform replayed an event we have
// already processed). The implementation uses INSERT ... ON CONFLICT
// DO NOTHING — the canonical PostgreSQL idiom for "did we just write
// this row?".
type DedupStore interface {
	TryRecordInboundEvent(ctx context.Context, provider, eventID string) (bool, error)
}

// dedupStep is the Step implementation that consults DedupStore on every
// event. It is unexported because callers must construct it via
// NewDedupStep so the *Step interface return type stays stable across
// future refactors.
type dedupStep struct {
	store DedupStore
}

// NewDedupStep returns a Step that consults store on each invocation
// and short-circuits the pipeline when a duplicate is observed. The
// Step uses the event's ChannelName as the dedup table's `provider`
// column and EventID as the dedup key — both fields are populated by
// the adapter layer (T5) during normalisation, so by the time an
// event reaches this Step it is guaranteed to carry both.
func NewDedupStep(store DedupStore) Step {
	return &dedupStep{store: store}
}

// Name returns the stable telemetry label for this Step.
func (s *dedupStep) Name() string { return "dedup" }

// Run records the (provider, eventID) pair. On a fresh insertion the
// pipeline continues; on a collision it Skips so downstream Steps
// (identity-bind, intent-recog, dispatch, reply) do not re-process a
// replayed event.
func (s *dedupStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	inserted, err := s.store.TryRecordInboundEvent(ctx, evt.ChannelName, evt.EventID)
	if err != nil {
		return evt, DecisionContinue, err
	}
	if !inserted {
		return evt, DecisionSkip, nil
	}
	return evt, DecisionContinue, nil
}

// dbQueriesTryRecorder is the slice of *db.Queries the production
// adapter actually consumes. Defining the seam as an interface lets
// the adapter be wired against either the real *db.Queries or a
// tx-bound *db.Queries (e.g. db.New(tx)) without dragging the whole
// Queries surface in.
type dbQueriesTryRecorder interface {
	TryRecordInboundEvent(ctx context.Context, arg db.TryRecordInboundEventParams) (pgtype.Timestamptz, error)
}

// dbDedupStore adapts the sqlc-generated *db.Queries shape (params
// struct in, timestamptz out, pgx.ErrNoRows on conflict) to the narrow
// DedupStore contract the Step expects. The translation lives at the
// dao boundary so the rest of the inbound package can stay free of
// pgx imports.
type dbDedupStore struct {
	q dbQueriesTryRecorder
}

// NewDBDedupStore wires the production DedupStore against the
// sqlc-generated Queries. Callers in cmd/server/main.go (or the T7+
// wiring code) pass *db.Queries; tests pass a fake satisfying
// DedupStore directly without going through this adapter.
func NewDBDedupStore(q dbQueriesTryRecorder) DedupStore {
	return &dbDedupStore{q: q}
}

// TryRecordInboundEvent translates between the sqlc :one return shape
// and the bool the Step contract uses. There are exactly three branches
// the underlying query can take, mapped here as follows:
//
//   - Insert succeeded (the row was new). sqlc returns the
//     processed_at timestamptz from RETURNING + a nil error. We
//     translate to (true, nil).
//
//   - ON CONFLICT DO NOTHING fired (the row already existed, i.e. the
//     platform replayed an event we have processed before). sqlc's
//     :one wrapper sees zero rows from RETURNING and surfaces it as
//     pgx.ErrNoRows. We translate to (false, nil) — the caller's
//     "duplicate, skip the rest of the pipeline" path.
//
//   - Anything else (driver / network / constraint failures). We
//     surface the underlying error so the dedup Step can propagate
//     it and the pipeline can abort, rather than silently dropping
//     the event.
//
// The pgx.ErrNoRows handling is the entire reason this adapter
// exists: keeping it confined here means the dedup Step itself
// (and every other file in the inbound package) stays free of pgx
// imports.
func (s *dbDedupStore) TryRecordInboundEvent(ctx context.Context, provider, eventID string) (bool, error) {
	_, err := s.q.TryRecordInboundEvent(ctx, db.TryRecordInboundEventParams{
		Provider: provider,
		EventID:  eventID,
	})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return false, err
}

// Compile-time interface conformance: a clear compile-time error here
// is friendlier than a confusing one at every call site if a method
// signature drifts.
var (
	_ Step       = (*dedupStep)(nil)
	_ DedupStore = (*dbDedupStore)(nil)
)
