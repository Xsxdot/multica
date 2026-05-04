package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// --- Error classification tests (TC-risk-5) ---

func TestIsRetryable_True(t *testing.T) {
	err := WrapRetryable(errors.New("feishu 5xx: code=500"))
	if !IsRetryable(err) {
		t.Error("expected IsRetryable to return true for wrapped retryable error")
	}
}

func TestIsRetryable_False(t *testing.T) {
	err := errors.New("feishu 4xx: code=230002 user not found")
	if IsRetryable(err) {
		t.Error("expected IsRetryable to return false for unwrapped error")
	}
}

func TestIsRetryable_Nil(t *testing.T) {
	if IsRetryable(nil) {
		t.Error("expected IsRetryable to return false for nil")
	}
}

func TestWrapRetryable_Nil(t *testing.T) {
	if WrapRetryable(nil) != nil {
		t.Error("expected WrapRetryable(nil) to return nil")
	}
}

func TestRetryableError_Unwrap(t *testing.T) {
	inner := errors.New("connection refused")
	wrapped := WrapRetryable(inner)
	if !errors.Is(wrapped, inner) {
		t.Error("expected wrapped error to unwrap to inner")
	}
}

// --- Backoff schedule tests (TC-risk-5: 30s/2min/10min) ---

func TestBackoffForAttempt(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 30 * time.Second},
		{1, 2 * time.Minute},
		{2, 10 * time.Minute},
		{3, 10 * time.Minute}, // default → same as attempt 2
		{99, 10 * time.Minute},
	}
	for _, tt := range tests {
		got := backoffForAttempt(tt.attempt)
		if got != tt.want {
			t.Errorf("backoffForAttempt(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

// --- RetryPayload tests ---

func TestRetryPayload_MarshalRoundtrip(t *testing.T) {
	payload := RetryPayload{
		ExternalUserID: "ou_abc123",
		Title:          "Test Title",
		Body:           "Test Body",
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded RetryPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ExternalUserID != payload.ExternalUserID {
		t.Errorf("ExternalUserID = %q, want %q", decoded.ExternalUserID, payload.ExternalUserID)
	}
	if decoded.Title != payload.Title {
		t.Errorf("Title = %q, want %q", decoded.Title, payload.Title)
	}
	if decoded.Body != payload.Body {
		t.Errorf("Body = %q, want %q", decoded.Body, payload.Body)
	}
}

// --- Mock RetrySender ---

type mockRetrySender struct {
	calls []mockRetryCall
	err   error // error to return from SendCard
}

type mockRetryCall struct {
	Provider       string
	ExternalUserID string
	Payload        RetryPayload
}

func (m *mockRetrySender) SendCard(_ context.Context, provider string, externalUserID string, card RetryPayload) error {
	m.calls = append(m.calls, mockRetryCall{
		Provider:       provider,
		ExternalUserID: externalUserID,
		Payload:        card,
	})
	return m.err
}

// --- Mock FailureStore ---

type mockFailureStore struct {
	claimed        []db.ChannelOutboundFailure
	claimErr       error
	incrementCalls []db.IncrementOutboundFailureAttemptsParams
	markDeadCalls  []db.MarkOutboundFailureDeadParams
	deleteCalls    []pgtype.UUID
	incrementErr   error
	markDeadErr    error
	deleteErr      error
}

func (m *mockFailureStore) ClaimPendingOutboundFailures(_ context.Context, _ int32) ([]db.ChannelOutboundFailure, error) {
	if m.claimErr != nil {
		return nil, m.claimErr
	}
	result := m.claimed
	m.claimed = nil // one-shot
	return result, nil
}

func (m *mockFailureStore) IncrementOutboundFailureAttempts(_ context.Context, arg db.IncrementOutboundFailureAttemptsParams) (db.ChannelOutboundFailure, error) {
	m.incrementCalls = append(m.incrementCalls, arg)
	return db.ChannelOutboundFailure{}, m.incrementErr
}

func (m *mockFailureStore) MarkOutboundFailureDead(_ context.Context, arg db.MarkOutboundFailureDeadParams) (db.ChannelOutboundFailure, error) {
	m.markDeadCalls = append(m.markDeadCalls, arg)
	return db.ChannelOutboundFailure{}, m.markDeadErr
}

func (m *mockFailureStore) DeleteOutboundFailure(_ context.Context, id pgtype.UUID) error {
	m.deleteCalls = append(m.deleteCalls, id)
	return m.deleteErr
}

// --- Helper to build test failure records ---

func makeFailure(id [16]byte, attempts int32, maxAttempts int32, provider string, externalUserID string) db.ChannelOutboundFailure {
	payload, _ := json.Marshal(RetryPayload{
		ExternalUserID: externalUserID,
		Title:          "Test Title",
		Body:           "Test Body",
	})
	return db.ChannelOutboundFailure{
		ID:                   pgtype.UUID{Bytes: id, Valid: true},
		Provider:             provider,
		TargetExternalUserID: pgtype.Text{String: externalUserID, Valid: true},
		Payload:              payload,
		Status:               "pending",
		Attempts:             attempts,
		MaxAttempts:          maxAttempts,
	}
}

var testID1 = [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

// --- processOne tests (TC-out-4, TC-out-5, TC-risk-5) ---

// TC-out-4: retryable error → IncrementAttempts with correct backoff
func TestProcessOne_RetryableError_IncrementsWithBackoff(t *testing.T) {
	tests := []struct {
		name        string
		attempts    int32
		maxAttempts int32
		wantBackoff time.Duration
	}{
		{
			name:        "attempt 0 → 30s backoff",
			attempts:    0,
			maxAttempts: 3,
			wantBackoff: 30 * time.Second,
		},
		{
			name:        "attempt 1 → 2min backoff",
			attempts:    1,
			maxAttempts: 3,
			wantBackoff: 2 * time.Minute,
		},
		{
			name:        "attempt 2 → 10min backoff",
			attempts:    2,
			maxAttempts: 4,
			wantBackoff: 10 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &mockFailureStore{}
			sender := &mockRetrySender{err: WrapRetryable(errors.New("5xx timeout"))}
			worker := NewRetryWorkerWithStore(store, sender)

			f := makeFailure(testID1, tt.attempts, tt.maxAttempts, "feishu", "ou_user1")
			worker.processOne(context.Background(), f)

			if len(store.incrementCalls) != 1 {
				t.Fatalf("expected 1 IncrementAttempts call, got %d", len(store.incrementCalls))
			}
			gotBackoff := time.Duration(store.incrementCalls[0].Column3.Microseconds) * time.Microsecond
			if gotBackoff != tt.wantBackoff {
				t.Errorf("backoff = %v, want %v", gotBackoff, tt.wantBackoff)
			}
			if len(store.markDeadCalls) != 0 {
				t.Errorf("expected 0 MarkDead calls, got %d", len(store.markDeadCalls))
			}
			if len(store.deleteCalls) != 0 {
				t.Errorf("expected 0 Delete calls, got %d", len(store.deleteCalls))
			}
		})
	}
}

// TC-out-5: non-retryable error (e.g. 230002) → mark dead immediately
func TestProcessOne_NonRetryableError_MarksDeadImmediately(t *testing.T) {
	store := &mockFailureStore{}
	sender := &mockRetrySender{err: errors.New("230002 user not found")}
	worker := NewRetryWorkerWithStore(store, sender)

	f := makeFailure(testID1, 0, 3, "feishu", "ou_user1")
	worker.processOne(context.Background(), f)

	if len(store.markDeadCalls) != 1 {
		t.Fatalf("expected 1 MarkDead call, got %d", len(store.markDeadCalls))
	}
	if store.markDeadCalls[0].LastError.String != "non-retryable: 230002 user not found" {
		t.Errorf("markDead reason = %q, want %q", store.markDeadCalls[0].LastError.String, "non-retryable: 230002 user not found")
	}
	if len(store.incrementCalls) != 0 {
		t.Errorf("expected 0 IncrementAttempts calls, got %d", len(store.incrementCalls))
	}
	if len(store.deleteCalls) != 0 {
		t.Errorf("expected 0 Delete calls, got %d", len(store.deleteCalls))
	}
}

// TC-risk-5: max attempts reached → mark dead
func TestProcessOne_MaxAttemptsReached_MarksDead(t *testing.T) {
	store := &mockFailureStore{}
	sender := &mockRetrySender{err: WrapRetryable(errors.New("5xx timeout"))}
	worker := NewRetryWorkerWithStore(store, sender)

	// attempts=2, maxAttempts=3 → nextAttempt=3 >= 3 → dead
	f := makeFailure(testID1, 2, 3, "feishu", "ou_user1")
	worker.processOne(context.Background(), f)

	if len(store.markDeadCalls) != 1 {
		t.Fatalf("expected 1 MarkDead call, got %d", len(store.markDeadCalls))
	}
	if store.markDeadCalls[0].LastError.String != "max attempts (3) exhausted: 5xx timeout" {
		t.Errorf("markDead reason = %q", store.markDeadCalls[0].LastError.String)
	}
	if len(store.incrementCalls) != 0 {
		t.Errorf("expected 0 IncrementAttempts calls, got %d", len(store.incrementCalls))
	}
}

// Success path → DELETE (not markDead)
func TestProcessOne_Success_DeletesRecord(t *testing.T) {
	store := &mockFailureStore{}
	sender := &mockRetrySender{err: nil} // success
	worker := NewRetryWorkerWithStore(store, sender)

	f := makeFailure(testID1, 1, 3, "feishu", "ou_user1")
	worker.processOne(context.Background(), f)

	if len(store.deleteCalls) != 1 {
		t.Fatalf("expected 1 Delete call, got %d", len(store.deleteCalls))
	}
	if store.deleteCalls[0] != f.ID {
		t.Errorf("deleted ID = %v, want %v", store.deleteCalls[0], f.ID)
	}
	if len(store.markDeadCalls) != 0 {
		t.Errorf("expected 0 MarkDead calls, got %d", len(store.markDeadCalls))
	}
	if len(store.incrementCalls) != 0 {
		t.Errorf("expected 0 IncrementAttempts calls, got %d", len(store.incrementCalls))
	}
}

// Bad payload → mark dead
func TestProcessOne_BadPayload_MarksDead(t *testing.T) {
	store := &mockFailureStore{}
	sender := &mockRetrySender{}
	worker := NewRetryWorkerWithStore(store, sender)

	f := db.ChannelOutboundFailure{
		ID:                   pgtype.UUID{Bytes: testID1, Valid: true},
		Provider:             "feishu",
		TargetExternalUserID: pgtype.Text{String: "ou_user1", Valid: true},
		Payload:              []byte("not-json"),
		Status:               "pending",
		Attempts:             0,
		MaxAttempts:          3,
	}
	worker.processOne(context.Background(), f)

	if len(store.markDeadCalls) != 1 {
		t.Fatalf("expected 1 MarkDead call, got %d", len(store.markDeadCalls))
	}
	if store.markDeadCalls[0].LastError.String != "unmarshal payload: invalid character 'o' in literal null (expecting 'u')" {
		t.Errorf("markDead reason = %q", store.markDeadCalls[0].LastError.String)
	}
}

// Missing external user ID → mark dead
func TestProcessOne_NoExternalUserID_MarksDead(t *testing.T) {
	store := &mockFailureStore{}
	sender := &mockRetrySender{}
	worker := NewRetryWorkerWithStore(store, sender)

	f := db.ChannelOutboundFailure{
		ID:                   pgtype.UUID{Bytes: testID1, Valid: true},
		Provider:             "feishu",
		TargetExternalUserID: pgtype.Text{String: "", Valid: false},
		Payload:              []byte(`{"external_user_id":"","title":"t","body":"b"}`),
		Status:               "pending",
		Attempts:             0,
		MaxAttempts:          3,
	}
	worker.processOne(context.Background(), f)

	if len(store.markDeadCalls) != 1 {
		t.Fatalf("expected 1 MarkDead call, got %d", len(store.markDeadCalls))
	}
	if store.markDeadCalls[0].LastError.String != "no external_user_id in failure record or payload" {
		t.Errorf("markDead reason = %q", store.markDeadCalls[0].LastError.String)
	}
}

// --- Helper function tests ---

func TestPgText(t *testing.T) {
	pt := pgText("hello")
	if !pt.Valid || pt.String != "hello" {
		t.Errorf("pgText(\"hello\") = %+v, want {String:hello, Valid:true}", pt)
	}
}

func TestPgInterval(t *testing.T) {
	pi := pgInterval(30 * time.Second)
	if !pi.Valid {
		t.Error("expected Valid=true")
	}
	expected := int64(30 * time.Second.Microseconds())
	if pi.Microseconds != expected {
		t.Errorf("Microseconds = %d, want %d", pi.Microseconds, expected)
	}
}

func TestUUIDStr_Nil(t *testing.T) {
	var u pgtype.UUID // zero-value, Valid=false
	result := uuidStr(u)
	if result != "<nil>" {
		t.Errorf("uuidStr(nil UUID) = %q, want \"<nil>\"", result)
	}
}

func TestUUIDStr_Valid(t *testing.T) {
	u := pgtype.UUID{}
	copy(u.Bytes[:], []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	u.Valid = true
	result := uuidStr(u)
	if result == "<nil>" || result == "" {
		t.Errorf("uuidStr(valid UUID) = %q, want non-empty hex string", result)
	}
}
