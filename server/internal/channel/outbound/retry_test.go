package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
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

// --- Mock RetrySender for processOne testing ---

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

// Ensure the context import is used.
var _ = context.Background
