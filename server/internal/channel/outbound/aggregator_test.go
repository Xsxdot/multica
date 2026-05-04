package outbound

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/channel/port"
)

// --- Mock sender ---

type mockSender struct {
	mu      sync.Mutex
	calls   []sendCall
	callSeq int
}

type sendCall struct {
	ExternalUserID string
	Card           port.OutboundCardMessage
}

func (m *mockSender) SendCard(externalUserID string, card port.OutboundCardMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callSeq++
	m.calls = append(m.calls, sendCall{ExternalUserID: externalUserID, Card: card})
}

func (m *mockSender) Calls() []sendCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]sendCall, len(m.calls))
	copy(cp, m.calls)
	return cp
}

// --- Tests ---

// TC-out-aggregator-a: 60s window merges 5 notifications into 1 card
// "你有 5 条新通知"
func TestAggregator_Merge(t *testing.T) {
	sender := &mockSender{}
	agg := NewAggregator(sender, 200*time.Millisecond)
	defer agg.Stop()

	userID := "ext-user-1"
	for i := 0; i < 5; i++ {
		agg.Add(userID, port.OutboundCardMessage{
			ChatID: userID,
			Title:  fmt.Sprintf("Issue %d", i+1),
			Body:   fmt.Sprintf("Body %d", i+1),
		}, false)
	}

	// Should not have flushed yet (within window)
	if len(sender.Calls()) != 0 {
		t.Fatalf("expected 0 sends before flush, got %d", len(sender.Calls()))
	}

	// Wait for flush
	time.Sleep(400 * time.Millisecond)

	calls := sender.Calls()
	if len(calls) != 1 {
		t.Fatalf("TC-a: expected 1 merged card, got %d", len(calls))
	}
	if calls[0].Card.Title != "你有 5 条新通知" {
		t.Errorf("TC-a: expected title '你有 5 条新通知', got %q", calls[0].Card.Title)
	}
	if calls[0].ExternalUserID != userID {
		t.Errorf("TC-a: expected ExternalUserID %q, got %q", userID, calls[0].ExternalUserID)
	}
}

// TC-out-aggregator-b: buffer limit 50 — 51st notification triggers immediate flush
// mock Sender receives 2 calls (flush at 50 + immediate send for 51st)
func TestAggregator_BufferLimit(t *testing.T) {
	sender := &mockSender{}
	agg := NewAggregator(sender, 24*time.Hour) // very long window to prevent timer flush
	defer agg.Stop()

	userID := "ext-user-limit"
	for i := 0; i < 51; i++ {
		agg.Add(userID, port.OutboundCardMessage{
			ChatID: userID,
			Title:  fmt.Sprintf("Issue %d", i+1),
			Body:   fmt.Sprintf("Body %d", i+1),
		}, false)
	}

	calls := sender.Calls()
	if len(calls) != 2 {
		t.Fatalf("TC-b: expected 2 send calls (flush at 50 + immediate for 51st), got %d", len(calls))
	}
	// First call: merged 50 notifications
	if calls[0].Card.Title != "你有 50 条新通知" {
		t.Errorf("TC-b: first call title = %q, want '你有 50 条新通知'", calls[0].Card.Title)
	}
	// Second call: single notification (51st, immediate)
	if calls[1].Card.Title != "Issue 51" {
		t.Errorf("TC-b: second call title = %q, want 'Issue 51'", calls[1].Card.Title)
	}
}

// TC-out-aggregator-c: urgent bypass — bypass_aggregation=true sends immediately (≤100ms)
func TestAggregator_UrgentBypass(t *testing.T) {
	sender := &mockSender{}
	agg := NewAggregator(sender, 24*time.Hour) // very long window
	defer agg.Stop()

	userID := "ext-user-urgent"
	start := time.Now()
	agg.Add(userID, port.OutboundCardMessage{
		ChatID: userID,
		Title:  "P0 Alert",
		Body:   "Critical incident",
	}, true)
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("TC-c: urgent send took %v, must be ≤100ms", elapsed)
	}

	calls := sender.Calls()
	if len(calls) != 1 {
		t.Fatalf("TC-c: expected 1 immediate send, got %d", len(calls))
	}
	if calls[0].Card.Title != "P0 Alert" {
		t.Errorf("TC-c: expected title 'P0 Alert', got %q", calls[0].Card.Title)
	}
}

// TC-out-aggregator-d: Stop() drops unflushed buffer (no failure queue)
func TestAggregator_StopDropsBuffer(t *testing.T) {
	sender := &mockSender{}
	agg := NewAggregator(sender, 24*time.Hour) // very long window

	userID := "ext-user-stop"
	for i := 0; i < 3; i++ {
		agg.Add(userID, port.OutboundCardMessage{
			ChatID: userID,
			Title:  fmt.Sprintf("Issue %d", i+1),
			Body:   fmt.Sprintf("Body %d", i+1),
		}, false)
	}

	// Stop without waiting for flush
	agg.Stop()

	// Give a moment to ensure no background flush happens
	time.Sleep(100 * time.Millisecond)

	calls := sender.Calls()
	if len(calls) != 0 {
		t.Errorf("TC-d: expected 0 sends after Stop(), got %d", len(calls))
	}
}

// Test: multiple users aggregated independently
func TestAggregator_MultiUser(t *testing.T) {
	sender := &mockSender{}
	agg := NewAggregator(sender, 200*time.Millisecond)
	defer agg.Stop()

	agg.Add("user-A", port.OutboundCardMessage{ChatID: "user-A", Title: "A1"}, false)
	agg.Add("user-B", port.OutboundCardMessage{ChatID: "user-B", Title: "B1"}, false)
	agg.Add("user-A", port.OutboundCardMessage{ChatID: "user-A", Title: "A2"}, false)

	time.Sleep(400 * time.Millisecond)

	calls := sender.Calls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 merged cards (one per user), got %d", len(calls))
	}

	// Find the calls by user
	var callA, callB *sendCall
	for i := range calls {
		switch calls[i].ExternalUserID {
		case "user-A":
			callA = &calls[i]
		case "user-B":
			callB = &calls[i]
		}
	}
	if callA == nil || callB == nil {
		t.Fatalf("expected calls for user-A and user-B, got %v", calls)
	}
	if callA.Card.Title != "你有 2 条新通知" {
		t.Errorf("user-A title = %q, want '你有 2 条新通知'", callA.Card.Title)
	}
	if callB.Card.Title != "你有 1 条新通知" {
		t.Errorf("user-B title = %q, want '你有 1 条新通知'", callB.Card.Title)
	}
}

// Test: urgent bypass for one user doesn't affect another user's buffer
func TestAggregator_UrgentDoesNotAffectOtherBuffer(t *testing.T) {
	sender := &mockSender{}
	agg := NewAggregator(sender, 200*time.Millisecond)
	defer agg.Stop()

	agg.Add("user-A", port.OutboundCardMessage{ChatID: "user-A", Title: "A1"}, false)
	agg.Add("user-B", port.OutboundCardMessage{ChatID: "user-B", Title: "B1"}, true) // urgent

	// user-B should have been sent immediately
	if len(sender.Calls()) != 1 {
		t.Fatalf("expected 1 immediate send for user-B, got %d", len(sender.Calls()))
	}

	// Wait for user-A's buffer to flush
	time.Sleep(400 * time.Millisecond)

	calls := sender.Calls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 total sends, got %d", len(calls))
	}
}
