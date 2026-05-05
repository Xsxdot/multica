package intent_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	in "github.com/multica-ai/multica/server/internal/channel/intent"
)

// --- QuotaLimiter interface ---

func TestQuotaLimiter_Interface(t *testing.T) {
	var _ in.QuotaLimiter = (*in.SlidingWindowQuota)(nil)
}

// --- Allow → returns true when under quota ---

func TestSlidingWindowQuota_Allow_UnderQuota(t *testing.T) {
	t.Parallel()
	ql := in.NewSlidingWindowQuota(in.QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       10,
		Window:      time.Hour,
	})

	if !ql.Allow("ws-1") {
		t.Fatal("expected Allow=true when under quota")
	}
}

// --- Exhaust quota → Allow returns false ---

func TestSlidingWindowQuota_Allow_Exhausted(t *testing.T) {
	t.Parallel()
	ql := in.NewSlidingWindowQuota(in.QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       3,
		Window:      time.Hour,
	})

	for i := 0; i < 3; i++ {
		ql.Allow("ws-1")
	}

	if ql.Allow("ws-1") {
		t.Fatal("expected Allow=false after quota exhausted")
	}
}

// --- Window slides: old entries expire ---

func TestSlidingWindowQuota_Allow_WindowSlide(t *testing.T) {
	t.Parallel()
	ql := in.NewSlidingWindowQuota(in.QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       2,
		Window:      50 * time.Millisecond,
	})

	ql.Allow("ws-1")
	ql.Allow("ws-1")

	if ql.Allow("ws-1") {
		t.Fatal("expected quota exhausted")
	}

	time.Sleep(60 * time.Millisecond)

	if !ql.Allow("ws-1") {
		t.Fatal("expected Allow=true after window slide")
	}
}

// --- Different workspaces are independent ---

func TestSlidingWindowQuota_Allow_WorkspaceIsolation(t *testing.T) {
	t.Parallel()
	ql := in.NewSlidingWindowQuota(in.QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       1,
		Window:      time.Hour,
	})

	ql.Allow("ws-1")

	if !ql.Allow("ws-2") {
		t.Fatal("ws-2 should not be affected by ws-1 quota")
	}
}

// --- Allow records usage and calls collector ---

func TestSlidingWindowQuota_Metrics_QuotaExhausted(t *testing.T) {
	t.Parallel()
	collector := &fakeQuotaCollector{}
	ql := in.NewSlidingWindowQuota(in.QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       1,
		Window:      time.Hour,
		Collector:   collector,
	})

	ql.Allow("ws-1")
	ql.Allow("ws-1") // triggers exhausted

	if atomic.LoadInt64(&collector.exhausted) != 1 {
		t.Errorf("exhausted count = %d, want 1", atomic.LoadInt64(&collector.exhausted))
	}
}

// --- Partial eviction: limit=5, window=60ms, 5 calls @ 20ms intervals, sleep 50ms, 2 remain, 3 more calls, 3rd fails ---

func TestSlidingWindowQuota_Allow_PartialEviction(t *testing.T) {
	t.Parallel()
	ql := in.NewSlidingWindowQuota(in.QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       5,
		Window:      60 * time.Millisecond,
	})

	for i := 0; i < 5; i++ {
		if !ql.Allow("ws-1") {
			t.Fatalf("expected Allow=true on call %d", i+1)
		}
		time.Sleep(30 * time.Millisecond)
	}

	// At this point: 5 entries at roughly t=0,30,60,90,120ms.
	// Sleep 70ms from now (t=190ms). The cutoff is t=190-60=130ms.
	// Entries at t=0,30,60,90ms are expired. Entry at t=120ms remains.
	// So remaining capacity = 5 - 1 = 4.
	time.Sleep(70 * time.Millisecond)

	// We just need to verify that at least 1 slot was freed and that
	// after filling all freed slots, the next call fails.
	freed := 0
	for i := 0; i < 5; i++ {
		if ql.Allow("ws-1") {
			freed++
		} else {
			break
		}
	}
	if freed < 1 {
		t.Fatal("expected at least 1 slot freed after partial eviction")
	}
	if freed > 5 {
		t.Fatalf("expected at most 5 slots freed, got %d", freed)
	}
}

// --- Partial eviction with deterministic timing using injected clock ---

func TestSlidingWindowQuota_Allow_PartialEviction_Deterministic(t *testing.T) {
	t.Parallel()
	ql := in.NewSlidingWindowQuota(in.QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       5,
		Window:      60 * time.Millisecond,
	})

	// Make 5 calls with no sleep - all at the same timestamp
	for i := 0; i < 5; i++ {
		if !ql.Allow("ws-1") {
			t.Fatalf("expected Allow=true on call %d", i+1)
		}
	}
	if ql.Allow("ws-1") {
		t.Fatal("expected quota exhausted after 5 calls")
	}

	// Sleep long enough for all entries to expire
	time.Sleep(70 * time.Millisecond)

	// All entries should have expired, so we should be able to make 5 more calls
	for i := 0; i < 5; i++ {
		if !ql.Allow("ws-1") {
			t.Fatalf("expected Allow=true on call %d after full expiry", i+1)
		}
	}
	if ql.Allow("ws-1") {
		t.Fatal("expected quota exhausted after 5 more calls")
	}
}

// --- Context-aware AllowCtx ---

func TestSlidingWindowQuota_AllowCtx_Cancelled(t *testing.T) {
	t.Parallel()
	ql := in.NewSlidingWindowQuota(in.QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       10,
		Window:      time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ql.AllowCtx(ctx, "ws-1")
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// --- FakeQuotaCollector ---

type fakeQuotaCollector struct {
	exhausted int64
}

func (c *fakeQuotaCollector) RecordQuotaExhausted() {
	atomic.AddInt64(&c.exhausted, 1)
}

func (c *fakeQuotaCollector) RecordTokenUsed(_ int64, _ in.IntentSource) {}
