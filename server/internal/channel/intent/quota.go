package intent

import (
	"context"
	"sync"
	"time"
)

// QuotaLimiter checks whether a workspace is within its LLM call quota.
type QuotaLimiter interface {
	Allow(workspaceID string) bool
	AllowCtx(ctx context.Context, workspaceID string) (bool, error)
}

// QuotaConfig configures a SlidingWindowQuota.
type QuotaConfig struct {
	WorkspaceID string        // default workspace (for convenience)
	Limit       int           // max calls per window
	Window      time.Duration // sliding window duration
	Collector   MetricsCollector
}

// SlidingWindowQuota implements a per-workspace sliding-window rate limiter.
// State is in-memory; restart resets quotas (acceptable for M2).
type SlidingWindowQuota struct {
	mu        sync.Mutex
	limit     int
	window    time.Duration
	collector MetricsCollector
	entries   map[string]*windowRing // workspaceID → ring buffer of timestamps
}

type windowRing struct {
	timestamps []time.Time
	head       int
	count      int
}

// NewSlidingWindowQuota creates a new sliding-window quota limiter.
func NewSlidingWindowQuota(cfg QuotaConfig) *SlidingWindowQuota {
	return &SlidingWindowQuota{
		limit:     cfg.Limit,
		window:    cfg.Window,
		collector: cfg.Collector,
		entries:   make(map[string]*windowRing),
	}
}

// Allow checks and records a usage attempt. Returns true if within quota.
func (q *SlidingWindowQuota) Allow(workspaceID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	e := q.getOrCreate(workspaceID)
	q.evictExpired(e, time.Now())

	if e.count >= q.limit {
		if q.collector != nil {
			q.collector.RecordQuotaExhausted()
		}
		return false
	}

	e.timestamps[e.head] = time.Now()
	e.head = (e.head + 1) % q.limit
	if e.count < q.limit {
		e.count++
	}
	return true
}

// AllowCtx is the context-aware version of Allow.
func (q *SlidingWindowQuota) AllowCtx(ctx context.Context, workspaceID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return q.Allow(workspaceID), nil
}

func (q *SlidingWindowQuota) getOrCreate(workspaceID string) *windowRing {
	e, ok := q.entries[workspaceID]
	if !ok {
		e = &windowRing{
			timestamps: make([]time.Time, q.limit),
		}
		q.entries[workspaceID] = e
	}
	return e
}

func (q *SlidingWindowQuota) evictExpired(e *windowRing, now time.Time) {
	cutoff := now.Add(-q.window)
	newCount := 0
	for i := 0; i < e.count; i++ {
		idx := (e.head - e.count + i + q.limit) % q.limit
		if e.timestamps[idx].After(cutoff) {
			if newCount != i {
				newIdx := (e.head - e.count + newCount + q.limit) % q.limit
				e.timestamps[newIdx] = e.timestamps[idx]
			}
			newCount++
		}
	}
	e.count = newCount
	e.head = (e.head - e.count + newCount + q.limit) % q.limit
}
