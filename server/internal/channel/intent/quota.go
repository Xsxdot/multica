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
	entries   map[string]*windowRing // workspaceID → slice of timestamps
}

type windowRing struct {
	ts []time.Time
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
	now := time.Now()
	cutoff := now.Add(-q.window)

	// Evict expired entries (partial eviction)
	keep := e.ts[:0]
	for _, t := range e.ts {
		if t.After(cutoff) {
			keep = append(keep, t)
		}
	}
	e.ts = keep

	if len(e.ts) >= q.limit {
		if q.collector != nil {
			q.collector.RecordQuotaExhausted()
		}
		return false
	}

	e.ts = append(e.ts, now)
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
		e = &windowRing{}
		q.entries[workspaceID] = e
	}
	return e
}
