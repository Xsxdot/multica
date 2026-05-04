package outbound

import (
	"fmt"
	"sync"
	"time"

	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// DefaultFlushInterval is the default aggregation window duration.
	DefaultFlushInterval = 60 * time.Second

	// MaxBufferSize is the maximum number of notifications per user
	// before an immediate flush is triggered.
	MaxBufferSize = 50
)

var (
	aggregatedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "channel_outbound_aggregated_total",
		Help: "Total number of notification cards sent after aggregation.",
	})
	droppedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "channel_outbound_dropped_total",
		Help: "Total number of notifications dropped (buffer overflow, stop, etc).",
	})
	metricsOnce sync.Once
)

func registerMetrics() {
	metricsOnce.Do(func() {
		prometheus.MustRegister(aggregatedTotal, droppedTotal)
	})
}

// CardSender is the interface used by the Aggregator to deliver merged
// card messages. The channel adapter (or a thin wrapper) typically
// implements this.
type CardSender interface {
	SendCard(externalUserID string, card port.OutboundCardMessage)
}

// notification is a single pending outbound notification for a user.
type notification struct {
	title string
	body  string
}

// userBuffer holds buffered notifications for a single external user.
type userBuffer struct {
	items []notification
	timer *time.Timer
}

// Aggregator implements user-level notification aggregation. Instead of
// sending one card per event, it buffers notifications per user and
// flushes them as a single merged card after a configurable window.
//
// Concurrency: all public methods are safe for concurrent use.
type Aggregator struct {
	sender    CardSender
	interval  time.Duration
	mu        sync.Mutex
	buffers   map[string]*userBuffer // keyed by externalUserID
	closed    bool
	closeOnce sync.Once
}

// NewAggregator creates an Aggregator with the given sender and flush
// interval. Pass 0 to use DefaultFlushInterval (60s).
func NewAggregator(sender CardSender, interval time.Duration) *Aggregator {
	registerMetrics()
	if interval <= 0 {
		interval = DefaultFlushInterval
	}
	return &Aggregator{
		sender:   sender,
		interval: interval,
		buffers:  make(map[string]*userBuffer),
	}
}

// Add enqueues a notification for the given external user. If bypass is
// true the notification is sent immediately without buffering (for P0 /
// urgent events). If the user's buffer reaches MaxBufferSize the buffer
// is flushed immediately and the new notification starts a fresh buffer.
func (a *Aggregator) Add(externalUserID string, card port.OutboundCardMessage, bypass bool) {
	if bypass {
		a.sender.SendCard(externalUserID, card)
		aggregatedTotal.Inc()
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		droppedTotal.Inc()
		return
	}

	buf, exists := a.buffers[externalUserID]
	if !exists {
		buf = &userBuffer{}
		a.buffers[externalUserID] = buf
		buf.timer = time.AfterFunc(a.interval, func() {
			a.flushUser(externalUserID)
		})
	}

	buf.items = append(buf.items, notification{
		title: card.Title,
		body:  card.Body,
	})

	// Buffer limit exceeded — flush immediately. The 51st notification
	// triggers a flush of the existing 50 items, then this notification
	// is sent directly (not buffered).
	if len(buf.items) > MaxBufferSize {
		// Pop the last item (the 51st) — it will be sent directly.
		lastIdx := len(buf.items) - 1
		last := notification{
			title: buf.items[lastIdx].title,
			body:  buf.items[lastIdx].body,
		}
		buf.items = buf.items[:lastIdx]

		// Flush the buffered 50 items (acquires/releases lock internally).
		a.flushUserLocked(externalUserID, buf)

		// Send the 51st notification immediately (not buffered).
		a.sender.SendCard(externalUserID, port.OutboundCardMessage{
			ChatID: externalUserID,
			Title:  last.title,
			Body:   last.body,
		})
		aggregatedTotal.Inc()
		return
	}
}

// flushUser is called by the timer goroutine. It acquires the lock and
// delegates to flushUserLocked.
func (a *Aggregator) flushUser(externalUserID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushUserLocked(externalUserID, a.buffers[externalUserID])
}

// flushUserLocked merges buffered notifications into a single card and
// sends it. Must be called with a.mu held.
func (a *Aggregator) flushUserLocked(externalUserID string, buf *userBuffer) {
	if buf == nil || len(buf.items) == 0 {
		return
	}

	// Stop the timer if it hasn't fired yet (e.g. called from buffer limit).
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}

	count := len(buf.items)
	mergedBody := buildMergedBody(buf.items)

	// Clear the buffer before releasing the lock.
	delete(a.buffers, externalUserID)
	a.mu.Unlock()

	mergedCard := port.OutboundCardMessage{
		ChatID: externalUserID,
		Title:  fmt.Sprintf("你有 %d 条新通知", count),
		Body:   mergedBody,
	}

	a.sender.SendCard(externalUserID, mergedCard)
	aggregatedTotal.Add(float64(count))

	// Re-acquire the lock for the caller.
	a.mu.Lock()
}

// buildMergedBody concatenates notification titles and bodies into a
// single body string for the merged card.
func buildMergedBody(items []notification) string {
	if len(items) == 1 {
		if items[0].body != "" {
			return items[0].title + "\n" + items[0].body
		}
		return items[0].title
	}

	result := ""
	for i, item := range items {
		if i > 0 {
			result += "\n"
		}
		result += fmt.Sprintf("[%d] %s", i+1, item.title)
		if item.body != "" {
			result += ": " + item.body
		}
	}
	return result
}

// Stop gracefully shuts down the aggregator. Any buffered notifications
// that have not yet been flushed are dropped (PRD tolerates loss on
// process restart). No failure queue entries are created.
func (a *Aggregator) Stop() {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		defer a.mu.Unlock()

		a.closed = true
		for _, buf := range a.buffers {
			if buf.timer != nil {
				buf.timer.Stop()
			}
			droppedTotal.Add(float64(len(buf.items)))
		}
		a.buffers = make(map[string]*userBuffer)
	})
}
