package outbound

import (
	"fmt"
	"log/slog"
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
		Help: "Total number of notifications dropped (buffer overflow, stop, send error, etc).",
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
// implements this. SendCard returns an error on transient failures so
// the aggregator can log and count dropped messages.
type CardSender interface {
	SendCard(externalUserID string, card port.OutboundCardMessage) error
}

// pendingCard is a card ready to be sent outside the lock.
type pendingCard struct {
	externalUserID string
	card           port.OutboundCardMessage
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

// sendCardSafe calls sender.SendCard with panic recovery and error
// handling. On error or panic the notification is counted as dropped
// and logged. (C1-new, R4-new)
func sendCardSafe(sender CardSender, externalUserID string, card port.OutboundCardMessage) {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("outbound aggregator: send card panic",
					"user_id", externalUserID,
					"title", card.Title,
					"panic", r,
				)
				droppedTotal.Inc()
				// TODO(T15): panic'd sends are likely unrecoverable;
				// decide whether to enqueue or permanently drop.
			}
		}()
		err = sender.SendCard(externalUserID, card)
	}()
	if err != nil {
		slog.Error("outbound aggregator: send card failed",
			"user_id", externalUserID,
			"title", card.Title,
			"error", err,
		)
		droppedTotal.Inc()
		// TODO(T15): enqueue to failure queue for retry
	}
}

// Add enqueues a notification for the given external user. If bypass is
// true the notification is sent immediately without buffering (for P0 /
// urgent events). If the user's buffer exceeds MaxBufferSize the buffer
// is flushed immediately and the new notification is sent directly.
func (a *Aggregator) Add(externalUserID string, card port.OutboundCardMessage, bypass bool) {
	if bypass {
		sendCardSafe(a.sender, externalUserID, card)
		aggregatedTotal.Inc()
		return
	}

	a.mu.Lock()

	if a.closed {
		a.mu.Unlock()
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

	// Buffer limit exceeded — prepare both cards, then unlock and send.
	if len(buf.items) > MaxBufferSize {
		// Pop the last item (the 51st) — it will be sent directly.
		lastIdx := len(buf.items) - 1
		last := notification{
			title: buf.items[lastIdx].title,
			body:  buf.items[lastIdx].body,
		}
		buf.items = buf.items[:lastIdx]

		// Drain the buffer (now 50 items) into a merged card and remove
		// the entry from the map so the next Add starts a fresh window.
		merged := a.prepareMerge(externalUserID)

		a.mu.Unlock()

		// Send merged card (50 items) then the 51st — both outside the lock.
		if merged != nil {
			sendCardSafe(a.sender, merged.externalUserID, merged.card)
			aggregatedTotal.Inc()
		}

		sendCardSafe(a.sender, externalUserID, port.OutboundCardMessage{
			ChatID: externalUserID,
			Title:  last.title,
			Body:   last.body,
		})
		aggregatedTotal.Inc()
		return
	}

	a.mu.Unlock()
}

// flushUser is called by the timer goroutine. It acquires the lock,
// prepares the merged card, then sends it outside the lock.
func (a *Aggregator) flushUser(externalUserID string) {
	a.mu.Lock()
	pending := a.prepareMerge(externalUserID)
	a.mu.Unlock()

	if pending == nil {
		return
	}

	sendCardSafe(a.sender, pending.externalUserID, pending.card)
	aggregatedTotal.Inc()
}

// prepareMerge drains the buffer for externalUserID into a merged card
// and removes the entry from a.buffers, ensuring subsequent Adds for
// the same user start a fresh aggregation window. Returns nil if the
// buffer is missing or empty.
//
// Must be called with a.mu held. Does NOT send the card — the caller
// sends it after releasing the lock. (R1-new, R2-new, C1-r3)
func (a *Aggregator) prepareMerge(externalUserID string) *pendingCard {
	buf, ok := a.buffers[externalUserID]
	if !ok || buf == nil || len(buf.items) == 0 {
		return nil
	}

	// Stop the timer if it hasn't fired yet (e.g. called from buffer limit).
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}

	count := len(buf.items)
	mergedBody := buildMergedBody(buf.items)

	// Clear buffer state in a single place — see TestAggregator_AddAfter*.
	delete(a.buffers, externalUserID)

	return &pendingCard{
		externalUserID: externalUserID,
		card: port.OutboundCardMessage{
			ChatID: externalUserID,
			Title:  fmt.Sprintf("你有 %d 条新通知", count),
			Body:   mergedBody,
		},
	}
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
			// Each buffered notification is a dropped message — count
			// per-notification, not per-user, so the SLO metric reflects
			// actual loss. (R1-r3)
			droppedTotal.Add(float64(len(buf.items)))
		}
		a.buffers = make(map[string]*userBuffer)
	})
}
