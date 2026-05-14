package outbound

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	channelmetrics "github.com/multica-ai/multica/server/internal/channel/metrics"
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

func AggregatorCollectors() []prometheus.Collector {
	return []prometheus.Collector{aggregatedTotal, droppedTotal}
}

// CardSender is the interface used by the Aggregator to deliver merged
// card messages. The channel adapter (or a thin wrapper) typically
// implements this. SendCard returns an error on transient failures so
// the aggregator can log and count dropped messages.
type CardSender interface {
	SendCard(externalUserID string, card port.OutboundCardMessage, meta AggregationMeta) error
}

// pendingCard is a card ready to be sent outside the lock.
type pendingCard struct {
	externalUserID string
	card           port.OutboundCardMessage
	meta           AggregationMeta
	count          int
}

// AggregationMeta preserves the per-user notification identity while cards
// wait in the aggregation buffer.
type AggregationMeta struct {
	Provider     string
	ConnectionID string
	EventKind    string
	TargetUserID pgtype.UUID
}

// notification is a single pending outbound notification for a user.
type notification struct {
	card port.OutboundCardMessage
	meta AggregationMeta
}

// userBuffer holds buffered notifications for a single external user.
type userBuffer struct {
	externalUserID string
	items          []notification
	timer          *time.Timer
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
	buffers   map[string]*userBuffer // keyed by external user + failure metadata
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

func aggregationKey(externalUserID string, meta AggregationMeta) string {
	return externalUserID + "\x00" + uuidStr(meta.TargetUserID) + "\x00" + meta.EventKind
}

// sendCardSafe calls sender.SendCard with panic recovery and error
// handling. On error or panic the notification is counted as dropped
// and logged. (C1-new, R4-new)
func sendCardSafe(sender CardSender, externalUserID string, card port.OutboundCardMessage, meta AggregationMeta) bool {
	var err error
	ok := true
	func() {
		defer func() {
			if r := recover(); r != nil {
				ok = false
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
		err = sender.SendCard(externalUserID, card, meta)
	}()
	if err != nil {
		ok = false
		slog.Error("outbound aggregator: send card failed",
			"user_id", externalUserID,
			"title", card.Title,
			"error", err,
		)
		droppedTotal.Inc()
		// TODO(T15): enqueue to failure queue for retry
	}
	return ok
}

// Add enqueues a notification for the given external user. If bypass is
// true the notification is sent immediately without buffering (for P0 /
// urgent events). If the user's buffer exceeds MaxBufferSize the buffer
// is flushed immediately and the new notification is sent directly.
func (a *Aggregator) Add(externalUserID string, card port.OutboundCardMessage, bypass bool) {
	a.AddWithMeta(externalUserID, card, AggregationMeta{}, bypass)
}

// AddWithMeta enqueues a notification with failure/audit metadata.
func (a *Aggregator) AddWithMeta(externalUserID string, card port.OutboundCardMessage, meta AggregationMeta, bypass bool) {
	if bypass {
		if sendCardSafe(a.sender, externalUserID, card, meta) {
			channelmetrics.M.RecordOutboundAggregate(meta.Provider, meta.EventKind, "sent", 1)
		} else {
			channelmetrics.M.RecordOutboundAggregate(meta.Provider, meta.EventKind, "error", 1)
		}
		aggregatedTotal.Inc()
		return
	}

	a.mu.Lock()

	if a.closed {
		a.mu.Unlock()
		droppedTotal.Inc()
		channelmetrics.M.RecordOutboundAggregate(meta.Provider, meta.EventKind, "closed", 1)
		return
	}

	key := aggregationKey(externalUserID, meta)
	buf, exists := a.buffers[key]
	if !exists {
		buf = &userBuffer{externalUserID: externalUserID}
		a.buffers[key] = buf
		buf.timer = time.AfterFunc(a.interval, func() {
			a.flushKey(key)
		})
	}

	buf.items = append(buf.items, notification{
		card: withFallbackTarget(card, externalUserID),
		meta: meta,
	})

	// Buffer limit exceeded — prepare both cards, then unlock and send.
	if len(buf.items) > MaxBufferSize {
		// Pop the last item (the 51st) — it will be sent directly.
		lastIdx := len(buf.items) - 1
		last := notification{
			card: buf.items[lastIdx].card,
			meta: buf.items[lastIdx].meta,
		}
		buf.items = buf.items[:lastIdx]

		// Drain the buffer (now 50 items) into a merged card and remove
		// the entry from the map so the next Add starts a fresh window.
		merged := a.prepareMerge(key)

		a.mu.Unlock()

		// Send merged card (50 items) then the 51st — both outside the lock.
		if merged != nil {
			if sendCardSafe(a.sender, merged.externalUserID, merged.card, merged.meta) {
				channelmetrics.M.RecordOutboundAggregate(merged.meta.Provider, merged.meta.EventKind, "merged", merged.count)
			} else {
				channelmetrics.M.RecordOutboundAggregate(merged.meta.Provider, merged.meta.EventKind, "error", merged.count)
			}
			aggregatedTotal.Inc()
		}

		if sendCardSafe(a.sender, externalUserID, last.card, last.meta) {
			channelmetrics.M.RecordOutboundAggregate(last.meta.Provider, last.meta.EventKind, "sent", 1)
		} else {
			channelmetrics.M.RecordOutboundAggregate(last.meta.Provider, last.meta.EventKind, "error", 1)
		}
		aggregatedTotal.Inc()
		return
	}

	a.mu.Unlock()
}

// flushKey is called by the timer goroutine. It acquires the lock,
// prepares the merged card, then sends it outside the lock.
func (a *Aggregator) flushKey(key string) {
	a.mu.Lock()
	pending := a.prepareMerge(key)
	a.mu.Unlock()

	if pending == nil {
		return
	}

	if sendCardSafe(a.sender, pending.externalUserID, pending.card, pending.meta) {
		channelmetrics.M.RecordOutboundAggregate(pending.meta.Provider, pending.meta.EventKind, "merged", pending.count)
	} else {
		channelmetrics.M.RecordOutboundAggregate(pending.meta.Provider, pending.meta.EventKind, "error", pending.count)
	}
	aggregatedTotal.Inc()
}

// prepareMerge drains the buffer for key into a merged card
// and removes the entry from a.buffers, ensuring subsequent Adds for
// the same user start a fresh aggregation window. Returns nil if the
// buffer is missing or empty.
//
// Must be called with a.mu held. Does NOT send the card — the caller
// sends it after releasing the lock. (R1-new, R2-new, C1-r3)
func (a *Aggregator) prepareMerge(key string) *pendingCard {
	buf, ok := a.buffers[key]
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
	meta := buf.items[0].meta
	firstCard := buf.items[0].card

	// Clear buffer state in a single place — see TestAggregator_AddAfter*.
	delete(a.buffers, key)

	return &pendingCard{
		externalUserID: buf.externalUserID,
		card: port.OutboundCardMessage{
			Target:   firstCard.Target,
			ChatID:   firstNonEmpty(firstCard.ChatID, firstCard.Target.ID),
			Title:    fmt.Sprintf("Multica 有 %d 条新通知", count),
			Body:     mergedBody,
			Mentions: firstCard.Mentions,
		},
		meta:  meta,
		count: count,
	}
}

func withFallbackTarget(card port.OutboundCardMessage, externalUserID string) port.OutboundCardMessage {
	if card.Target.ID == "" {
		card.Target = port.TargetUser(externalUserID)
	}
	if card.ChatID == "" {
		card.ChatID = card.Target.ID
	}
	return card
}

// buildMergedBody concatenates notification titles and bodies into a
// single body string for the merged card.
func buildMergedBody(items []notification) string {
	if len(items) == 1 {
		if items[0].card.Body != "" {
			return items[0].card.Title + "\n" + items[0].card.Body
		}
		return items[0].card.Title
	}

	result := ""
	for i, item := range items {
		if i > 0 {
			result += "\n"
		}
		result += fmt.Sprintf("[%d] %s", i+1, item.card.Title)
		if item.card.Body != "" {
			result += ": " + item.card.Body
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
			if len(buf.items) > 0 {
				meta := buf.items[0].meta
				channelmetrics.M.RecordOutboundAggregate(meta.Provider, meta.EventKind, "stop_dropped", len(buf.items))
			}
		}
		a.buffers = make(map[string]*userBuffer)
	})
}
