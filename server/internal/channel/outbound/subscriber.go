package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	channelmetrics "github.com/multica-ai/multica/server/internal/channel/metrics"
	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

var (
	// ErrNotBound is returned when a user has no channel_user_binding row.
	ErrNotBound = errors.New("outbound: user not bound to channel")
)

// BindingStore abstracts the channel_user_binding lookup so the subscriber
// can be tested without a real database.
type BindingStore interface {
	// FindUserID returns the Multica user_id for the given (provider,
	// external_user_id) pair. Returns ErrNotBound if no binding exists.
	FindUserID(ctx context.Context, provider, externalUserID string) (pgtype.UUID, error)

	// ResolveExternalID returns the external_user_id for the given
	// (provider, user_id) pair. Returns ErrNotBound if no binding exists.
	ResolveExternalID(ctx context.Context, provider, userID string) (string, error)
}

// FailureRecorder records retryable outbound send failures. *db.Queries
// satisfies this interface.
type FailureRecorder interface {
	InsertOutboundFailure(ctx context.Context, arg db.InsertOutboundFailureParams) (db.ChannelOutboundFailure, error)
}

// DBPool is the minimal pgx interface we need.
type DBPool interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// DBBindingStore implements BindingStore using raw SQL against the
// channel_user_binding table. (sqlc queries for this table are not yet
// generated; this uses pgx directly.)
type DBBindingStore struct {
	pool DBPool
}

// NewDBBindingStore creates a BindingStore backed by the database.
func NewDBBindingStore(pool DBPool) *DBBindingStore {
	return &DBBindingStore{pool: pool}
}

// FindUserID looks up the Multica user_id for a given channel connection and external_user_id pair.
// Returns ErrNotBound when no row exists; wraps real DB errors for fail-closed behavior.
func (s *DBBindingStore) FindUserID(ctx context.Context, connectionID, externalUserID string) (pgtype.UUID, error) {
	var uid pgtype.UUID
	err := s.pool.QueryRow(ctx,
		`SELECT user_id FROM channel_user_binding WHERE connection_id = $1 AND external_user_id = $2`,
		connectionID, externalUserID,
	).Scan(&uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return pgtype.UUID{}, ErrNotBound
	}
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("find user id: %w", err)
	}
	return uid, nil
}

// ResolveExternalID looks up the external_user_id for a given channel connection and user_id pair.
// Returns ErrNotBound when no row exists; wraps real DB errors for fail-closed behavior.
func (s *DBBindingStore) ResolveExternalID(ctx context.Context, connectionID, userID string) (string, error) {
	var extID string
	err := s.pool.QueryRow(ctx,
		`SELECT external_user_id FROM channel_user_binding WHERE connection_id = $1 AND user_id = $2`,
		connectionID, userID,
	).Scan(&extID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotBound
	}
	if err != nil {
		return "", fmt.Errorf("resolve external id: %w", err)
	}
	return extID, nil
}

// Subscriber subscribes to events.Bus and forwards qualifying events to
// the channel adapter as card messages. It implements the outbound
// notification pipeline for M2 T13.
//
// Event flow:
//
//	events.Bus -> Subscriber -> binding lookup -> pref filter -> channel.SendCard
//
// The subscriber can be workspace-scoped. When workspaceID is empty it
// processes all workspaces, which is the production event-bus wiring.
type Subscriber struct {
	bus         *events.Bus
	channel     port.Channel
	bindings    BindingStore
	prefs       PrefStore
	workspaceID string
	aggregator  *Aggregator
	failures    FailureRecorder
	outbox      NotificationEnqueuer
	activeFunc  func() bool
}

type channelConnectionNamer interface {
	ConnectionID() string
}

type channelProviderNamer interface {
	ProviderName() string
}

func channelConnectionID(ch port.Channel) string {
	if named, ok := ch.(channelConnectionNamer); ok && named.ConnectionID() != "" {
		return named.ConnectionID()
	}
	return ch.Name()
}

func channelProviderName(ch port.Channel) string {
	if named, ok := ch.(channelProviderNamer); ok && named.ProviderName() != "" {
		return named.ProviderName()
	}
	return ch.Name()
}

// NewSubscriber creates an outbound subscriber. Call Start() to begin
// listening for events.
func NewSubscriber(
	bus *events.Bus,
	ch port.Channel,
	bindings BindingStore,
	prefs PrefStore,
	workspaceID string,
) *Subscriber {
	return &Subscriber{
		bus:         bus,
		channel:     ch,
		bindings:    bindings,
		prefs:       prefs,
		workspaceID: workspaceID,
	}
}

// Start begins listening for events on the bus. It subscribes to the
// event types defined in the spec:
//   - comment:created
//   - inbox:new
//   - subscriber:added
//   - issue:updated (status change notifications, M3a)
func (s *Subscriber) Start() {
	s.bus.Subscribe(protocol.EventCommentCreated, s.handleCommentCreated)
	s.bus.Subscribe(protocol.EventInboxNew, s.handleInboxNew)
	s.bus.Subscribe(protocol.EventSubscriberAdded, s.handleSubscriberAdded)
	s.bus.Subscribe(protocol.EventIssueUpdated, s.handleIssueUpdated)
}

func (s *Subscriber) SetAggregator(aggregator *Aggregator) {
	s.aggregator = aggregator
}

func (s *Subscriber) SetFailureRecorder(failures FailureRecorder) {
	s.failures = failures
}

func (s *Subscriber) SetNotificationEnqueuer(outbox NotificationEnqueuer) {
	s.outbox = outbox
}

// SetActiveFunc gates direct outbound delivery. Durable outbox enqueue is not
// gated so every API node can persist notifications; workers/senders decide
// which process is allowed to talk to the external channel.
func (s *Subscriber) SetActiveFunc(activeFunc func() bool) {
	s.activeFunc = activeFunc
}

func (s *Subscriber) Stop() {
	if s.aggregator != nil {
		s.aggregator.Stop()
	}
}

func (s *Subscriber) isActive() bool {
	return s.activeFunc == nil || s.activeFunc()
}

func (s *Subscriber) shouldHandleEvent() bool {
	return s.outbox != nil || s.isActive()
}

// handleCommentCreated processes comment:created events.
// Extracts subscriber user_ids from the payload and sends cards to
// bound, unmuted users.
func (s *Subscriber) handleCommentCreated(e events.Event) {
	if !s.shouldHandleEvent() {
		return
	}
	if s.workspaceID != "" && e.WorkspaceID != s.workspaceID {
		return
	}

	payload, ok := e.Payload.(map[string]any)
	if !ok {
		return
	}

	subscriberIDs := extractStringSlice(payload["subscribers"])
	if len(subscriberIDs) == 0 {
		return
	}

	issueTitle, _ := payload["issue_title"].(string)

	commentObj, ok := payload["comment"].(map[string]any)
	if !ok {
		return
	}
	commentContent, _ := commentObj["content"].(string)

	for _, userID := range subscriberIDs {
		if userID == e.ActorID {
			continue // don't notify self
		}
		s.sendToUser(e.WorkspaceID, userID, "comment_mention", issueTitle, commentContent)
	}
}

// handleInboxNew processes inbox:new events.
// Sends a card to the target user.
func (s *Subscriber) handleInboxNew(e events.Event) {
	if !s.shouldHandleEvent() {
		return
	}
	if s.workspaceID != "" && e.WorkspaceID != s.workspaceID {
		return
	}

	payload, ok := e.Payload.(map[string]any)
	if !ok {
		return
	}

	userID, _ := payload["user_id"].(string)
	if userID == "" || userID == e.ActorID {
		return // no target or self-notification
	}

	issueTitle, _ := payload["title"].(string)
	inboxType, _ := payload["inbox_type"].(string)

	eventKind := mapInboxTypeToEventKind(inboxType)

	s.sendToUser(e.WorkspaceID, userID, eventKind, issueTitle, "")
}

// handleSubscriberAdded processes subscriber:added events.
func (s *Subscriber) handleSubscriberAdded(e events.Event) {
	if !s.shouldHandleEvent() {
		return
	}
	if s.workspaceID != "" && e.WorkspaceID != s.workspaceID {
		return
	}

	payload, ok := e.Payload.(map[string]any)
	if !ok {
		return
	}

	subscriberID, _ := payload["subscriber_id"].(string)
	if subscriberID == "" || subscriberID == e.ActorID {
		return
	}

	issueTitle, _ := payload["issue_title"].(string)

	s.sendToUser(e.WorkspaceID, subscriberID, "issue_mention", issueTitle, "")
}

// handleIssueUpdated processes issue:updated events. When the status
// field changed and the new status is one of the notify-worthy values
// (in_review, done, blocked), a card is sent to the issue's assignee
// so the relevant party is notified of the transition.
func (s *Subscriber) handleIssueUpdated(e events.Event) {
	if !s.shouldHandleEvent() {
		return
	}
	if s.workspaceID != "" && e.WorkspaceID != s.workspaceID {
		return
	}

	payload, ok := e.Payload.(map[string]any)
	if !ok {
		return
	}

	statusChanged, _ := payload["status_changed"].(bool)
	if !statusChanged {
		return
	}

	issueObj, ok := payload["issue"].(map[string]any)
	if !ok {
		return
	}

	status, _ := issueObj["status"].(string)
	eventKind := mapStatusToEventKind(status)
	if eventKind == "" {
		return // unsupported status — not a notify-worthy transition
	}

	issueTitle, _ := issueObj["title"].(string)
	issueIdentifier, _ := issueObj["identifier"].(string)

	// Notify the assignee if present and different from the actor.
	assigneeID, _ := issueObj["assignee_id"].(string)
	if assigneeID != "" && assigneeID != e.ActorID {
		body := fmt.Sprintf("Issue %s 状态已变更为 %s", issueIdentifier, statusLabel(status))
		s.sendToUser(e.WorkspaceID, assigneeID, eventKind, issueTitle, body)
	}
}

// mapStatusToEventKind maps issue statuses to the preference JSONB key
// names. Only the three M3a statuses produce a non-empty kind.
func mapStatusToEventKind(status string) string {
	switch status {
	case "in_review":
		return "status_in_review"
	case "done":
		return "status_done"
	case "blocked":
		return "status_blocked"
	default:
		return ""
	}
}

// statusLabel returns a human-readable Chinese label for the status.
func statusLabel(status string) string {
	switch status {
	case "in_review":
		return "评审中"
	case "done":
		return "已完成"
	case "blocked":
		return "已阻塞"
	default:
		return status
	}
}

// sendToUser resolves the user's binding, checks preferences, and
// sends a card message.
func (s *Subscriber) sendToUser(workspaceID, userID, eventKind, title, body string) {
	ctx := context.Background()
	providerName := channelProviderName(s.channel)
	connectionID := channelConnectionID(s.channel)

	// R4: parseUUID returns error; log+drop on invalid UUID.
	wsUUID, err := parseUUID(workspaceID)
	if err != nil {
		channelmetrics.M.RecordOutboundFailure(providerName, eventKind, "parse_workspace_id", false)
		slog.Error("outbound: invalid workspace id", "workspace_id", workspaceID, "error", err)
		return
	}
	userUUID, err := parseUUID(userID)
	if err != nil {
		channelmetrics.M.RecordOutboundFailure(providerName, eventKind, "parse_user_id", false)
		slog.Error("outbound: invalid user id", "user_id", userID, "error", err)
		return
	}

	enabled, err := s.prefs.GetChannelPref(ctx, wsUUID, userUUID, providerName, eventKind)
	if err != nil {
		channelmetrics.M.RecordOutboundFailure(providerName, eventKind, "pref_lookup", false)
		slog.Error("outbound: check pref", "user_id", userID, "error", err)
		return
	}
	if !enabled {
		channelmetrics.M.RecordOutboundCard(providerName, eventKind, "muted")
		return // muted
	}

	// R5: Inline ResolveExternalID (removed wrapper).
	externalUserID, err := s.bindings.ResolveExternalID(ctx, connectionID, userID)
	if err != nil {
		if errors.Is(err, ErrNotBound) {
			channelmetrics.M.RecordOutboundCard(providerName, eventKind, "unbound")
			return // TC-out-2: unbound -> drop silently
		}
		channelmetrics.M.RecordOutboundFailure(providerName, eventKind, "binding_lookup", false)
		slog.Error("outbound: resolve binding", "user_id", userID, "error", err)
		return
	}

	card := port.OutboundCardMessage{
		Target: port.TargetUser(externalUserID),
		ChatID: externalUserID,
		Title:  title,
		Body:   body,
	}

	if s.outbox != nil {
		if err := s.outbox.EnqueueNotification(ctx, NotificationEnqueueRequest{
			Provider:             providerName,
			ConnectionID:         connectionID,
			EventKind:            eventKind,
			TargetUserID:         userUUID,
			TargetExternalUserID: externalUserID,
			Title:                title,
			Body:                 body,
		}); err != nil {
			channelmetrics.M.RecordOutboundFailure(providerName, eventKind, "outbox_enqueue", true)
			slog.Error("outbound: enqueue notification", "user_id", userID, "error", err)
			return
		}
		channelmetrics.M.RecordOutboundCard(providerName, eventKind, "queued")
		channelmetrics.M.RecordOutboundOutbox(providerName, "queued", 1)
		return
	}

	if !s.isActive() {
		channelmetrics.M.RecordOutboundCard(providerName, eventKind, "inactive")
		return
	}

	if s.aggregator != nil {
		s.aggregator.AddWithMeta(externalUserID, card, AggregationMeta{
			Provider:     providerName,
			ConnectionID: connectionID,
			EventKind:    eventKind,
			TargetUserID: userUUID,
		}, false)
		channelmetrics.M.RecordOutboundCard(providerName, eventKind, "queued")
		return
	}

	result, err := s.channel.SendCard(ctx, card)
	if err != nil {
		channelmetrics.M.RecordOutboundCard(providerName, eventKind, "error")
		channelmetrics.M.RecordOutboundFailure(providerName, eventKind, "send", result.Retryable)
		if result.Retryable {
			s.recordFailure(ctx, providerName, connectionID, eventKind, userUUID, externalUserID, card, err)
		}
		slog.Error("outbound: send card", "user_id", userID, "error", err)
		return
	}

	channelmetrics.M.RecordOutboundCard(providerName, eventKind, "sent")
	slog.Info("outbound: card sent",
		"user_id", userID,
		"platform_msg_id", result.PlatformMessageID,
		"event_kind", eventKind,
	)
}

// mapInboxTypeToEventKind maps inbox notification types to the
// preference JSONB key names.
func mapInboxTypeToEventKind(inboxType string) string {
	switch inboxType {
	case "issue_assigned", "assignee_changed", "unassigned":
		return "issue_assigned"
	case "mentioned":
		return "issue_mention"
	case "new_comment":
		return "comment_mention"
	default:
		return "issue_assigned"
	}
}

// extractStringSlice safely extracts a []string from an any value,
// handling both []string and []any (from JSON deserialization).
func extractStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	if ss, ok := v.([]string); ok {
		return ss
	}
	if arr, ok := v.([]any); ok {
		result := make([]string, 0, len(arr))
		for _, item := range arr {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	}
	return nil
}

func (s *Subscriber) recordFailure(ctx context.Context, providerName, connectionID, eventKind string, targetUserID pgtype.UUID, externalUserID string, card port.OutboundCardMessage, sendErr error) {
	if s.failures == nil {
		return
	}
	payload, err := json.Marshal(RetryPayload{
		Title: card.Title,
		Body:  card.Body,
	})
	if err != nil {
		channelmetrics.M.RecordOutboundFailure(providerName, eventKind, "retry_payload_marshal", true)
		slog.Error("outbound: marshal retry payload", "user_id", uuidStr(targetUserID), "error", err)
		return
	}
	if _, err := s.failures.InsertOutboundFailure(ctx, db.InsertOutboundFailureParams{
		Provider:             providerName,
		ConnectionID:         connectionID,
		EventKind:            eventKind,
		TargetUserID:         targetUserID,
		TargetExternalUserID: pgtype.Text{String: externalUserID, Valid: externalUserID != ""},
		Payload:              payload,
		MaxAttempts:          3,
	}); err != nil {
		channelmetrics.M.RecordOutboundFailure(providerName, eventKind, "failure_insert", true)
		slog.Error("outbound: insert failure",
			"user_id", uuidStr(targetUserID),
			"event_kind", eventKind,
			"send_error", sendErr,
			"error", err,
		)
	} else {
		channelmetrics.M.RecordOutboundFailure(providerName, eventKind, "failure_recorded", true)
	}
}

// R4: parseUUID returns (pgtype.UUID, error) for fail-closed behavior.
func parseUUID(s string) (pgtype.UUID, error) {
	var u pgtype.UUID
	if err := u.Scan(s); err != nil {
		return pgtype.UUID{}, fmt.Errorf("parse uuid %q: %w", s, err)
	}
	return u, nil
}
