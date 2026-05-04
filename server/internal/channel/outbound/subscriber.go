package outbound

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/events"
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

// DBPool is the minimal pgx interface we need.
type DBPool interface {
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

// Row is a minimal interface for pgx.Row.
type Row interface {
	Scan(dest ...any) error
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

// FindUserID looks up the Multica user_id for a given (provider, external_user_id) pair.
// Returns ErrNotBound when no row exists; wraps real DB errors for fail-closed behavior.
func (s *DBBindingStore) FindUserID(ctx context.Context, provider, externalUserID string) (pgtype.UUID, error) {
	var uid pgtype.UUID
	err := s.pool.QueryRow(ctx,
		`SELECT user_id FROM channel_user_binding WHERE provider = $1 AND external_user_id = $2`,
		provider, externalUserID,
	).Scan(&uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return pgtype.UUID{}, ErrNotBound
	}
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("find user id: %w", err)
	}
	return uid, nil
}

// ResolveExternalID looks up the external_user_id for a given (provider, user_id) pair.
// Returns ErrNotBound when no row exists; wraps real DB errors for fail-closed behavior.
func (s *DBBindingStore) ResolveExternalID(ctx context.Context, provider, userID string) (string, error) {
	var extID string
	err := s.pool.QueryRow(ctx,
		`SELECT external_user_id FROM channel_user_binding WHERE provider = $1 AND user_id = $2`,
		provider, userID,
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
// The subscriber is workspace-scoped: it only processes events whose
// WorkspaceID matches the configured workspaceID.
type Subscriber struct {
	bus         *events.Bus
	channel     port.Channel
	bindings    BindingStore
	prefs       PrefStore
	workspaceID string
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
// three event types defined in the spec:
//   - comment:created
//   - inbox:new
//   - subscriber:added
func (s *Subscriber) Start() {
	s.bus.Subscribe(protocol.EventCommentCreated, s.handleCommentCreated)
	s.bus.Subscribe(protocol.EventInboxNew, s.handleInboxNew)
	s.bus.Subscribe(protocol.EventSubscriberAdded, s.handleSubscriberAdded)
}

func (s *Subscriber) Stop() {}

// handleCommentCreated processes comment:created events.
// Extracts subscriber user_ids from the payload and sends cards to
// bound, unmuted users.
func (s *Subscriber) handleCommentCreated(e events.Event) {
	if e.WorkspaceID != s.workspaceID {
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
	if e.WorkspaceID != s.workspaceID {
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
	if e.WorkspaceID != s.workspaceID {
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

// sendToUser resolves the user's binding, checks preferences, and
// sends a card message.
func (s *Subscriber) sendToUser(workspaceID, userID, eventKind, title, body string) {
	ctx := context.Background()

	// R4: parseUUID returns error; log+drop on invalid UUID.
	wsUUID, err := parseUUID(workspaceID)
	if err != nil {
		slog.Error("outbound: invalid workspace id", "workspace_id", workspaceID, "error", err)
		return
	}
	userUUID, err := parseUUID(userID)
	if err != nil {
		slog.Error("outbound: invalid user id", "user_id", userID, "error", err)
		return
	}

	enabled, err := s.prefs.GetChannelPref(ctx, wsUUID, userUUID, s.channel.Name(), eventKind)
	if err != nil {
		slog.Error("outbound: check pref", "user_id", userID, "error", err)
		return
	}
	if !enabled {
		return // muted
	}

	// R5: Inline ResolveExternalID (removed wrapper).
	externalUserID, err := s.bindings.ResolveExternalID(ctx, s.channel.Name(), userID)
	if err != nil {
		if errors.Is(err, ErrNotBound) {
			return // TC-out-2: unbound -> drop silently
		}
		slog.Error("outbound: resolve binding", "user_id", userID, "error", err)
		return
	}

	card := port.OutboundCardMessage{
		ChatID: externalUserID,
		Title:  title,
		Body:   body,
	}

	result, err := s.channel.SendCard(ctx, card)
	if err != nil {
		slog.Error("outbound: send card", "user_id", userID, "error", err)
		return
	}

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

// R4: parseUUID returns (pgtype.UUID, error) for fail-closed behavior.
func parseUUID(s string) (pgtype.UUID, error) {
	var u pgtype.UUID
	if err := u.Scan(s); err != nil {
		return pgtype.UUID{}, fmt.Errorf("parse uuid %q: %w", s, err)
	}
	return u, nil
}
