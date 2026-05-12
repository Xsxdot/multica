package outbound

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// --- Mocks ---

type mockChannel struct {
	name     string
	messages []port.OutboundCardMessage
	mu       sync.Mutex
}

func (m *mockChannel) Name() string                       { return m.name }
func (m *mockChannel) Connect(_ context.Context) error    { return nil }
func (m *mockChannel) Disconnect(_ context.Context) error { return nil }
func (m *mockChannel) Send(_ context.Context, _ port.OutboundMessage) (port.SendResult, error) {
	return port.SendResult{}, nil
}
func (m *mockChannel) SendCard(_ context.Context, msg port.OutboundCardMessage) (port.SendResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	return port.SendResult{PlatformMessageID: "msg-123"}, nil
}
func (m *mockChannel) Events() <-chan port.InboundEvent { return nil }
func (m *mockChannel) GetChatInfo(_ context.Context, _ string) (port.ChatInfo, error) {
	return port.ChatInfo{}, nil
}
func (m *mockChannel) GetUserInfo(_ context.Context, _ string) (port.UserInfo, error) {
	return port.UserInfo{}, nil
}

func (m *mockChannel) Messages() []port.OutboundCardMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]port.OutboundCardMessage, len(m.messages))
	copy(cp, m.messages)
	return cp
}

// mockBindingStore implements BindingStore and also supports reverse
// lookup for testing (user_id → external_user_id).
type mockBindingStore struct {
	bindings map[string]map[string]string // provider -> external_user_id -> user_id
	mu       sync.RWMutex
}

func (m *mockBindingStore) FindUserID(_ context.Context, provider, externalUserID string) (pgtype.UUID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if byProvider, ok := m.bindings[provider]; ok {
		if uid, ok := byProvider[externalUserID]; ok {
			return parseTestUUID(uid), nil
		}
	}
	return pgtype.UUID{}, ErrNotBound
}

func (m *mockBindingStore) ResolveExternalID(_ context.Context, provider, userID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if byProvider, ok := m.bindings[provider]; ok {
		for extID, uid := range byProvider {
			if uid == userID {
				return extID, nil
			}
		}
	}
	return "", ErrNotBound
}

type mockPrefStore struct {
	prefs map[string]map[string]string // user_id -> preferences map
	mu    sync.RWMutex
}

func (m *mockPrefStore) GetChannelPref(_ context.Context, _, userID pgtype.UUID, channelName, eventKind string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	uid := pgtypeUUIDToString(userID)
	p, ok := m.prefs[uid]
	if !ok {
		return true, nil // default true
	}
	key := channelName + "." + eventKind
	if v, ok := p[key]; ok {
		return v != "muted", nil
	}
	return true, nil
}

type mockOutbox struct {
	mu       sync.Mutex
	requests []NotificationEnqueueRequest
}

func (m *mockOutbox) EnqueueNotification(_ context.Context, req NotificationEnqueueRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, req)
	return nil
}

func (m *mockOutbox) Requests() []NotificationEnqueueRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]NotificationEnqueueRequest, len(m.requests))
	copy(cp, m.requests)
	return cp
}

// --- Helpers ---

func parseTestUUID(s string) pgtype.UUID {
	var u pgtype.UUID
	_ = u.Scan(s)
	return u
}

func pgtypeUUIDToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	// Format as standard UUID string
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// --- Tests ---

func TestSubscriber_InactiveDoesNotHandleEvents(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	bindingStore := &mockBindingStore{bindings: map[string]map[string]string{
		"feishu": {
			"ext-user-1": "00000000-0000-0000-0000-000000000001",
		},
	}}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.SetActiveFunc(func() bool { return false })
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     "00000000-0000-0000-0000-000000000099",
		Payload: map[string]any{
			"user_id":    "00000000-0000-0000-0000-000000000001",
			"inbox_type": "issue_assigned",
			"title":      "Test Issue",
		},
	})

	if got := len(ch.Messages()); got != 0 {
		t.Fatalf("messages = %d, want 0 when subscriber is inactive", got)
	}
}

func TestSubscriber_StopUnsubscribesAndSuppressesOutbox(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	bindingStore := &mockBindingStore{bindings: map[string]map[string]string{
		"feishu": {
			"ext-user-1": "00000000-0000-0000-0000-000000000001",
		},
	}}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}
	outbox := &mockOutbox{}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.SetNotificationEnqueuer(outbox)
	sub.Start()
	sub.Stop()
	sub.Stop()

	bus.Publish(events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     "00000000-0000-0000-0000-000000000099",
		Payload: map[string]any{
			"user_id":    "00000000-0000-0000-0000-000000000001",
			"inbox_type": "issue_assigned",
			"title":      "Test Issue",
		},
	})

	if got := len(outbox.Requests()); got != 0 {
		t.Fatalf("outbox requests = %d, want 0 after subscriber stop", got)
	}
}

func TestSubscriber_OutboxEnqueuesWhenDirectDeliveryInactive(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000001"
	bindingStore := &mockBindingStore{bindings: map[string]map[string]string{
		"feishu": {
			"ext-user-1": userID,
		},
	}}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}
	outbox := &mockOutbox{}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.SetActiveFunc(func() bool { return false })
	sub.SetNotificationEnqueuer(outbox)
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     "00000000-0000-0000-0000-000000000099",
		Payload: map[string]any{
			"user_id":    userID,
			"inbox_type": "issue_assigned",
			"title":      "Test Issue",
		},
	})

	if got := len(ch.Messages()); got != 0 {
		t.Fatalf("messages = %d, want 0 when direct delivery is inactive", got)
	}
	requests := outbox.Requests()
	if len(requests) != 1 {
		t.Fatalf("outbox requests = %d, want 1", len(requests))
	}
	if requests[0].TargetExternalUserID != "ext-user-1" {
		t.Fatalf("target external user = %q, want ext-user-1", requests[0].TargetExternalUserID)
	}
}

// TC-out-2: Unbound user → drop, no message sent
func TestSubscriber_UnboundUser_DropsEvent(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	bindingStore := &mockBindingStore{bindings: map[string]map[string]string{}}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     "actor-1",
		Payload: map[string]any{
			"user_id":    "00000000-0000-0000-0000-000000000001",
			"user_type":  "member",
			"inbox_type": "issue_assigned",
			"issue_id":   "issue-1",
			"title":      "Test Issue",
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 0 {
		t.Errorf("TC-out-2: expected 0 messages for unbound user, got %d", len(msgs))
	}
}

// TC-out-1: Bound user → message sent within 5s (AC6.1: enqueue latency)
func TestSubscriber_BoundUser_SendsCard(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": userID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	// We need a custom subscriber that can resolve external IDs from mock
	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	start := time.Now()
	bus.Publish(events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     "actor-1",
		Payload: map[string]any{
			"user_id":    userID,
			"user_type":  "member",
			"inbox_type": "issue_assigned",
			"issue_id":   "issue-1",
			"title":      "Test Issue",
		},
	})
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Errorf("TC-out-1: enqueue took %v, must be <= 5s", elapsed)
	}

	msgs := ch.Messages()
	if len(msgs) != 1 {
		t.Fatalf("TC-out-1: expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ChatID != "ext-user-1" {
		t.Errorf("TC-out-1: expected ChatID ext-user-1, got %s", msgs[0].ChatID)
	}
}

// TC-out-3: Preference muted → drop
func TestSubscriber_PrefMuted_DropsEvent(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": userID},
		},
	}
	prefStore := &mockPrefStore{
		prefs: map[string]map[string]string{
			userID: {
				"feishu.issue_assigned": "muted",
			},
		},
	}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     "actor-1",
		Payload: map[string]any{
			"user_id":    userID,
			"user_type":  "member",
			"inbox_type": "issue_assigned",
			"issue_id":   "issue-1",
			"title":      "Test Issue",
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 0 {
		t.Errorf("TC-out-3: expected 0 messages when pref muted, got %d", len(msgs))
	}
}

// Test: comment:created event triggers send
func TestSubscriber_CommentCreated_SendsCard(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": userID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventCommentCreated,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     "actor-1",
		Payload: map[string]any{
			"comment": map[string]any{
				"id":       "comment-1",
				"issue_id": "issue-1",
				"content":  "Hello",
			},
			"subscribers": []string{userID},
			"issue_title": "Test Issue",
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message for comment:created, got %d", len(msgs))
	}
}

// Test: subscriber:added event triggers send
func TestSubscriber_SubscriberAdded_SendsCard(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": userID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventSubscriberAdded,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     "actor-1",
		Payload: map[string]any{
			"subscriber_id":   userID,
			"subscriber_type": "member",
			"issue_id":        "issue-1",
			"issue_title":     "Test Issue",
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message for subscriber:added, got %d", len(msgs))
	}
}

// Test: wrong workspace → drop
func TestSubscriber_WrongWorkspace_DropsEvent(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": userID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: "00000000-0000-0000-0000-000000000200",
		ActorID:     "actor-1",
		Payload: map[string]any{
			"user_id":    userID,
			"user_type":  "member",
			"inbox_type": "issue_assigned",
			"issue_id":   "issue-1",
			"title":      "Test Issue",
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages for wrong workspace, got %d", len(msgs))
	}
}

// --- Status Change Notification Tests (M3a) ---

// Test: issue:updated with status=in_review sends card to assignee
func TestSubscriber_StatusInReview_SendsCard(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	assigneeID := "00000000-0000-0000-0000-000000000098"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": assigneeID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventIssueUpdated,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     userID,
		Payload: map[string]any{
			"status_changed": true,
			"issue": map[string]any{
				"id":          "issue-1",
				"title":       "Test Issue",
				"identifier":  "MUL-1",
				"status":      "in_review",
				"assignee_id": assigneeID,
			},
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message for in_review status, got %d", len(msgs))
	}
	if msgs[0].ChatID != "ext-user-1" {
		t.Errorf("expected ChatID ext-user-1, got %s", msgs[0].ChatID)
	}
	wantBody := "Issue MUL-1 状态已变更为 评审中"
	if msgs[0].Body != wantBody {
		t.Errorf("expected body %q, got %q", wantBody, msgs[0].Body)
	}
}

// Test: issue:updated with status=done sends card to assignee
func TestSubscriber_StatusDone_SendsCard(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	assigneeID := "00000000-0000-0000-0000-000000000098"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": assigneeID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventIssueUpdated,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     userID,
		Payload: map[string]any{
			"status_changed": true,
			"issue": map[string]any{
				"id":          "issue-1",
				"title":       "Test Issue",
				"identifier":  "MUL-1",
				"status":      "done",
				"assignee_id": assigneeID,
			},
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message for done status, got %d", len(msgs))
	}
	wantBody := "Issue MUL-1 状态已变更为 已完成"
	if msgs[0].Body != wantBody {
		t.Errorf("expected body %q, got %q", wantBody, msgs[0].Body)
	}
}

// Test: issue:updated with status=blocked sends card to assignee
func TestSubscriber_StatusBlocked_SendsCard(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	assigneeID := "00000000-0000-0000-0000-000000000098"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": assigneeID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventIssueUpdated,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     userID,
		Payload: map[string]any{
			"status_changed": true,
			"issue": map[string]any{
				"id":          "issue-1",
				"title":       "Test Issue",
				"identifier":  "MUL-1",
				"status":      "blocked",
				"assignee_id": assigneeID,
			},
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message for blocked status, got %d", len(msgs))
	}
	wantBody := "Issue MUL-1 状态已变更为 已阻塞"
	if msgs[0].Body != wantBody {
		t.Errorf("expected body %q, got %q", wantBody, msgs[0].Body)
	}
}

// Test: issue:updated with status change but preference muted → drop
func TestSubscriber_StatusInReview_PrefMuted_DropsEvent(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	assigneeID := "00000000-0000-0000-0000-000000000098"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": assigneeID},
		},
	}
	prefStore := &mockPrefStore{
		prefs: map[string]map[string]string{
			assigneeID: {
				"feishu.status_in_review": "muted",
			},
		},
	}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventIssueUpdated,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     userID,
		Payload: map[string]any{
			"status_changed": true,
			"issue": map[string]any{
				"id":          "issue-1",
				"title":       "Test Issue",
				"identifier":  "MUL-1",
				"status":      "in_review",
				"assignee_id": assigneeID,
			},
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages when status_in_review pref muted, got %d", len(msgs))
	}
}

// Test: issue:updated with status change but no assignee → drop
func TestSubscriber_StatusDone_NoAssignee_DropsEvent(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": userID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventIssueUpdated,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     userID,
		Payload: map[string]any{
			"status_changed": true,
			"issue": map[string]any{
				"id":          "issue-1",
				"title":       "Test Issue",
				"identifier":  "MUL-1",
				"status":      "done",
				"assignee_id": nil,
			},
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages when no assignee, got %d", len(msgs))
	}
}

// Test: issue:updated with status change but actor is assignee → drop (self-notification)
func TestSubscriber_StatusBlocked_SelfNotification_DropsEvent(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": userID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventIssueUpdated,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     userID,
		Payload: map[string]any{
			"status_changed": true,
			"issue": map[string]any{
				"id":          "issue-1",
				"title":       "Test Issue",
				"identifier":  "MUL-1",
				"status":      "blocked",
				"assignee_id": userID,
			},
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages for self-notification on status change, got %d", len(msgs))
	}
}

// Test: issue:updated with status change to non-notify status (e.g. todo) → drop
func TestSubscriber_StatusTodo_NotNotifyWorthy_DropsEvent(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	assigneeID := "00000000-0000-0000-0000-000000000098"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": assigneeID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventIssueUpdated,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     userID,
		Payload: map[string]any{
			"status_changed": true,
			"issue": map[string]any{
				"id":          "issue-1",
				"title":       "Test Issue",
				"identifier":  "MUL-1",
				"status":      "todo",
				"assignee_id": assigneeID,
			},
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages for non-notify status, got %d", len(msgs))
	}
}

// Test: actor_id == user_id → drop (don't notify self)
func TestSubscriber_SelfNotification_DropsEvent(t *testing.T) {
	bus := events.New()
	ch := &mockChannel{name: "feishu"}
	userID := "00000000-0000-0000-0000-000000000099"
	bindingStore := &mockBindingStore{
		bindings: map[string]map[string]string{
			"feishu": {"ext-user-1": userID},
		},
	}
	prefStore := &mockPrefStore{prefs: map[string]map[string]string{}}

	sub := NewSubscriber(bus, ch, bindingStore, prefStore, "00000000-0000-0000-0000-000000000100")
	sub.Start()

	bus.Publish(events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: "00000000-0000-0000-0000-000000000100",
		ActorID:     userID,
		Payload: map[string]any{
			"user_id":    userID,
			"user_type":  "member",
			"inbox_type": "issue_assigned",
			"issue_id":   "issue-1",
			"title":      "Test Issue",
		},
	})

	time.Sleep(10 * time.Millisecond)

	msgs := ch.Messages()
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages for self-notification, got %d", len(msgs))
	}
}
