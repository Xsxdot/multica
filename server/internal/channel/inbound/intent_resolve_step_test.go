package inbound_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/channel/inbound"
	chintent "github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/util"
)

type fakeIntentResolver struct {
	name    string
	matched bool
	intent  chintent.Intent
	calls   int
	lastReq chintent.IntentRequest
}

func (f *fakeIntentResolver) Name() string { return f.name }

func (f *fakeIntentResolver) Resolve(_ context.Context, req chintent.IntentRequest) (chintent.IntentResult, error) {
	f.calls++
	f.lastReq = req
	if !f.matched {
		return chintent.IntentResult{}, nil
	}
	return chintent.IntentResult{Matched: true, Intent: f.intent}, nil
}

type fakeWorkspaceLookup struct {
	id  pgtype.UUID
	err error
}

func (f fakeWorkspaceLookup) LookupWorkspaceID(context.Context, string, string) (pgtype.UUID, error) {
	return f.id, f.err
}

func TestIntentResolveStep_PriorityShortCircuits(t *testing.T) {
	t.Parallel()

	first := &fakeIntentResolver{
		name:    "command",
		matched: true,
		intent: chintent.Intent{
			Kind:       chintent.IntentCreateIssue,
			Confidence: 1,
			Params:     map[string]string{"title": "from command"},
			Source:     chintent.SourceCommand,
		},
	}
	second := &fakeIntentResolver{
		name:    "chat",
		matched: true,
		intent:  chintent.Intent{Kind: chintent.IntentCreateIssue, Source: chintent.SourceChat},
	}

	step := inbound.NewIntentResolveStep(first, second)
	evt, d, err := step.Run(context.Background(), port.InboundEvent{Text: "/create x"})
	if err != nil {
		t.Fatal(err)
	}
	if d != inbound.DecisionContinue {
		t.Fatalf("decision = %v", d)
	}
	if first.calls != 1 {
		t.Fatalf("first calls = %d, want 1", first.calls)
	}
	if second.calls != 0 {
		t.Fatalf("second calls = %d, want 0", second.calls)
	}
	if evt.Intent.Source != port.SourceCommand {
		t.Fatalf("source = %q, want command", evt.Intent.Source)
	}
	if evt.Intent.Params["title"] != "from command" {
		t.Fatalf("title = %q", evt.Intent.Params["title"])
	}
}

func TestIntentResolveStep_FallsThroughToChat(t *testing.T) {
	t.Parallel()

	rule := &fakeIntentResolver{name: "rule"}
	chat := &fakeIntentResolver{
		name:    "chat",
		matched: true,
		intent: chintent.Intent{
			Kind:       chintent.IntentCreateIssue,
			Confidence: 0.9,
			Params:     map[string]string{"title": "from chat"},
			Source:     chintent.SourceChat,
		},
	}

	step := inbound.NewIntentResolveStep(rule, chat)
	evt, _, err := step.Run(context.Background(), port.InboundEvent{Text: "自然语言"})
	if err != nil {
		t.Fatal(err)
	}
	if rule.calls != 1 || chat.calls != 1 {
		t.Fatalf("calls rule=%d chat=%d, want 1/1", rule.calls, chat.calls)
	}
	if evt.Intent.Source != port.SourceChat {
		t.Fatalf("source = %q, want chat", evt.Intent.Source)
	}
	if evt.Intent.Params["title"] != "from chat" {
		t.Fatalf("title = %q", evt.Intent.Params["title"])
	}
}

func TestIntentResolveStep_NoMatchUnknown(t *testing.T) {
	t.Parallel()

	step := inbound.NewIntentResolveStep(&fakeIntentResolver{name: "rule"})
	evt, _, err := step.Run(context.Background(), port.InboundEvent{Text: "??"})
	if err != nil {
		t.Fatal(err)
	}
	if evt.Intent.Kind != port.IntentUnknown {
		t.Fatalf("kind = %q, want Unknown", evt.Intent.Kind)
	}
	if evt.Intent.Source != port.SourceRule {
		t.Fatalf("source = %q, want rule", evt.Intent.Source)
	}
	if evt.Intent.Params == nil {
		t.Fatal("params should be non-nil")
	}
}

func TestIntentResolveStep_PassesWorkspaceID(t *testing.T) {
	t.Parallel()

	wsID := util.MustParseUUID("550e8400-e29b-41d4-a716-446655440000")
	resolver := &fakeIntentResolver{
		name:    "chat",
		matched: true,
		intent:  chintent.Intent{Kind: chintent.IntentUnknown, Source: chintent.SourceChat},
	}
	step := inbound.NewIntentResolveStepWithWorkspace(fakeWorkspaceLookup{id: wsID}, resolver)

	if _, _, err := step.Run(context.Background(), port.InboundEvent{
		ChannelName: "feishu",
		ChatID:      "chat-1",
		Text:        "自然语言",
	}); err != nil {
		t.Fatal(err)
	}

	if resolver.lastReq.WorkspaceID != util.UUIDToString(wsID) {
		t.Fatalf("workspace id = %q, want %q", resolver.lastReq.WorkspaceID, util.UUIDToString(wsID))
	}
}
