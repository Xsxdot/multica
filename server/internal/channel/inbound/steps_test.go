package inbound_test

// Tests for the named placeholder Steps that compose the M1 pipeline:
//
//   normalize → dedup → identity-bind → intent-recog → dispatch → reply
//
// Coverage:
//   - normalize (the only step with real behaviour in T6): empty EventID
//     short-circuits with Skip; well-formed events Continue.
//   - identity-bind / intent-recog / dispatch / reply: M1 placeholders
//     that return Continue and announce a stable Name(). T8/T9-T11 will
//     replace these without needing to touch the orchestration code.
//
// These tests deliberately reference symbols that do not yet exist
// (NewNormalizeStep / NewIdentityBindStep / NewIntentRecogStep /
// NewDispatchStep / NewReplyStep) so the build fails until the Green
// phase ships them.

import (
	"context"
	"testing"

	"github.com/multica-ai/multica/server/internal/channel/inbound"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

// ---------------------------------------------------------------------------
// normalize: defensive validation. Issue §"步骤 1 normalize" requires that
// an event with an empty EventID / ChatID / SenderID short-circuit with
// Skip; otherwise the event passes through unchanged and the pipeline
// continues.
// ---------------------------------------------------------------------------

func TestNormalizeStep_Name(t *testing.T) {
	t.Parallel()

	if got := inbound.NewNormalizeStep().Name(); got != "normalize" {
		t.Errorf("Name = %q, want %q", got, "normalize")
	}
}

func TestNormalizeStep_WellFormedEvent_Continues(t *testing.T) {
	t.Parallel()

	step := inbound.NewNormalizeStep()
	evt := port.InboundEvent{
		ChannelName: "feishu",
		EventID:     "evt-1",
		ChatID:      "chat-1",
		SenderID:    "sender-1",
		Type:        port.EventTypeMessageReceived,
	}
	out, d, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if d != inbound.DecisionContinue {
		t.Errorf("decision = %v, want Continue", d)
	}
	// The event must be returned unchanged (normalize is defensive
	// validation, not transformation, in T6). Compare field-by-field
	// because port.InboundEvent contains a json.RawMessage (slice)
	// which is not == comparable.
	if out.ChannelName != evt.ChannelName ||
		out.EventID != evt.EventID ||
		out.ChatID != evt.ChatID ||
		out.SenderID != evt.SenderID ||
		out.Type != evt.Type {
		t.Errorf("event was mutated: got %+v, want %+v", out, evt)
	}
}

func TestNormalizeStep_EmptyEventID_Skips(t *testing.T) {
	t.Parallel()

	step := inbound.NewNormalizeStep()
	_, d, err := step.Run(context.Background(), port.InboundEvent{
		ChatID:   "c",
		SenderID: "s",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if d != inbound.DecisionSkip {
		t.Errorf("decision = %v, want Skip", d)
	}
}

func TestNormalizeStep_EmptyChatID_Skips(t *testing.T) {
	t.Parallel()

	step := inbound.NewNormalizeStep()
	_, d, err := step.Run(context.Background(), port.InboundEvent{
		EventID:  "e",
		SenderID: "s",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if d != inbound.DecisionSkip {
		t.Errorf("decision = %v, want Skip", d)
	}
}

func TestNormalizeStep_EmptySenderID_Skips(t *testing.T) {
	t.Parallel()

	step := inbound.NewNormalizeStep()
	_, d, err := step.Run(context.Background(), port.InboundEvent{
		EventID: "e",
		ChatID:  "c",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if d != inbound.DecisionSkip {
		t.Errorf("decision = %v, want Skip", d)
	}
}

// ---------------------------------------------------------------------------
// Placeholder steps (identity-bind, intent-recog, dispatch, reply) — each
// must declare a stable Name() (so Outcome.Terminal is meaningful when one
// of them happens to be the last step the pipeline runs) and return
// Continue. Behaviour is filled in by T8 / T9–T11.
// ---------------------------------------------------------------------------

func TestPlaceholderSteps_NameAndDecision(t *testing.T) {
	t.Parallel()

	cases := []struct {
		wantName string
		step     inbound.Step
	}{
		{"identity-bind", inbound.NewIdentityBindStep()},
		{"intent-recog", inbound.NewIntentRecogStep()},
		{"dispatch", inbound.NewDispatchStep(inbound.DispatchConfig{})},
		{"reply", inbound.NewReplyStep()},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.wantName, func(t *testing.T) {
			t.Parallel()
			if got := tc.step.Name(); got != tc.wantName {
				t.Errorf("Name = %q, want %q", got, tc.wantName)
			}
			_, d, err := tc.step.Run(context.Background(), port.InboundEvent{
				EventID:  "e",
				ChatID:   "c",
				SenderID: "s",
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if d != inbound.DecisionContinue {
				t.Errorf("decision = %v, want Continue (placeholder always continues)", d)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// End-to-end smoke: when all five named steps + the dedup step are wired
// in the documented order, a well-formed event runs every step in order
// and Outcome.Terminal is "reply".
// ---------------------------------------------------------------------------

func TestPipeline_AllPlaceholderSteps_RunInOrder(t *testing.T) {
	t.Parallel()

	store := &fakeDedupStore{responses: []dedupResp{{Inserted: true}}}

	p := inbound.NewPipeline(
		inbound.NewNormalizeStep(),
		inbound.NewDedupStep(store),
		inbound.NewIdentityBindStep(),
		inbound.NewIntentRecogStep(),
		inbound.NewDispatchStep(inbound.DispatchConfig{}),
		inbound.NewReplyStep(),
	)
	out, err := p.Run(context.Background(), port.InboundEvent{
		ChannelName: "feishu",
		EventID:     "evt-e2e",
		ChatID:      "chat-e2e",
		SenderID:    "sender-e2e",
		Type:        port.EventTypeMessageReceived,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Terminal != "reply" {
		t.Errorf("Terminal = %q, want %q", out.Terminal, "reply")
	}
	if out.Decision != inbound.DecisionContinue {
		t.Errorf("Decision = %v, want Continue", out.Decision)
	}
}
