package inbound

import (
	"context"

	"github.com/multica-ai/multica/server/internal/channel/port"
)

// This file holds the Step implementations whose real behaviour lives
// outside T6:
//
//   - identity-bind  → T8 (looks up channel_user_binding; on miss,
//                          issues a binding token via
//                          binding.TokenIssuer and pushes the one-shot
//                          link via channel.Registry.Get(provider).Send)
//   - intent-recog   → T9–T10 (parses the user message into IntentKind
//                              per PRD F5 via resolver chain)
//   - dispatch       → T11 (routes IntentKind to the appropriate
//                            facade.IssueFacade / facade.CommentFacade
//                            handler)
//   - reply          → T11 (finalises telemetry / log lines after
//                            dispatch has produced its output)
//
// In M1 every one of these is a no-op that returns Continue, so the
// pipeline orchestration (Pipeline.Run, dedup short-circuit, the named
// telemetry labels carried in Outcome.Terminal) can be exercised end
// to end before the real logic lands. The Step interface is the only
// surface the wiring code (cmd/server/main.go) couples against:
// replacing a placeholder is a one-line constructor swap there, with
// zero changes to pipeline.go or to upstream Steps.
//
// The four placeholders share the same passthrough shape, so they're
// all built from a tiny common type rather than duplicated.

// passthroughStep is a Step that always returns Continue and carries a
// caller-supplied Name. It exists exclusively for the M1 placeholder
// constructors below; production implementations should be their own
// types so they can hold dependencies (db, registry, …).
type passthroughStep struct {
	name string
}

func (s passthroughStep) Name() string { return s.name }

func (s passthroughStep) Run(_ context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	return evt, DecisionContinue, nil
}

// NewIdentityBindStep returns the M1 placeholder identity-bind Step.
//
// The real M2+ implementation will:
//   - look up (provider, external_user_id) in channel_user_binding;
//   - on hit, fill evt.SenderID / a future MulticaUserID slot and
//     return Continue;
//   - on miss, issue a binding token via binding.TokenIssuer, push
//     the one-shot link via the appropriate port.Channel.Send, and
//     return Skip.
//
// Both paths require dependencies (db.Queries, channel.Registry,
// binding.TokenIssuer) that the M1 wiring task injects.
func NewIdentityBindStep() Step { return passthroughStep{name: "identity-bind"} }

// NewIntentRecogStep returns the M1 placeholder intent-recog Step.
// T9–T10 will replace it with the real PRD F5 intent parser; for now
// every event is tagged IntentUnknown (which the dispatch placeholder
// forwards as Continue).
func NewIntentRecogStep() Step { return &intentRecogPlaceholder{} }

type intentRecogPlaceholder struct{}

func (intentRecogPlaceholder) Name() string { return "intent-recog" }

func (intentRecogPlaceholder) Run(_ context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	evt.Intent = port.InboundIntent{
		Kind:       port.IntentUnknown,
		Confidence: 1,
		Params:     map[string]string{},
		Source:     port.SourceRule,
	}
	return evt, DecisionContinue, nil
}

// NewDispatchStep has been replaced by the real implementation in
// dispatcher.go (T11). The production constructor takes a DispatchConfig.

// NewReplyStep returns the M1 placeholder reply Step. The real reply
// step is responsible for finalising telemetry (counters / log lines)
// after dispatch has produced its output; in M1 it is a no-op so
// Pipeline.Run can declare a stable terminal step name.
func NewReplyStep() Step { return passthroughStep{name: "reply"} }

// Compile-time interface conformance for the placeholder shape. A
// drift in the Step signature (e.g. an added Run argument) surfaces
// here rather than at every call site.
var _ Step = passthroughStep{}
