package inbound

import (
	"context"

	"github.com/multica-ai/multica/server/internal/channel/port"
)

// This file holds the Step implementations whose real behaviour lives
// outside T6:
//
//   - identity-bind  → T8 (looks up channel_user_binding, pushes the
//                          one-shot binding link via the registry on miss)
//   - intent-recog   → T9–T10 (parses the user message into IntentKind)
//   - dispatch       → T11 (routes IntentKind to the appropriate handler)
//   - reply          → T11 (telemetry / reply finalisation)
//
// In M1 every one of these is a no-op that returns Continue, so the
// pipeline orchestration (Pipeline.Run, dedup short-circuit, the named
// telemetry labels carried in Outcome.Terminal) can be exercised end
// to end before the real logic lands. Replacing one of these with a
// real implementation in a later milestone requires zero changes to
// pipeline.go — the wiring code in cmd/server/main.go simply swaps
// the constructor.
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
// every event is treated as an unknown intent (which the dispatch
// placeholder happily forwards as Continue).
func NewIntentRecogStep() Step { return passthroughStep{name: "intent-recog"} }

// NewDispatchStep returns the M1 placeholder dispatch Step. T11 will
// replace it with the real intent → handler router; for now no reply
// is produced, so downstream Steps still see the original event.
func NewDispatchStep() Step { return passthroughStep{name: "dispatch"} }

// NewReplyStep returns the M1 placeholder reply Step. The real reply
// step is responsible for finalising telemetry (counters / log lines)
// after dispatch has produced its output; in M1 it is a no-op so
// Pipeline.Run can declare a stable terminal step name.
func NewReplyStep() Step { return passthroughStep{name: "reply"} }
