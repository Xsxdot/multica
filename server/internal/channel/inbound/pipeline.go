package inbound

import (
	"context"

	"github.com/multica-ai/multica/server/internal/channel/port"
)

// Decision communicates how the Pipeline should proceed after a Step
// completes. Errors travel via the Step.Run error return value, not via
// Decision, so the two exit paths are typed separately (DESIGN §3.1).
type Decision int

const (
	// DecisionContinue advances the Pipeline to the next Step.
	DecisionContinue Decision = iota

	// DecisionSkip terminates the Pipeline without invoking any further
	// Step. Skip is the contract a Step uses to short-circuit cleanly,
	// e.g. dedup detecting a replay or identity-bind detecting an
	// unbound user it has already prompted out-of-band.
	DecisionSkip
)

// String renders Decision as a stable human label so test failures and
// log lines do not print bare integers.
func (d Decision) String() string {
	switch d {
	case DecisionContinue:
		return "Continue"
	case DecisionSkip:
		return "Skip"
	default:
		return "Decision(?)"
	}
}

// Step is the unit of work composed by Pipeline. Each Step has a stable
// Name() — used by Outcome.Terminal so the caller / telemetry can label
// the step that terminated the pipeline — and a Run method that
// processes a single InboundEvent.
//
// Run returns the (possibly mutated) event, a Decision, and an error.
// The pipeline aborts with that error if it is non-nil; otherwise the
// Decision determines whether the next step runs (Continue) or the
// pipeline terminates here (Skip).
type Step interface {
	Name() string
	Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error)
}

// Outcome captures how a Pipeline.Run invocation finished. It is only
// meaningful when Run returned a nil error; on error the caller MUST
// ignore Outcome (zero value).
//
// Terminal is the Name() of the last Step the Pipeline executed (the
// one that returned the final Decision). Decision is that Step's
// Decision — either DecisionContinue (i.e. all steps ran to completion)
// or DecisionSkip (i.e. that step short-circuited the rest).
type Outcome struct {
	Terminal string
	Decision Decision
}

// Pipeline runs a fixed, ordered list of Steps over a single
// InboundEvent. The list is captured at construction time and is
// immutable thereafter; Pipeline is therefore safe for concurrent use
// as long as every Step it holds is itself safe for concurrent use.
//
// The intent-recog and dispatch placeholders shipped in M1 (T6) are
// drop-in replaceable in T9–T11 by registering different Step
// implementations against the same constructor — the orchestration
// here does not need to change when intent recognition lands.
type Pipeline struct {
	steps []Step
}

// NewPipeline composes the supplied steps into a Pipeline. Steps run in
// the supplied order; passing zero steps is valid (Run will return a
// zero Outcome and a nil error) but uncommon outside tests.
func NewPipeline(steps ...Step) *Pipeline {
	// Defensive copy — callers occasionally hold the slice they passed
	// in and mutate it later; we don't want that mutation observable
	// from inside Run.
	cp := make([]Step, len(steps))
	copy(cp, steps)
	return &Pipeline{steps: cp}
}

// Run executes each Step in order, threading the event through them.
// It stops at the first Step that returns DecisionSkip or a non-nil
// error. On error, the returned Outcome is the zero value and the
// caller must inspect only the error.
func (p *Pipeline) Run(ctx context.Context, evt port.InboundEvent) (Outcome, error) {
	var (
		outcome Outcome
		err     error
		d       Decision
	)
	for _, s := range p.steps {
		evt, d, err = s.Run(ctx, evt)
		if err != nil {
			return Outcome{}, err
		}
		outcome = Outcome{Terminal: s.Name(), Decision: d}
		if d == DecisionSkip {
			return outcome, nil
		}
	}
	return outcome, nil
}
