package inbound

import (
	"context"

	chintent "github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

type ruleIntentStep struct{}

func NewRuleIntentStep() Step {
	return ruleIntentStep{}
}

func (ruleIntentStep) Name() string { return "intent-recog" }

func (ruleIntentStep) Run(_ context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	m := chintent.NewRuleMatcher()
	if intent, ok := m.Match(evt.Text); ok {
		evt.Intent = port.InboundIntent{
			Kind:       port.IntentKind(intent.Kind),
			Confidence: intent.Confidence,
			Params:     intent.Params,
			Source:     port.SourceRule,
		}
		return evt, DecisionContinue, nil
	}
	evt.Intent = port.InboundIntent{Kind: port.IntentUnknown, Source: port.SourceRule, Params: map[string]string{}}
	return evt, DecisionContinue, nil
}

var _ Step = ruleIntentStep{}
