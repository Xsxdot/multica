// Package intent implements the zero-token rule matcher for channel inbound
// messages (DESIGN §8 T9). Regex hits are bounded to sub-millisecond CPU work;
// LLM fallback lives elsewhere (T10).
package intent

import "strings"

// IntentSource identifies how an intent was recognised.
type IntentSource string

const (
	// SourceRule means the regex rule engine produced this intent.
	SourceRule IntentSource = "rule"
)

// IntentKind is the high-level command category per PRD F5 / outbound tests.
type IntentKind string

const (
	IntentCreateIssue IntentKind = "CreateIssue"
	IntentAddComment  IntentKind = "AddComment"
	IntentQueryIssue  IntentKind = "QueryIssue"
	IntentSetStatus   IntentKind = "SetStatus"
	IntentUnsupported IntentKind = "Unsupported"
	IntentUnknown     IntentKind = "Unknown"
)

// Intent is the rule engine output attached to an inbound message before
// dispatch (T11). Params keys are stable lowercase_snake for facade wiring.
type Intent struct {
	Kind       IntentKind
	Confidence float64
	Params     map[string]string
	Source     IntentSource
}

// RuleMatcher matches normalised user text (mention markers already stripped)
// against ordered regex rules. A false second return means “no rule hit” so
// upstream can fall back to LLM recognition (T10).
type RuleMatcher interface {
	Match(text string) (Intent, bool)
}

type chainMatcher struct {
	rules []rule
}

// NewRuleMatcher returns the production RuleMatcher with DESIGN §8 T9 default rules.
func NewRuleMatcher() RuleMatcher {
	return &chainMatcher{rules: defaultRules()}
}

func (m *chainMatcher) Match(text string) (Intent, bool) {
	s := strings.TrimSpace(text)
	if s == "" {
		return Intent{}, false
	}
	for _, r := range m.rules {
		sub := r.re.FindStringSubmatch(s)
		if sub == nil {
			continue
		}
		params := r.params(sub)
		if params == nil {
			params = map[string]string{}
		}
		return Intent{
			Kind:       r.kind,
			Confidence: r.confidence,
			Params:     params,
			Source:     SourceRule,
		}, true
	}
	return Intent{}, false
}
