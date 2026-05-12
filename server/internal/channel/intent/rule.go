// Package intent implements structured channel intent resolution.
package intent

import "strings"

// IntentSource identifies how an intent was recognised.
type IntentSource string

const (
	// SourceCommand means a slash/command path produced this intent.
	SourceCommand IntentSource = "command"
	// SourceRule means the regex rule engine produced this intent.
	SourceRule IntentSource = "rule"
	// SourceChat means the chat semantic resolver produced this intent.
	SourceChat IntentSource = "chat"
)

// IntentKind is the high-level command category per PRD F5 / outbound tests.
type IntentKind string

const (
	IntentCreateIssue   IntentKind = "CreateIssue"
	IntentAddComment    IntentKind = "AddComment"
	IntentQueryIssue    IntentKind = "QueryIssue"
	IntentIssueDetail   IntentKind = "IssueDetail"
	IntentIssueTimeline IntentKind = "IssueTimeline"
	IntentIssueLogs     IntentKind = "IssueLogs"
	IntentSetStatus     IntentKind = "SetStatus"
	IntentSetAssignee   IntentKind = "SetAssignee"
	IntentSetPriority   IntentKind = "SetPriority"
	IntentSetLabel      IntentKind = "SetLabel"
	IntentConfirmAction IntentKind = "ConfirmAction"
	IntentCancelAction  IntentKind = "CancelAction"
	IntentDelete        IntentKind = "Delete"
	IntentUnsupported   IntentKind = "Unsupported"
	IntentUnknown       IntentKind = "Unknown"
	IntentASKClarify    IntentKind = "ASK_CLARIFY"
)

// Intent is the rule engine output attached to an inbound message before
// dispatch (T11). Params keys are stable lowercase_snake for facade wiring.
type Intent struct {
	Kind       IntentKind
	Confidence float64
	Params     map[string]string
	Source     IntentSource
}

// RuleMatcher matches normalised user text (mention markers already stripped).
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
