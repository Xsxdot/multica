package intent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	defaultChatTimeout = 8 * time.Second
	minChatConfidence  = 0.7
)

// IntentRequest is the stable input every resolver sees.
type IntentRequest struct {
	WorkspaceID      string
	DefaultProjectID string
	Text             string
	Channel          string
	ChatID           string
	ChatType         string
	SenderID         string
	SenderName       string
	InboundEventID   string
	SourceHint       IntentSource
}

// IntentResult is a resolver's answer. Matched=false lets the chain continue.
type IntentResult struct {
	Matched bool
	Intent  Intent
	Reply   string
}

// IntentResolver turns a channel message into one structured Intent.
type IntentResolver interface {
	Name() string
	Resolve(ctx context.Context, req IntentRequest) (IntentResult, error)
}

type RuleResolver struct {
	matcher RuleMatcher
}

func NewRuleResolver(matcher RuleMatcher) *RuleResolver {
	if matcher == nil {
		matcher = NewRuleMatcher()
	}
	return &RuleResolver{matcher: matcher}
}

func (*RuleResolver) Name() string { return "rule" }

func (r *RuleResolver) Resolve(_ context.Context, req IntentRequest) (IntentResult, error) {
	in, ok := r.matcher.Match(req.Text)
	if !ok {
		return IntentResult{}, nil
	}
	if req.SourceHint == SourceCommand {
		in.Source = SourceCommand
	}
	return IntentResult{Matched: true, Intent: in}, nil
}

// ChatIntentClient is the synchronous semantic parser behind ChatIntentResolver.
// Production implementations may use daemon-backed chat/agent infrastructure,
// but they still return raw text only; this package validates the JSON shape
// before any dispatcher can act on it.
type ChatIntentClient interface {
	CompleteIntent(ctx context.Context, req IntentRequest) (string, error)
}

type AsyncChatIntentClient interface {
	StartIntent(ctx context.Context, req IntentRequest) (string, error)
	ParseIntentResult(ctx context.Context, taskID string) (IntentResult, bool, error)
}

type ChatIntentResolver struct {
	client  ChatIntentClient
	timeout time.Duration
}

type ChatIntentResolverConfig struct {
	Client  ChatIntentClient
	Timeout time.Duration
}

func NewChatIntentResolver(cfg ChatIntentResolverConfig) *ChatIntentResolver {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultChatTimeout
	}
	return &ChatIntentResolver{client: cfg.Client, timeout: timeout}
}

func (*ChatIntentResolver) Name() string { return "chat" }

func (r *ChatIntentResolver) Resolve(ctx context.Context, req IntentRequest) (IntentResult, error) {
	if r.client == nil || strings.TrimSpace(req.Text) == "" || strings.TrimSpace(req.WorkspaceID) == "" {
		return IntentResult{}, nil
	}

	callCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	raw, err := r.client.CompleteIntent(callCtx, req)
	if err != nil {
		return IntentResult{Matched: true, Intent: fallbackIntent(IntentUnknown)}, nil
	}

	result, err := NormalizeChatIntentResult(raw)
	if err != nil {
		return IntentResult{Matched: true, Intent: fallbackIntent(IntentUnknown)}, nil
	}
	return result, nil
}

type chatIntentResponse struct {
	Intent     string            `json:"intent"`
	Confidence float64           `json:"confidence"`
	Params     map[string]string `json:"params"`
}

func BuildChatIntentPrompt(req IntentRequest) string {
	var b strings.Builder
	b.WriteString("You are resolving a Multica channel chat message into one safe structured intent.\n")
	b.WriteString("Return only JSON: {\"intent\":\"<IntentKind>\",\"confidence\":0.0-1.0,\"params\":{...}}\n")
	b.WriteString("Allowed intents: CreateIssue, AddComment, QueryIssue, SetStatus, SetAssignee, SetPriority, SetLabel, Unsupported, Unknown, ASK_CLARIFY.\n")
	b.WriteString("Destructive operations such as delete must be Unsupported. Do not execute anything.\n\n")
	fmt.Fprintf(&b, "Workspace ID: %s\nDefault project ID: %s\nChannel: %s\nChat type: %s\nSender: %s (%s)\n\n", req.WorkspaceID, req.DefaultProjectID, req.Channel, req.ChatType, req.SenderName, req.SenderID)
	fmt.Fprintf(&b, "User message:\n%s\n", req.Text)
	return b.String()
}

func parseChatIntent(raw string) (Intent, error) {
	var resp chatIntentResponse
	if err := json.Unmarshal([]byte(stripMarkdownFences(raw)), &resp); err != nil {
		return Intent{}, err
	}
	kind := IntentKind(resp.Intent)
	if kind == IntentDelete {
		kind = IntentUnsupported
	}
	if !isValidIntentKind(kind) {
		return Intent{}, fmt.Errorf("unknown intent kind %q", resp.Intent)
	}
	if resp.Confidence < 0 {
		resp.Confidence = 0
	}
	if resp.Confidence > 1 {
		resp.Confidence = 1
	}
	params := resp.Params
	if params == nil {
		params = map[string]string{}
	}
	return Intent{Kind: kind, Confidence: resp.Confidence, Params: params, Source: SourceChat}, nil
}

func NormalizeChatIntentResult(raw string) (IntentResult, error) {
	in, err := parseChatIntent(raw)
	if err != nil {
		return IntentResult{}, err
	}
	if in.Confidence < minChatConfidence {
		return IntentResult{Matched: true, Intent: fallbackIntent(IntentASKClarify)}, nil
	}
	if !intentHasRequiredParams(in) {
		return IntentResult{Matched: true, Intent: fallbackIntent(IntentASKClarify)}, nil
	}
	in.Source = SourceChat
	return IntentResult{Matched: true, Intent: in}, nil
}

func fallbackIntent(kind IntentKind) Intent {
	return Intent{Kind: kind, Confidence: 0, Params: map[string]string{}, Source: SourceChat}
}

func isValidIntentKind(k IntentKind) bool {
	switch k {
	case IntentCreateIssue, IntentAddComment, IntentQueryIssue,
		IntentSetStatus, IntentSetAssignee, IntentSetPriority, IntentSetLabel,
		IntentUnsupported, IntentUnknown, IntentASKClarify:
		return true
	default:
		return false
	}
}

func intentHasRequiredParams(in Intent) bool {
	switch in.Kind {
	case IntentCreateIssue:
		return strings.TrimSpace(in.Params["title"]) != ""
	case IntentAddComment:
		return strings.TrimSpace(in.Params["issue_key"]) != "" && strings.TrimSpace(in.Params["comment"]) != ""
	case IntentSetStatus:
		return strings.TrimSpace(in.Params["issue_key"]) != "" && strings.TrimSpace(in.Params["status"]) != ""
	case IntentSetAssignee:
		return strings.TrimSpace(in.Params["issue_key"]) != "" && strings.TrimSpace(in.Params["assignee"]) != ""
	case IntentSetPriority:
		return strings.TrimSpace(in.Params["issue_key"]) != "" && strings.TrimSpace(in.Params["priority"]) != ""
	case IntentSetLabel:
		return strings.TrimSpace(in.Params["issue_key"]) != "" &&
			strings.TrimSpace(in.Params["label"]) != "" &&
			(in.Params["op"] == "add" || in.Params["op"] == "remove")
	default:
		return true
	}
}

func stripMarkdownFences(content string) string {
	s := strings.TrimSpace(content)
	if strings.HasPrefix(s, "```json") {
		s = strings.TrimPrefix(s, "```json")
	} else if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```")
	}
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "```") {
		s = strings.TrimSuffix(s, "```")
	}
	return strings.TrimSpace(s)
}
