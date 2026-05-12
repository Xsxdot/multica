package intent

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	defaultChatTimeout = 8 * time.Second
	minChatConfidence  = 0.7
)

var (
	legalIssueKeyRe = regexp.MustCompile(`(?i)\b[A-Z]{2,5}-[1-9][0-9]*\b`)
	issueLikeKeyRe  = regexp.MustCompile(`(?i)\b[A-Z]+-\d+\b`)
)

// IntentRequest is the stable input every resolver sees.
type IntentRequest struct {
	WorkspaceID      string
	DefaultProjectID string
	// AgentID, when non-empty, forces channel intent to use that agent only
	// (no fallback to another agent if unavailable).
	AgentID        string
	Text           string
	Channel        string
	ConnectionID   string
	ChatID         string
	ChatType       string
	SenderID       string
	SenderName     string
	InboundEventID string
	SourceHint     IntentSource
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

	result, err := NormalizeChatIntentResultForRequest(raw, req)
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
	b.WriteString("Allowed intents: CreateIssue, AddComment, QueryIssue, IssueDetail, IssueTimeline, IssueLogs, SetStatus, SetAssignee, SetPriority, SetLabel, ConfirmAction, CancelAction, Unsupported, Unknown, ASK_CLARIFY.\n")
	b.WriteString("Destructive operations such as delete must be Unsupported. Do not execute anything.\n\n")
	b.WriteString("Rules:\n")
	b.WriteString("- If the message contains an issue key such as sta-1 or STA-1, return it as uppercase params.issue_key.\n")
	b.WriteString("- QueryIssue without params.issue_key is only for todo-list requests such as 我的待办, 待办列表, 看一下待办, 我有哪些待办.\n")
	b.WriteString("- If the user appears to ask about a specific issue but the issue key or action is unclear, return ASK_CLARIFY instead of QueryIssue.\n\n")
	fmt.Fprintf(&b, "Workspace ID: %s\nDefault project ID: %s\nChannel: %s\nConnection ID: %s\nChat type: %s\nSender: %s (%s)\n\n", req.WorkspaceID, req.DefaultProjectID, req.Channel, req.ConnectionID, req.ChatType, req.SenderName, req.SenderID)
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
	if issueKey := strings.TrimSpace(params["issue_key"]); issueKey != "" {
		params["issue_key"] = keyParam(issueKey)
	}
	return Intent{Kind: kind, Confidence: resp.Confidence, Params: params, Source: SourceChat}, nil
}

func NormalizeChatIntentResult(raw string) (IntentResult, error) {
	return NormalizeChatIntentResultForText(raw, "")
}

func NormalizeChatIntentResultForRequest(raw string, req IntentRequest) (IntentResult, error) {
	return NormalizeChatIntentResultForText(raw, req.Text)
}

func NormalizeChatIntentResultForText(raw string, sourceText string) (IntentResult, error) {
	in, err := parseChatIntent(raw)
	if err != nil {
		return IntentResult{}, err
	}
	if in.Confidence < minChatConfidence {
		return IntentResult{Matched: true, Intent: fallbackIntent(IntentASKClarify)}, nil
	}
	in = refineChatIntentWithSourceText(in, sourceText)
	if !intentHasRequiredParams(in) {
		return IntentResult{Matched: true, Intent: fallbackIntent(IntentASKClarify)}, nil
	}
	in.Source = SourceChat
	return IntentResult{Matched: true, Intent: in}, nil
}

func refineChatIntentWithSourceText(in Intent, sourceText string) Intent {
	if in.Kind != IntentQueryIssue {
		return in
	}
	if in.Params == nil {
		in.Params = map[string]string{}
	}
	if issueKey := strings.TrimSpace(in.Params["issue_key"]); issueKey != "" {
		in.Params["issue_key"] = keyParam(issueKey)
		return in
	}

	text := strings.TrimSpace(sourceText)
	if text == "" {
		return in
	}
	keys := extractIssueKeys(text)
	issueLikes := extractIssueLikeKeys(text)
	if len(keys) == 1 && len(issueLikes) == 1 {
		in.Params["issue_key"] = keys[0]
		return in
	}
	if len(keys) > 0 || len(issueLikes) > 0 || !isTodoListQuery(text) {
		return fallbackIntent(IntentASKClarify)
	}
	return in
}

func extractIssueKeys(text string) []string {
	return uniqueNormalizedMatches(legalIssueKeyRe.FindAllString(text, -1))
}

func extractIssueLikeKeys(text string) []string {
	return uniqueNormalizedMatches(issueLikeKeyRe.FindAllString(text, -1))
}

func uniqueNormalizedMatches(matches []string) []string {
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(matches))
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		key := keyParam(match)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func isTodoListQuery(text string) bool {
	compact := strings.ToLower(strings.Join(strings.Fields(text), ""))
	compact = strings.Trim(compact, "？?！!.。")
	switch compact {
	case "我的待办", "待办列表", "看一下待办", "我有哪些待办":
		return true
	default:
		return false
	}
}

func fallbackIntent(kind IntentKind) Intent {
	return Intent{Kind: kind, Confidence: 0, Params: map[string]string{}, Source: SourceChat}
}

func isValidIntentKind(k IntentKind) bool {
	switch k {
	case IntentCreateIssue, IntentAddComment, IntentQueryIssue, IntentIssueDetail, IntentIssueTimeline, IntentIssueLogs,
		IntentSetStatus, IntentSetAssignee, IntentSetPriority, IntentSetLabel,
		IntentConfirmAction, IntentCancelAction,
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
	case IntentIssueDetail, IntentIssueTimeline, IntentIssueLogs:
		return strings.TrimSpace(in.Params["issue_key"]) != ""
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
	case IntentConfirmAction, IntentCancelAction:
		return strings.TrimSpace(in.Params["code"]) != ""
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
