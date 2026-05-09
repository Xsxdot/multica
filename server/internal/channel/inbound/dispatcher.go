package inbound

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/channel"
	"github.com/multica-ai/multica/server/internal/channel/facade"
	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/util"
)

const (
	replyIssueCreated       = "ISSUE_CREATED"
	replyCommentAdded       = "COMMENT_ADDED"
	replyStatusChanged      = "STATUS_CHANGED"
	replyAssigneeChanged    = "ASSIGNEE_CHANGED"
	replyPriorityChanged    = "PRIORITY_CHANGED"
	replyLabelAdded         = "LABEL_ADDED"
	replyLabelRemoved       = "LABEL_REMOVED"
	replyUnsupportedOp      = "UNSUPPORTED_OP"
	replyUnknown            = "UNKNOWN"
	replyAskClarify         = "ASK_CLARIFY"
	replyIgnoredSuffix      = "IGNORED_SUFFIX"
	replyMissingParam       = "MISSING_PARAM"
	replyIssueNotFound      = "ISSUE_NOT_FOUND"
	replyInternalError      = "INTERNAL_ERROR"
	replyPrivateUnsupported = "PRIVATE_UNSUPPORTED"
	replyMessageRecalled    = "MESSAGE_RECALLED"
)

type ChatBindingLookup interface {
	LookupWorkspaceID(ctx context.Context, channelName, chatID string) (pgtype.UUID, error)
}

type UserInfoResolver interface {
	Resolve(ctx context.Context, channelName, externalUserID string) (ResolvedUser, error)
}

type ProjectWorkspaceValidator interface {
	ValidateProjectInWorkspace(ctx context.Context, workspaceID, projectID pgtype.UUID) error
}

type ResolvedUser struct {
	MulticaUserID pgtype.UUID
	DisplayName   string
}

type DispatchConfig struct {
	IssueFacade      facade.IssueFacade
	CommentFacade    facade.CommentFacade
	Registry         *channel.Registry
	ChatBinding      ChatBindingLookup
	UserResolver     UserInfoResolver
	ProjectValidator ProjectWorkspaceValidator
	DispatchStore    DispatchCompletionStore
}

type dispatchStep struct {
	cfg DispatchConfig
}

func NewDispatchStep(cfg DispatchConfig) Step {
	return &dispatchStep{cfg: cfg}
}

func (dispatchStep) Name() string { return "dispatch" }

func (d *dispatchStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	if d.cfg.DispatchStore != nil && evt.RuntimeEventID != "" {
		reply, ok, err := d.cfg.DispatchStore.GetDispatchCompletion(ctx, evt.RuntimeEventID)
		if err != nil {
			return evt, DecisionContinue, fmt.Errorf("load dispatch completion: %w", err)
		}
		if ok {
			if err := d.sendReply(ctx, evt, reply); err != nil {
				return evt, DecisionContinue, fmt.Errorf("send completed dispatch reply: %w", err)
			}
			return evt, DecisionContinue, nil
		}
	}

	// PRD E6: recall events are annotated in the chat thread but never
	// mutate any Issue or Comment. They bypass intent recognition entirely.
	if evt.Type == port.EventTypeMessageRecalled {
		reply := fmt.Sprintf("[%s] 上游消息已撤回 (message_id: %s)", replyMessageRecalled, evt.MessageID)
		if err := d.persistDispatchCompletion(ctx, evt, reply); err != nil {
			return evt, DecisionContinue, err
		}
		if sendErr := d.sendReply(ctx, evt, reply); sendErr != nil {
			return evt, DecisionContinue, fmt.Errorf("send recall annotation: %w", sendErr)
		}
		return evt, DecisionContinue, nil
	}

	intent := evt.Intent

	slog.Debug("dispatch: routing intent",
		"intent", string(intent.Kind),
		"confidence", intent.Confidence,
		"source", string(intent.Source),
		"channel", evt.ChannelName,
		"chat_id", evt.ChatID,
	)

	var reply string
	var err error

	switch intent.Kind {
	case port.IntentCreateIssue:
		reply, err = d.handleCreateIssue(ctx, evt)
	case port.IntentAddComment:
		reply, err = d.handleAddComment(ctx, evt)
	case port.IntentQueryIssue:
		reply, err = d.handleQueryIssue(ctx, evt)
	case port.IntentSetStatus:
		reply, err = d.handleSetStatus(ctx, evt)
	case port.IntentSetAssignee:
		reply, err = d.handleSetAssignee(ctx, evt)
	case port.IntentSetPriority:
		reply, err = d.handleSetPriority(ctx, evt)
	case port.IntentSetLabel:
		reply, err = d.handleSetLabel(ctx, evt)
	case port.IntentUnsupported:
		reply = fmt.Sprintf("[%s] 此操作不支持在群内执行，请回 Web 端操作。", replyUnsupportedOp)
	case port.IntentUnknown:
		reply = fmt.Sprintf("[%s] 没有理解你的意思。支持的操作：创建 Issue、加评论、查状态、改状态、改指派人、改优先级、加/去标签。", replyUnknown)
	case port.IntentASKClarify:
		reply = fmt.Sprintf("[%s] 没有理解你的意思，请说得更明确一些。", replyAskClarify)
	default:
		reply = fmt.Sprintf("[%s] 没有理解你的意思。支持的操作：创建 Issue、加评论、查状态、改状态、改指派人、改优先级、加/去标签。", replyUnknown)
	}

	if err != nil {
		slog.Error("dispatch: handler error", "intent", string(intent.Kind), "error", err)
		return evt, DecisionContinue, err
	}

	if _, hasSuffix := intent.Params["_ignored_suffix"]; hasSuffix {
		reply += fmt.Sprintf("\n[%s] 消息中包含多个意图，已忽略附加部分。", replyIgnoredSuffix)
	}

	// This checkpoint replays already persisted replies. Query replies are
	// intentionally at-least-current if a worker crashes before this write.
	if err := d.persistDispatchCompletion(ctx, evt, reply); err != nil {
		return evt, DecisionContinue, err
	}

	if sendErr := d.sendReply(ctx, evt, reply); sendErr != nil {
		return evt, DecisionContinue, fmt.Errorf("send dispatch reply: %w", sendErr)
	}

	return evt, DecisionContinue, nil
}

func (d *dispatchStep) persistDispatchCompletion(ctx context.Context, evt port.InboundEvent, reply string) error {
	if d.cfg.DispatchStore == nil || evt.RuntimeEventID == "" {
		return nil
	}
	if err := d.cfg.DispatchStore.MarkDispatchCompleted(ctx, evt.RuntimeEventID, reply); err != nil {
		return fmt.Errorf("mark dispatch completed: %w", err)
	}
	return nil
}

func (d *dispatchStep) handleCreateIssue(ctx context.Context, evt port.InboundEvent) (string, error) {
	title, _ := evt.Intent.Params["title"]
	if title == "" {
		return fmt.Sprintf("[%s] 缺少 Issue 标题，请提供要创建的内容。", replyMissingParam), nil
	}

	wsID, err := d.cfg.ChatBinding.LookupWorkspaceID(ctx, evt.ChannelName, evt.ChatID)
	if err != nil {
		return "", fmt.Errorf("lookup workspace: %w", err)
	}

	user, err := d.cfg.UserResolver.Resolve(ctx, evt.ChannelName, evt.SenderID)
	if err != nil {
		return "", fmt.Errorf("resolve user: %w", err)
	}

	var projectID pgtype.UUID
	if rawProjectID := evt.Intent.Params["project_id"]; rawProjectID != "" {
		parsed, err := util.ParseUUID(rawProjectID)
		if err != nil {
			return fmt.Sprintf("[%s] project_id 格式不正确。", replyMissingParam), nil
		}
		projectID = parsed
		if d.cfg.ProjectValidator == nil {
			return "", errors.New("validate project: project validator is not configured")
		}
		if err := d.cfg.ProjectValidator.ValidateProjectInWorkspace(ctx, wsID, projectID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Sprintf("[%s] project_id 不属于当前 workspace。", replyMissingParam), nil
			}
			return "", fmt.Errorf("validate project: %w", err)
		}
	}

	issue, err := d.cfg.IssueFacade.CreateIssue(ctx, facade.CreateIssueReq{
		WorkspaceID:    wsID,
		ActorID:        user.MulticaUserID,
		ProjectID:      projectID,
		InboundEventID: parseRuntimeEventID(evt.RuntimeEventID),
		Title:          title,
		Description:    evt.Intent.Params["description"],
	})
	if err != nil {
		return "", fmt.Errorf("create issue: %w", err)
	}

	return fmt.Sprintf("[%s] 已创建 Issue %s：%s", replyIssueCreated, issue.Identifier, issue.Title), nil
}

func (d *dispatchStep) handleAddComment(ctx context.Context, evt port.InboundEvent) (string, error) {
	issueKey, _ := evt.Intent.Params["issue_key"]
	comment, _ := evt.Intent.Params["comment"]
	if issueKey == "" || comment == "" {
		return fmt.Sprintf("[%s] 缺少参数：需要 Issue 编号和评论内容。", replyMissingParam), nil
	}

	if !ValidIdentifierFormat(issueKey) {
		return fmt.Sprintf("[%s] Issue 编号格式不正确。", replyIssueNotFound), nil
	}

	wsID, err := d.cfg.ChatBinding.LookupWorkspaceID(ctx, evt.ChannelName, evt.ChatID)
	if err != nil {
		return "", fmt.Errorf("lookup workspace: %w", err)
	}

	issue, err := d.cfg.IssueFacade.GetIssueByIdentifier(ctx, wsID, issueKey)
	if err != nil {
		return fmt.Sprintf("[%s] 找不到 Issue %s。", replyIssueNotFound, issueKey), nil
	}

	user, err := d.cfg.UserResolver.Resolve(ctx, evt.ChannelName, evt.SenderID)
	if err != nil {
		return "", fmt.Errorf("resolve user: %w", err)
	}

	if _, err := d.cfg.CommentFacade.AddComment(ctx, facade.AddCommentReq{
		IssueID:        issue.ID,
		ActorID:        user.MulticaUserID,
		InboundEventID: parseRuntimeEventID(evt.RuntimeEventID),
		Content:        comment,
	}); err != nil {
		return "", fmt.Errorf("add comment: %w", err)
	}

	return fmt.Sprintf("[%s] 已在 %s 上添加评论。", replyCommentAdded, issueKey), nil
}

func (d *dispatchStep) handleQueryIssue(ctx context.Context, evt port.InboundEvent) (string, error) {
	issueKey, hasKey := evt.Intent.Params["issue_key"]

	wsID, err := d.cfg.ChatBinding.LookupWorkspaceID(ctx, evt.ChannelName, evt.ChatID)
	if err != nil {
		return "", fmt.Errorf("lookup workspace: %w", err)
	}

	if !hasKey || issueKey == "" {
		user, err := d.cfg.UserResolver.Resolve(ctx, evt.ChannelName, evt.SenderID)
		if err != nil {
			return "", fmt.Errorf("resolve user: %w", err)
		}

		issues, err := d.cfg.IssueFacade.ListMyTodos(ctx, wsID, user.MulticaUserID)
		if err != nil {
			return "", fmt.Errorf("list todos: %w", err)
		}
		if len(issues) == 0 {
			return "你没有待办的 Issue。", nil
		}
		msg := "你的待办：\n"
		for i, iss := range issues {
			if i >= 10 {
				msg += fmt.Sprintf("... 还有 %d 条\n", len(issues)-10)
				break
			}
			msg += fmt.Sprintf("  %s [%s] %s\n", iss.Identifier, iss.Status, iss.Title)
		}
		return msg, nil
	}

	if !ValidIdentifierFormat(issueKey) {
		return fmt.Sprintf("[%s] Issue 编号格式不正确。", replyIssueNotFound), nil
	}

	issue, err := d.cfg.IssueFacade.GetIssueByIdentifier(ctx, wsID, issueKey)
	if err != nil {
		return fmt.Sprintf("[%s] 找不到 Issue %s。", replyIssueNotFound, issueKey), nil
	}

	msg := fmt.Sprintf("📋 %s [%s] %s",
		issue.Identifier, issue.Status, issue.Title)

	if user, err := d.cfg.UserResolver.Resolve(ctx, evt.ChannelName, evt.SenderID); err == nil && user.DisplayName != "" {
		msg += fmt.Sprintf("\n查询者: %s", user.DisplayName)
	}

	return msg, nil
}

func (d *dispatchStep) handleSetStatus(ctx context.Context, evt port.InboundEvent) (string, error) {
	issueKey, status := evt.Intent.Params["issue_key"], evt.Intent.Params["status"]
	if issueKey == "" || status == "" {
		return fmt.Sprintf("[%s] 缺少参数：需要 Issue 编号和目标状态。", replyMissingParam), nil
	}
	issue, user, err := d.resolveIssueAndUser(ctx, evt, issueKey)
	if err != nil {
		return "", err
	}
	if issue.ID == (pgtype.UUID{}) {
		return fmt.Sprintf("[%s] 找不到 Issue %s。", replyIssueNotFound, issueKey), nil
	}
	if err := d.cfg.IssueFacade.SetIssueStatus(ctx, issue.ID, user.MulticaUserID, status, facade.ChannelMutationContext{InboundEventID: parseRuntimeEventID(evt.RuntimeEventID)}); err != nil {
		return "", fmt.Errorf("set status: %w", err)
	}
	return fmt.Sprintf("[%s] 已将 %s 状态改为 %s。", replyStatusChanged, issueKey, status), nil
}

func (d *dispatchStep) handleSetAssignee(ctx context.Context, evt port.InboundEvent) (string, error) {
	issueKey, assignee := evt.Intent.Params["issue_key"], evt.Intent.Params["assignee"]
	if issueKey == "" || assignee == "" {
		return fmt.Sprintf("[%s] 缺少参数：需要 Issue 编号和指派人。", replyMissingParam), nil
	}
	issue, user, err := d.resolveIssueAndUser(ctx, evt, issueKey)
	if err != nil {
		return "", err
	}
	if issue.ID == (pgtype.UUID{}) {
		return fmt.Sprintf("[%s] 找不到 Issue %s。", replyIssueNotFound, issueKey), nil
	}
	if err := d.cfg.IssueFacade.SetIssueAssignee(ctx, issue.ID, user.MulticaUserID, assignee, facade.ChannelMutationContext{InboundEventID: parseRuntimeEventID(evt.RuntimeEventID)}); err != nil {
		return "", fmt.Errorf("set assignee: %w", err)
	}
	return fmt.Sprintf("[%s] 已将 %s 的指派人改为 %s。", replyAssigneeChanged, issueKey, assignee), nil
}

func (d *dispatchStep) handleSetPriority(ctx context.Context, evt port.InboundEvent) (string, error) {
	issueKey, priority := evt.Intent.Params["issue_key"], evt.Intent.Params["priority"]
	if issueKey == "" || priority == "" {
		return fmt.Sprintf("[%s] 缺少参数：需要 Issue 编号和目标优先级。", replyMissingParam), nil
	}
	issue, user, err := d.resolveIssueAndUser(ctx, evt, issueKey)
	if err != nil {
		return "", err
	}
	if issue.ID == (pgtype.UUID{}) {
		return fmt.Sprintf("[%s] 找不到 Issue %s。", replyIssueNotFound, issueKey), nil
	}
	if err := d.cfg.IssueFacade.SetIssuePriority(ctx, issue.ID, user.MulticaUserID, priority, facade.ChannelMutationContext{InboundEventID: parseRuntimeEventID(evt.RuntimeEventID)}); err != nil {
		return "", fmt.Errorf("set priority: %w", err)
	}
	return fmt.Sprintf("[%s] 已将 %s 的优先级改为 %s。", replyPriorityChanged, issueKey, priority), nil
}

func (d *dispatchStep) handleSetLabel(ctx context.Context, evt port.InboundEvent) (string, error) {
	issueKey, label, op := evt.Intent.Params["issue_key"], evt.Intent.Params["label"], evt.Intent.Params["op"]
	if issueKey == "" || label == "" {
		return fmt.Sprintf("[%s] 缺少参数：需要 Issue 编号和标签名。", replyMissingParam), nil
	}
	issue, user, err := d.resolveIssueAndUser(ctx, evt, issueKey)
	if err != nil {
		return "", err
	}
	if issue.ID == (pgtype.UUID{}) {
		return fmt.Sprintf("[%s] 找不到 Issue %s。", replyIssueNotFound, issueKey), nil
	}
	if op == "remove" {
		if err := d.cfg.IssueFacade.RemoveIssueLabel(ctx, issue.ID, user.MulticaUserID, label, facade.ChannelMutationContext{InboundEventID: parseRuntimeEventID(evt.RuntimeEventID)}); err != nil {
			return "", fmt.Errorf("remove label: %w", err)
		}
		return fmt.Sprintf("[%s] 已从 %s 去掉标签 %s。", replyLabelRemoved, issueKey, label), nil
	}
	if err := d.cfg.IssueFacade.AddIssueLabel(ctx, issue.ID, user.MulticaUserID, label, facade.ChannelMutationContext{InboundEventID: parseRuntimeEventID(evt.RuntimeEventID)}); err != nil {
		return "", fmt.Errorf("add label: %w", err)
	}
	return fmt.Sprintf("[%s] 已为 %s 添加标签 %s。", replyLabelAdded, issueKey, label), nil
}

func (d *dispatchStep) resolveIssueAndUser(ctx context.Context, evt port.InboundEvent, issueKey string) (facade.Issue, ResolvedUser, error) {
	if !ValidIdentifierFormat(issueKey) {
		return facade.Issue{}, ResolvedUser{}, fmt.Errorf("[%s] Issue 编号格式不正确。", replyIssueNotFound)
	}
	wsID, err := d.cfg.ChatBinding.LookupWorkspaceID(ctx, evt.ChannelName, evt.ChatID)
	if err != nil {
		return facade.Issue{}, ResolvedUser{}, fmt.Errorf("lookup workspace: %w", err)
	}
	issue, err := d.cfg.IssueFacade.GetIssueByIdentifier(ctx, wsID, issueKey)
	if err != nil {
		return facade.Issue{}, ResolvedUser{}, nil // not found — caller formats reply
	}
	user, err := d.cfg.UserResolver.Resolve(ctx, evt.ChannelName, evt.SenderID)
	if err != nil {
		return facade.Issue{}, ResolvedUser{}, fmt.Errorf("resolve user: %w", err)
	}
	return issue, user, nil
}

func (d *dispatchStep) sendReply(ctx context.Context, evt port.InboundEvent, text string) error {
	if d.cfg.Registry == nil {
		return nil
	}
	ch, err := d.cfg.Registry.Get(evt.ChannelName)
	if err != nil {
		return fmt.Errorf("channel %q not in registry: %w", evt.ChannelName, err)
	}

	_, err = ch.Send(ctx, port.OutboundMessage{
		ChatID: evt.ChatID,
		Text:   text,
	})
	return err
}

func parseRuntimeEventID(id string) pgtype.UUID {
	if id == "" {
		return pgtype.UUID{}
	}
	parsed, err := util.ParseUUID(id)
	if err != nil {
		return pgtype.UUID{}
	}
	return parsed
}

// identifierRe matches valid issue identifiers like STA-39, MUL-123.
// Format: 2-5 uppercase letters, hyphen, positive integer (no leading zeros).
var identifierRe = regexp.MustCompile(`^[A-Z]{2,5}-[1-9][0-9]*$`)

// ValidIdentifierFormat checks if an issue identifier matches the expected
// format (e.g. STA-39, MUL-123). Exported for testing.
func ValidIdentifierFormat(key string) bool {
	return identifierRe.MatchString(key)
}
