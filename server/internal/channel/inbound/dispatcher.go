package inbound

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/channel"
	"github.com/multica-ai/multica/server/internal/channel/facade"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

const (
	replyIssueCreated       = "ISSUE_CREATED"
	replyCommentAdded       = "COMMENT_ADDED"
	replyStatusChanged      = "STATUS_CHANGED"
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

type ResolvedUser struct {
	MulticaUserID pgtype.UUID
	DisplayName   string
}

type DispatchConfig struct {
	IssueFacade   facade.IssueFacade
	CommentFacade facade.CommentFacade
	Registry      *channel.Registry
	ChatBinding   ChatBindingLookup
	UserResolver  UserInfoResolver
}

type dispatchStep struct {
	cfg DispatchConfig
}

func NewDispatchStep(cfg DispatchConfig) Step {
	return &dispatchStep{cfg: cfg}
}

func (dispatchStep) Name() string { return "dispatch" }

func (d *dispatchStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	// PRD E6: recall events are annotated in the chat thread but never
	// mutate any Issue or Comment. They bypass intent recognition entirely.
	if evt.Type == port.EventTypeMessageRecalled {
		reply := fmt.Sprintf("[%s] 上游消息已撤回 (message_id: %s)", replyMessageRecalled, evt.MessageID)
		if sendErr := d.sendReply(ctx, evt, reply); sendErr != nil {
			slog.Error("dispatch: send recall annotation failed",
				"channel", evt.ChannelName,
				"chat_id", evt.ChatID,
				"error", sendErr,
			)
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
	case port.IntentUnsupported:
		reply = fmt.Sprintf("[%s] 此操作不支持在群内执行，请回 Web 端操作。", replyUnsupportedOp)
	case port.IntentUnknown:
		reply = fmt.Sprintf("[%s] 没有理解你的意思。支持的操作：创建 Issue、加评论、查状态、改状态。", replyUnknown)
	case port.IntentASKClarify:
		reply = fmt.Sprintf("[%s] 没有理解你的意思，请说得更明确一些。", replyAskClarify)
	default:
		reply = fmt.Sprintf("[%s] 没有理解你的意思。支持的操作：创建 Issue、加评论、查状态、改状态。", replyUnknown)
	}

	if err != nil {
		slog.Error("dispatch: handler error", "intent", string(intent.Kind), "error", err)
		reply = fmt.Sprintf("[%s] 处理请求时出错，请稍后重试。", replyInternalError)
		err = nil
	}

	if _, hasSuffix := intent.Params["_ignored_suffix"]; hasSuffix {
		reply += fmt.Sprintf("\n[%s] 消息中包含多个意图，已忽略附加部分。", replyIgnoredSuffix)
	}

	if sendErr := d.sendReply(ctx, evt, reply); sendErr != nil {
		slog.Error("dispatch: send reply failed",
			"channel", evt.ChannelName,
			"chat_id", evt.ChatID,
			"error", sendErr,
		)
	}

	return evt, DecisionContinue, nil
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

	issue, err := d.cfg.IssueFacade.CreateIssue(ctx, facade.CreateIssueReq{
		WorkspaceID: wsID,
		ActorID:     user.MulticaUserID,
		Title:       title,
		Description: evt.Intent.Params["description"],
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
		IssueID: issue.ID,
		ActorID: user.MulticaUserID,
		Content: comment,
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
	issueKey, _ := evt.Intent.Params["issue_key"]
	status, _ := evt.Intent.Params["status"]
	if issueKey == "" || status == "" {
		return fmt.Sprintf("[%s] 缺少参数：需要 Issue 编号和目标状态。", replyMissingParam), nil
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

	if err := d.cfg.IssueFacade.SetIssueStatus(ctx, issue.ID, user.MulticaUserID, status); err != nil {
		return "", fmt.Errorf("set status: %w", err)
	}

	return fmt.Sprintf("[%s] 已将 %s 状态改为 %s。", replyStatusChanged, issueKey, status), nil
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

// identifierRe matches valid issue identifiers like STA-39, MUL-123.
// Format: 2-5 uppercase letters, hyphen, positive integer (no leading zeros).
var identifierRe = regexp.MustCompile(`^[A-Z]{2,5}-[1-9][0-9]*$`)

// ValidIdentifierFormat checks if an issue identifier matches the expected
// format (e.g. STA-39, MUL-123). Exported for testing.
func ValidIdentifierFormat(key string) bool {
	return identifierRe.MatchString(key)
}
