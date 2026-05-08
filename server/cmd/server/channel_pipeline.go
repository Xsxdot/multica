package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/channel"
	"github.com/multica-ai/multica/server/internal/channel/binding"
	"github.com/multica-ai/multica/server/internal/channel/facade"
	"github.com/multica-ai/multica/server/internal/channel/inbound"
	chintent "github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/channel/port"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func newChannelInboundPipeline(pool *pgxpool.Pool, registry *channel.Registry) *inbound.Pipeline {
	queries := db.New(pool)
	issueSvc := &channelIssueService{pool: pool}
	commentSvc := &channelCommentService{queries: queries, issueSvc: issueSvc}
	bindings := &channelChatBindingLookup{pool: pool}

	return inbound.NewPipeline(
		inbound.NewNormalizeStep(),
		inbound.NewDedupStep(inbound.NewDBDedupStore(queries)),
		newChannelIdentityBindStep(pool, registry, binding.NewTokenIssuer(queries)),
		newChannelChatBindCommandStep(registry, binding.NewTokenIssuer(queries)),
		inbound.NewSlashStep(inbound.SlashConfig{Registry: registry}),
		channelRuleIntentStep{},
		inbound.NewAuthzStep(inbound.AuthzConfig{
			Store:        bindings,
			Registry:     registry,
			SendReplies:  true,
			RejectAsSkip: true,
		}),
		inbound.NewDispatchStep(inbound.DispatchConfig{
			IssueFacade:   facade.NewIssueFacade(issueSvc),
			CommentFacade: facade.NewCommentFacade(commentSvc),
			Registry:      registry,
			ChatBinding:   bindings,
			UserResolver:  &channelUserInfoResolver{pool: pool},
		}),
		inbound.NewReplyStep(),
	)
}

type channelIdentityBindStep struct {
	pool     *pgxpool.Pool
	registry *channel.Registry
	issuer   *binding.TokenIssuer
}

func newChannelIdentityBindStep(pool *pgxpool.Pool, registry *channel.Registry, issuer *binding.TokenIssuer) inbound.Step {
	return &channelIdentityBindStep{pool: pool, registry: registry, issuer: issuer}
}

func (*channelIdentityBindStep) Name() string { return "identity-bind" }

func (s *channelIdentityBindStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, inbound.Decision, error) {
	var userID pgtype.UUID
	err := s.pool.QueryRow(ctx, `
		SELECT user_id FROM channel_user_binding
		WHERE provider = $1 AND external_user_id = $2
	`, evt.ChannelName, evt.SenderID).Scan(&userID)
	if err == nil {
		return evt, inbound.DecisionContinue, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return evt, inbound.DecisionContinue, fmt.Errorf("identity-bind: lookup binding: %w", err)
	}

	token, err := s.issuer.IssueUserIdentity(ctx, evt.ChannelName, evt.SenderID)
	if err != nil {
		return evt, inbound.DecisionContinue, fmt.Errorf("identity-bind: issue token: %w", err)
	}
	ch, err := s.registry.Get(evt.ChannelName)
	if err != nil {
		return evt, inbound.DecisionContinue, fmt.Errorf("identity-bind: get channel: %w", err)
	}
	body := fmt.Sprintf("点击绑定 Multica 账号（10 分钟内有效）: %s", channelBindURL("user", token.Plaintext))
	if _, err := ch.Send(ctx, port.OutboundMessage{
		Target: port.TargetUser(evt.SenderID),
		Text:   body,
	}); err != nil {
		if evt.ChatType == port.ChatTypeGroup {
			notice := "请先和机器人私聊或开启机器人私聊权限后，在群里重新发送 /bind。"
			if _, sendErr := ch.Send(ctx, port.OutboundMessage{
				Target: port.TargetChat(evt.ChatID),
				Text:   notice,
			}); sendErr != nil {
				return evt, inbound.DecisionContinue, fmt.Errorf("identity-bind: send private link failed: %w; group notice failed: %v", err, sendErr)
			}
			return evt, inbound.DecisionSkip, nil
		}
		return evt, inbound.DecisionContinue, fmt.Errorf("identity-bind: send private link: %w", err)
	}
	return evt, inbound.DecisionSkip, nil
}

type channelChatBindCommandStep struct {
	registry *channel.Registry
	issuer   *binding.TokenIssuer
}

func newChannelChatBindCommandStep(registry *channel.Registry, issuer *binding.TokenIssuer) inbound.Step {
	return &channelChatBindCommandStep{registry: registry, issuer: issuer}
}

func (*channelChatBindCommandStep) Name() string { return "chat-bind-command" }

func (s *channelChatBindCommandStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, inbound.Decision, error) {
	if strings.TrimSpace(evt.Text) != "/bind" {
		return evt, inbound.DecisionContinue, nil
	}

	ch, err := s.registry.Get(evt.ChannelName)
	if err != nil {
		return evt, inbound.DecisionContinue, fmt.Errorf("chat-bind-command: get channel: %w", err)
	}

	if evt.ChatType == port.ChatTypeDirect {
		if _, err := ch.Send(ctx, port.OutboundMessage{
			Target: port.TargetUser(evt.SenderID),
			Text:   "请在飞书群里发送 /bind 绑定群。",
		}); err != nil {
			return evt, inbound.DecisionContinue, fmt.Errorf("chat-bind-command: send direct notice: %w", err)
		}
		return evt, inbound.DecisionSkip, nil
	}

	chatInfo := port.ChatInfo{ID: evt.ChatID, Type: evt.ChatType}
	if info, err := ch.GetChatInfo(ctx, evt.ChatID); err == nil {
		chatInfo = info
	}
	if chatInfo.ID == "" {
		chatInfo.ID = evt.ChatID
	}
	if chatInfo.Type == "" {
		chatInfo.Type = evt.ChatType
	}

	token, err := s.issuer.IssueChatWorkspace(ctx, binding.IssueChatWorkspaceReq{
		Provider:                evt.ChannelName,
		InitiatorExternalUserID: evt.SenderID,
		ExternalChatID:          chatInfo.ID,
		ExternalChatType:        string(chatInfo.Type),
		ExternalChatName:        chatInfo.Name,
	})
	if err != nil {
		return evt, inbound.DecisionContinue, fmt.Errorf("chat-bind-command: issue token: %w", err)
	}

	body := fmt.Sprintf("点击绑定当前群到 Multica 工作区（10 分钟内有效）: %s", channelBindURL("chat", token.Plaintext))
	if _, err := ch.Send(ctx, port.OutboundMessage{
		Target: port.TargetUser(evt.SenderID),
		Text:   body,
	}); err == nil {
		return evt, inbound.DecisionSkip, nil
	}

	if _, err := ch.Send(ctx, port.OutboundMessage{
		Target: port.TargetChat(evt.ChatID),
		Text:   body,
	}); err != nil {
		return evt, inbound.DecisionContinue, fmt.Errorf("chat-bind-command: send group fallback link: %w", err)
	}
	return evt, inbound.DecisionSkip, nil
}

func channelBindURL(kind, token string) string {
	baseURL := strings.TrimRight(os.Getenv("MULTICA_APP_URL"), "/")
	if baseURL == "" {
		baseURL = "http://localhost:3000"
	}
	return fmt.Sprintf("%s/bind?kind=%s&token=%s", baseURL, url.QueryEscape(kind), url.QueryEscape(token))
}

type channelRuleIntentStep struct{}

func (channelRuleIntentStep) Name() string { return "intent-recog" }

func (channelRuleIntentStep) Run(_ context.Context, evt port.InboundEvent) (port.InboundEvent, inbound.Decision, error) {
	m := chintent.NewRuleMatcher()
	if intent, ok := m.Match(evt.Text); ok {
		evt.Intent = port.InboundIntent{
			Kind:       port.IntentKind(intent.Kind),
			Confidence: intent.Confidence,
			Params:     intent.Params,
			Source:     port.SourceRule,
		}
		return evt, inbound.DecisionContinue, nil
	}
	evt.Intent = port.InboundIntent{Kind: port.IntentUnknown, Source: port.SourceRule, Params: map[string]string{}}
	return evt, inbound.DecisionContinue, nil
}

type channelChatBindingLookup struct {
	pool *pgxpool.Pool
}

func (l *channelChatBindingLookup) LookupWorkspaceID(ctx context.Context, channelName, chatID string) (pgtype.UUID, error) {
	var wsID pgtype.UUID
	err := l.pool.QueryRow(ctx, `
		SELECT workspace_id FROM channel_chat_binding
		WHERE provider = $1 AND external_chat_id = $2
	`, channelName, chatID).Scan(&wsID)
	return wsID, err
}

func (l *channelChatBindingLookup) LookupPrimaryWorkspaceID(ctx context.Context, channelName, chatID string) (pgtype.UUID, error) {
	var wsID pgtype.UUID
	err := l.pool.QueryRow(ctx, `
		SELECT workspace_id FROM channel_chat_binding
		WHERE provider = $1 AND external_chat_id = $2 AND is_primary = TRUE
	`, channelName, chatID).Scan(&wsID)
	return wsID, err
}

func (l *channelChatBindingLookup) ResolveUserID(ctx context.Context, channelName, externalUserID string) (pgtype.UUID, error) {
	var userID pgtype.UUID
	err := l.pool.QueryRow(ctx, `
		SELECT user_id FROM channel_user_binding
		WHERE provider = $1 AND external_user_id = $2
	`, channelName, externalUserID).Scan(&userID)
	return userID, err
}

func (l *channelChatBindingLookup) IsWorkspaceMember(ctx context.Context, userID, workspaceID pgtype.UUID) (bool, error) {
	var exists bool
	err := l.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM member
			WHERE user_id = $1 AND workspace_id = $2
		)
	`, userID, workspaceID).Scan(&exists)
	return exists, err
}

func (l *channelChatBindingLookup) CheckIssuePermission(ctx context.Context, workspaceID, _ pgtype.UUID, issueKey string) error {
	var exists bool
	err := l.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM issue i
			JOIN workspace w ON w.id = i.workspace_id
			WHERE i.workspace_id = $1
			  AND (w.issue_prefix || '-' || i.number::text) = $2
		)
	`, workspaceID, issueKey).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return pgx.ErrNoRows
	}
	return nil
}

type channelUserInfoResolver struct {
	pool *pgxpool.Pool
}

func (r *channelUserInfoResolver) Resolve(ctx context.Context, channelName, externalUserID string) (inbound.ResolvedUser, error) {
	var (
		userID pgtype.UUID
		name   string
	)
	err := r.pool.QueryRow(ctx, `
		SELECT u.id, u.name
		FROM channel_user_binding b
		JOIN "user" u ON u.id = b.user_id
		WHERE b.provider = $1 AND b.external_user_id = $2
	`, channelName, externalUserID).Scan(&userID, &name)
	if err != nil {
		return inbound.ResolvedUser{}, err
	}
	return inbound.ResolvedUser{MulticaUserID: userID, DisplayName: name}, nil
}

type channelIssueService struct {
	pool *pgxpool.Pool
}

func (s *channelIssueService) CreateIssue(ctx context.Context, req facade.CreateIssueReq) (facade.Issue, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return facade.Issue{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var number int32
	if err := tx.QueryRow(ctx, `
		UPDATE workspace SET issue_counter = issue_counter + 1
		WHERE id = $1
		RETURNING issue_counter
	`, req.WorkspaceID).Scan(&number); err != nil {
		return facade.Issue{}, fmt.Errorf("bump issue counter: %w", err)
	}

	queries := db.New(tx)
	issue, err := queries.CreateIssue(ctx, db.CreateIssueParams{
		WorkspaceID: req.WorkspaceID,
		Title:       req.Title,
		Description: pgtype.Text{String: req.Description, Valid: req.Description != ""},
		Status:      "todo",
		Priority:    "none",
		CreatorType: "member",
		CreatorID:   req.ActorID,
		Number:      number,
	})
	if err != nil {
		return facade.Issue{}, fmt.Errorf("insert issue: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return facade.Issue{}, fmt.Errorf("commit: %w", err)
	}
	return s.toFacadeIssue(ctx, issue)
}

func (s *channelIssueService) GetIssue(ctx context.Context, id pgtype.UUID) (facade.Issue, error) {
	issue, err := db.New(s.pool).GetIssue(ctx, id)
	if err != nil {
		return facade.Issue{}, err
	}
	return s.toFacadeIssue(ctx, issue)
}

func (s *channelIssueService) GetIssueByIdentifier(ctx context.Context, workspaceID pgtype.UUID, identifier string) (facade.Issue, error) {
	var issue db.Issue
	err := s.pool.QueryRow(ctx, `
		SELECT i.id, i.workspace_id, i.title, i.description, i.status, i.priority,
		       i.assignee_type, i.assignee_id, i.creator_type, i.creator_id,
		       i.parent_issue_id, i.acceptance_criteria, i.context_refs, i.position,
		       i.due_date, i.created_at, i.updated_at, i.number, i.project_id,
		       i.origin_type, i.origin_id, i.first_executed_at
		FROM issue i
		JOIN workspace w ON w.id = i.workspace_id
		WHERE i.workspace_id = $1
		  AND (w.issue_prefix || '-' || i.number::text) = $2
	`, workspaceID, identifier).Scan(
		&issue.ID,
		&issue.WorkspaceID,
		&issue.Title,
		&issue.Description,
		&issue.Status,
		&issue.Priority,
		&issue.AssigneeType,
		&issue.AssigneeID,
		&issue.CreatorType,
		&issue.CreatorID,
		&issue.ParentIssueID,
		&issue.AcceptanceCriteria,
		&issue.ContextRefs,
		&issue.Position,
		&issue.DueDate,
		&issue.CreatedAt,
		&issue.UpdatedAt,
		&issue.Number,
		&issue.ProjectID,
		&issue.OriginType,
		&issue.OriginID,
		&issue.FirstExecutedAt,
	)
	if err != nil {
		return facade.Issue{}, err
	}
	return s.toFacadeIssue(ctx, issue)
}

func (s *channelIssueService) SetIssueStatus(ctx context.Context, id pgtype.UUID, _ pgtype.UUID, status string) error {
	_, err := db.New(s.pool).UpdateIssueStatus(ctx, db.UpdateIssueStatusParams{ID: id, Status: status})
	return err
}

func (s *channelIssueService) SetIssueAssignee(ctx context.Context, id pgtype.UUID, _ pgtype.UUID, assigneeIdentifier string) error {
	var assigneeID pgtype.UUID
	clean := strings.TrimPrefix(assigneeIdentifier, "@")
	if err := s.pool.QueryRow(ctx, `
		SELECT m.user_id
		FROM member m
		JOIN issue i ON i.workspace_id = m.workspace_id
		LEFT JOIN "user" u ON u.id = m.user_id
		WHERE i.id = $1
		  AND (u.name = $2 OR m.user_id::text = $2)
		LIMIT 1
	`, id, clean).Scan(&assigneeID); err != nil {
		return fmt.Errorf("user %s is not in this workspace: %w", assigneeIdentifier, err)
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE issue SET assignee_type = 'member', assignee_id = $1, updated_at = now()
		WHERE id = $2
	`, assigneeID, id)
	return err
}

func (s *channelIssueService) SetIssuePriority(ctx context.Context, id pgtype.UUID, _ pgtype.UUID, priority string) error {
	valid := map[string]bool{"urgent": true, "high": true, "medium": true, "low": true, "no_priority": true, "none": true}
	if !valid[priority] {
		return fmt.Errorf("unsupported priority %q", priority)
	}
	if priority == "none" {
		priority = "no_priority"
	}
	_, err := s.pool.Exec(ctx, `UPDATE issue SET priority = $1, updated_at = now() WHERE id = $2`, priority, id)
	return err
}

func (s *channelIssueService) AddIssueLabel(ctx context.Context, id pgtype.UUID, _ pgtype.UUID, labelName string) error {
	wsID, labelID, err := s.resolveIssueLabel(ctx, id, labelName)
	if err != nil {
		return err
	}
	return db.New(s.pool).AttachLabelToIssue(ctx, db.AttachLabelToIssueParams{IssueID: id, LabelID: labelID, WorkspaceID: wsID})
}

func (s *channelIssueService) RemoveIssueLabel(ctx context.Context, id pgtype.UUID, _ pgtype.UUID, labelName string) error {
	wsID, labelID, err := s.resolveIssueLabel(ctx, id, labelName)
	if err != nil {
		return err
	}
	return db.New(s.pool).DetachLabelFromIssue(ctx, db.DetachLabelFromIssueParams{IssueID: id, LabelID: labelID, WorkspaceID: wsID})
}

func (s *channelIssueService) ListMyTodos(ctx context.Context, workspaceID, userID pgtype.UUID) ([]facade.Issue, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, workspace_id, title, status, number
		FROM issue
		WHERE workspace_id = $1
		  AND assignee_type = 'member'
		  AND assignee_id = $2
		  AND status NOT IN ('done', 'canceled')
		ORDER BY updated_at DESC
		LIMIT 10
	`, workspaceID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []facade.Issue{}
	for rows.Next() {
		var issue db.Issue
		if err := rows.Scan(&issue.ID, &issue.WorkspaceID, &issue.Title, &issue.Status, &issue.Number); err != nil {
			return nil, err
		}
		f, err := s.toFacadeIssue(ctx, issue)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *channelIssueService) resolveIssueLabel(ctx context.Context, issueID pgtype.UUID, labelName string) (pgtype.UUID, pgtype.UUID, error) {
	var wsID, labelID pgtype.UUID
	if err := s.pool.QueryRow(ctx, `SELECT workspace_id FROM issue WHERE id = $1`, issueID).Scan(&wsID); err != nil {
		return pgtype.UUID{}, pgtype.UUID{}, err
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT id FROM issue_label WHERE workspace_id = $1 AND name = $2
	`, wsID, labelName).Scan(&labelID); err != nil {
		return pgtype.UUID{}, pgtype.UUID{}, fmt.Errorf("label %q not found: %w", labelName, err)
	}
	return wsID, labelID, nil
}

func (s *channelIssueService) toFacadeIssue(ctx context.Context, issue db.Issue) (facade.Issue, error) {
	var prefix string
	if err := s.pool.QueryRow(ctx, `SELECT issue_prefix FROM workspace WHERE id = $1`, issue.WorkspaceID).Scan(&prefix); err != nil {
		return facade.Issue{}, err
	}
	return facade.Issue{
		ID:          issue.ID,
		WorkspaceID: issue.WorkspaceID,
		Identifier:  fmt.Sprintf("%s-%d", prefix, issue.Number),
		Title:       issue.Title,
		Status:      issue.Status,
	}, nil
}

type channelCommentService struct {
	queries  *db.Queries
	issueSvc *channelIssueService
}

func (s *channelCommentService) AddComment(ctx context.Context, req facade.AddCommentReq) (facade.Comment, error) {
	issue, err := s.issueSvc.GetIssue(ctx, req.IssueID)
	if err != nil {
		return facade.Comment{}, err
	}
	comment, err := s.queries.CreateComment(ctx, db.CreateCommentParams{
		IssueID:     req.IssueID,
		WorkspaceID: issue.WorkspaceID,
		AuthorType:  "member",
		AuthorID:    req.ActorID,
		Content:     req.Content,
		Type:        "comment",
	})
	if err != nil {
		return facade.Comment{}, err
	}
	return facade.Comment{
		ID:      comment.ID,
		IssueID: comment.IssueID,
		Content: comment.Content,
	}, nil
}

var (
	_ inbound.ChatBindingLookup = (*channelChatBindingLookup)(nil)
	_ inbound.UserInfoResolver  = (*channelUserInfoResolver)(nil)
	_ facade.IssueService       = (*channelIssueService)(nil)
	_ facade.CommentService     = (*channelCommentService)(nil)
	_ inbound.Step              = (*channelIdentityBindStep)(nil)
	_ inbound.Step              = (*channelChatBindCommandStep)(nil)
	_ inbound.AuthzStore        = (*channelChatBindingLookup)(nil)
	_ inbound.Step              = channelRuleIntentStep{}
)
