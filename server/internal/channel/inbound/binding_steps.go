package inbound

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
	"github.com/multica-ai/multica/server/internal/channel/port"
)

type userIdentityBindStep struct {
	pool     *pgxpool.Pool
	registry *channel.Registry
	issuer   *binding.TokenIssuer
}

func NewUserIdentityBindStep(pool *pgxpool.Pool, registry *channel.Registry, issuer *binding.TokenIssuer) Step {
	return &userIdentityBindStep{pool: pool, registry: registry, issuer: issuer}
}

func (*userIdentityBindStep) Name() string { return "identity-bind" }

func (s *userIdentityBindStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	var userID pgtype.UUID
	err := s.pool.QueryRow(ctx, `
		SELECT user_id FROM channel_user_binding
		WHERE provider = $1 AND external_user_id = $2
	`, evt.ChannelName, evt.SenderID).Scan(&userID)
	if err == nil {
		return evt, DecisionContinue, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return evt, DecisionContinue, fmt.Errorf("identity-bind: lookup binding: %w", err)
	}

	token, err := s.issuer.IssueUserIdentity(ctx, evt.ChannelName, evt.SenderID)
	if err != nil {
		return evt, DecisionContinue, fmt.Errorf("identity-bind: issue token: %w", err)
	}
	ch, err := s.registry.Get(evt.ChannelName)
	if err != nil {
		return evt, DecisionContinue, fmt.Errorf("identity-bind: get channel: %w", err)
	}
	body := fmt.Sprintf("点击绑定 Multica 账号（10 分钟内有效）: %s", ChannelBindURL("user", token.Plaintext))
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
				return evt, DecisionContinue, fmt.Errorf("identity-bind: send private link failed: %w; group notice failed: %v", err, sendErr)
			}
			return evt, DecisionSkip, nil
		}
		return evt, DecisionContinue, fmt.Errorf("identity-bind: send private link: %w", err)
	}
	return evt, DecisionSkip, nil
}

type chatBindCommandStep struct {
	registry *channel.Registry
	issuer   *binding.TokenIssuer
}

func NewChatBindCommandStep(registry *channel.Registry, issuer *binding.TokenIssuer) Step {
	return &chatBindCommandStep{registry: registry, issuer: issuer}
}

func (*chatBindCommandStep) Name() string { return "chat-bind-command" }

func (s *chatBindCommandStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	if strings.TrimSpace(evt.Text) != "/bind" {
		return evt, DecisionContinue, nil
	}

	ch, err := s.registry.Get(evt.ChannelName)
	if err != nil {
		return evt, DecisionContinue, fmt.Errorf("chat-bind-command: get channel: %w", err)
	}

	if evt.ChatType == port.ChatTypeDirect {
		if _, err := ch.Send(ctx, port.OutboundMessage{
			Target: port.TargetUser(evt.SenderID),
			Text:   "请在飞书群里发送 /bind 绑定群。",
		}); err != nil {
			return evt, DecisionContinue, fmt.Errorf("chat-bind-command: send direct notice: %w", err)
		}
		return evt, DecisionSkip, nil
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
		return evt, DecisionContinue, fmt.Errorf("chat-bind-command: issue token: %w", err)
	}

	body := fmt.Sprintf("点击绑定当前群到 Multica 工作区（10 分钟内有效）: %s", ChannelBindURL("chat", token.Plaintext))
	if _, err := ch.Send(ctx, port.OutboundMessage{
		Target: port.TargetUser(evt.SenderID),
		Text:   body,
	}); err == nil {
		return evt, DecisionSkip, nil
	}

	if _, err := ch.Send(ctx, port.OutboundMessage{
		Target: port.TargetChat(evt.ChatID),
		Text:   body,
	}); err != nil {
		return evt, DecisionContinue, fmt.Errorf("chat-bind-command: send group fallback link: %w", err)
	}
	return evt, DecisionSkip, nil
}

func ChannelBindURL(kind, token string) string {
	baseURL := strings.TrimRight(os.Getenv("MULTICA_APP_URL"), "/")
	if baseURL == "" {
		baseURL = "http://localhost:3000"
	}
	return fmt.Sprintf("%s/bind?kind=%s&token=%s", baseURL, url.QueryEscape(kind), url.QueryEscape(token))
}

type DBChatBindingLookup struct {
	pool *pgxpool.Pool
}

func NewDBChatBindingLookup(pool *pgxpool.Pool) *DBChatBindingLookup {
	return &DBChatBindingLookup{pool: pool}
}

func (l *DBChatBindingLookup) LookupWorkspaceID(ctx context.Context, channelName, chatID string) (pgtype.UUID, error) {
	var wsID pgtype.UUID
	err := l.pool.QueryRow(ctx, `
		SELECT workspace_id FROM channel_chat_binding
		WHERE provider = $1 AND external_chat_id = $2
	`, channelName, chatID).Scan(&wsID)
	return wsID, err
}

func (l *DBChatBindingLookup) LookupPrimaryWorkspaceID(ctx context.Context, channelName, chatID string) (pgtype.UUID, error) {
	var wsID pgtype.UUID
	err := l.pool.QueryRow(ctx, `
		SELECT workspace_id FROM channel_chat_binding
		WHERE provider = $1 AND external_chat_id = $2 AND is_primary = TRUE
	`, channelName, chatID).Scan(&wsID)
	return wsID, err
}

func (l *DBChatBindingLookup) ResolveUserID(ctx context.Context, channelName, externalUserID string) (pgtype.UUID, error) {
	var userID pgtype.UUID
	err := l.pool.QueryRow(ctx, `
		SELECT user_id FROM channel_user_binding
		WHERE provider = $1 AND external_user_id = $2
	`, channelName, externalUserID).Scan(&userID)
	return userID, err
}

func (l *DBChatBindingLookup) IsWorkspaceMember(ctx context.Context, userID, workspaceID pgtype.UUID) (bool, error) {
	var exists bool
	err := l.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM member
			WHERE user_id = $1 AND workspace_id = $2
		)
	`, userID, workspaceID).Scan(&exists)
	return exists, err
}

func (l *DBChatBindingLookup) CheckIssuePermission(ctx context.Context, workspaceID, _ pgtype.UUID, issueKey string) error {
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

type DBUserInfoResolver struct {
	pool *pgxpool.Pool
}

func NewDBUserInfoResolver(pool *pgxpool.Pool) *DBUserInfoResolver {
	return &DBUserInfoResolver{pool: pool}
}

func (r *DBUserInfoResolver) Resolve(ctx context.Context, channelName, externalUserID string) (ResolvedUser, error) {
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
		return ResolvedUser{}, err
	}
	return ResolvedUser{MulticaUserID: userID, DisplayName: name}, nil
}

var (
	_ Step              = (*userIdentityBindStep)(nil)
	_ Step              = (*chatBindCommandStep)(nil)
	_ ChatBindingLookup = (*DBChatBindingLookup)(nil)
	_ UserInfoResolver  = (*DBUserInfoResolver)(nil)
	_ AuthzStore        = (*DBChatBindingLookup)(nil)
)
