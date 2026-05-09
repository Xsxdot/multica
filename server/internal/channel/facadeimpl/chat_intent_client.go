package facadeimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	chintent "github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const defaultChannelIntentPollInterval = 200 * time.Millisecond

type ChannelIntentAccess interface {
	ResolveUserID(ctx context.Context, channelName, externalUserID string) (pgtype.UUID, error)
	IsWorkspaceMember(ctx context.Context, userID, workspaceID pgtype.UUID) (bool, error)
}

type TaskBackedChatIntentClient struct {
	queries      *db.Queries
	tasks        *service.TaskService
	access       ChannelIntentAccess
	pollInterval time.Duration
}

func NewTaskBackedChatIntentClient(queries *db.Queries, tasks *service.TaskService, access ChannelIntentAccess) *TaskBackedChatIntentClient {
	return &TaskBackedChatIntentClient{
		queries:      queries,
		tasks:        tasks,
		access:       access,
		pollInterval: defaultChannelIntentPollInterval,
	}
}

func (c *TaskBackedChatIntentClient) CompleteIntent(ctx context.Context, req chintent.IntentRequest) (string, error) {
	taskID, err := c.StartIntent(ctx, req)
	if err != nil {
		return "", err
	}
	taskUUID, err := util.ParseUUID(taskID)
	if err != nil {
		return "", err
	}
	output, err := c.waitForResult(ctx, taskUUID)
	if err != nil {
		c.cancelTask(taskUUID)
		return "", err
	}
	return output, nil
}

func (c *TaskBackedChatIntentClient) StartIntent(ctx context.Context, req chintent.IntentRequest) (string, error) {
	if c == nil || c.queries == nil || c.tasks == nil {
		return "", fmt.Errorf("chat intent client is not configured")
	}
	workspaceID, err := util.ParseUUID(strings.TrimSpace(req.WorkspaceID))
	if err != nil {
		return "", err
	}
	requesterID, err := c.authorizeRequester(ctx, req, workspaceID)
	if err != nil {
		return "", err
	}
	if req.InboundEventID != "" {
		existing, err := c.queries.GetChannelIntentTaskByInboundEvent(ctx, req.InboundEventID)
		if err == nil {
			return util.UUIDToString(existing.ID), nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("lookup existing channel intent task: %w", err)
		}
	}
	agent, err := c.selectAgent(ctx, workspaceID)
	if err != nil {
		return "", err
	}

	task, err := c.tasks.EnqueueChannelIntentTask(ctx, workspaceID, agent.ID, service.ChannelIntentTaskParams{
		Prompt:         chintent.BuildChatIntentPrompt(req),
		Message:        req.Text,
		RequesterID:    requesterID,
		Channel:        req.Channel,
		ChatID:         req.ChatID,
		ChatType:       req.ChatType,
		SenderID:       req.SenderID,
		SenderName:     req.SenderName,
		InboundEventID: req.InboundEventID,
	})
	if err != nil {
		return "", err
	}
	return util.UUIDToString(task.ID), nil
}

func (c *TaskBackedChatIntentClient) ParseIntentResult(ctx context.Context, taskID string) (chintent.IntentResult, bool, error) {
	if c == nil || c.queries == nil {
		return chintent.IntentResult{}, true, fmt.Errorf("chat intent client is not configured")
	}
	taskUUID, err := util.ParseUUID(strings.TrimSpace(taskID))
	if err != nil {
		return chintent.IntentResult{}, true, err
	}
	task, err := c.queries.GetAgentTask(ctx, taskUUID)
	if err != nil {
		return chintent.IntentResult{}, true, fmt.Errorf("load channel intent task: %w", err)
	}
	switch task.Status {
	case "completed":
		output, err := channelIntentOutput(task)
		if err != nil {
			return chintent.IntentResult{}, true, err
		}
		result, err := chintent.NormalizeChatIntentResult(output)
		if err != nil {
			return chintent.IntentResult{
				Matched: true,
				Intent: chintent.Intent{
					Kind:       chintent.IntentUnknown,
					Confidence: 0,
					Params:     map[string]string{},
					Source:     chintent.SourceChat,
				},
			}, true, nil
		}
		return result, true, nil
	case "failed":
		if task.Error.Valid && strings.TrimSpace(task.Error.String) != "" {
			return chintent.IntentResult{}, true, fmt.Errorf("channel intent task failed: %s", task.Error.String)
		}
		return chintent.IntentResult{}, true, fmt.Errorf("channel intent task failed")
	case "cancelled":
		return chintent.IntentResult{}, true, fmt.Errorf("channel intent task cancelled")
	default:
		return chintent.IntentResult{}, false, nil
	}
}

func (c *TaskBackedChatIntentClient) authorizeRequester(ctx context.Context, req chintent.IntentRequest, workspaceID pgtype.UUID) (string, error) {
	if c.access == nil {
		return "", nil
	}
	userID, err := c.access.ResolveUserID(ctx, req.Channel, req.SenderID)
	if err != nil {
		return "", fmt.Errorf("resolve channel user: %w", err)
	}
	if !userID.Valid {
		return "", fmt.Errorf("resolve channel user: invalid user id")
	}
	member, err := c.access.IsWorkspaceMember(ctx, userID, workspaceID)
	if err != nil {
		return "", fmt.Errorf("check workspace membership: %w", err)
	}
	if !member {
		return "", fmt.Errorf("sender is not a workspace member")
	}
	return util.UUIDToString(userID), nil
}

func (c *TaskBackedChatIntentClient) selectAgent(ctx context.Context, workspaceID pgtype.UUID) (db.Agent, error) {
	agents, err := c.queries.ListAgents(ctx, workspaceID)
	if err != nil {
		return db.Agent{}, fmt.Errorf("list agents: %w", err)
	}
	for _, agent := range agents {
		if !agent.RuntimeID.Valid {
			continue
		}
		runtime, err := c.queries.GetAgentRuntime(ctx, agent.RuntimeID)
		if err != nil || runtime.Status != "online" || !runtimeSupportsChannelIntent(runtime) {
			continue
		}
		return agent, nil
	}
	return db.Agent{}, fmt.Errorf("no online channel-intent-capable agent runtime available")
}

func runtimeSupportsChannelIntent(runtime db.AgentRuntime) bool {
	var metadata struct {
		Capabilities []string `json:"capabilities"`
	}
	if err := json.Unmarshal(runtime.Metadata, &metadata); err != nil {
		return false
	}
	for _, capability := range metadata.Capabilities {
		if capability == protocol.DaemonCapabilityChannelIntent {
			return true
		}
	}
	return false
}

func (c *TaskBackedChatIntentClient) waitForResult(ctx context.Context, taskID pgtype.UUID) (string, error) {
	pollInterval := c.pollInterval
	if pollInterval <= 0 {
		pollInterval = defaultChannelIntentPollInterval
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		task, err := c.queries.GetAgentTask(ctx, taskID)
		if err != nil {
			return "", fmt.Errorf("load channel intent task: %w", err)
		}
		switch task.Status {
		case "completed":
			return channelIntentOutput(task)
		case "failed":
			if task.Error.Valid && strings.TrimSpace(task.Error.String) != "" {
				return "", fmt.Errorf("channel intent task failed: %s", task.Error.String)
			}
			return "", fmt.Errorf("channel intent task failed")
		case "cancelled":
			return "", fmt.Errorf("channel intent task cancelled")
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *TaskBackedChatIntentClient) cancelTask(taskID pgtype.UUID) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = c.tasks.CancelTask(ctx, taskID)
}

func channelIntentOutput(task db.AgentTaskQueue) (string, error) {
	var payload protocol.TaskCompletedPayload
	if err := json.Unmarshal(task.Result, &payload); err != nil {
		return "", fmt.Errorf("parse channel intent task result: %w", err)
	}
	output := strings.TrimSpace(payload.Output)
	if output == "" {
		return "", fmt.Errorf("channel intent task completed without output")
	}
	return output, nil
}

var (
	_ chintent.ChatIntentClient      = (*TaskBackedChatIntentClient)(nil)
	_ chintent.AsyncChatIntentClient = (*TaskBackedChatIntentClient)(nil)
)
