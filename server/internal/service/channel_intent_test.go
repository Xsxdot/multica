package service

import (
	"context"
	"encoding/json"
	"testing"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestResolveTaskWorkspaceID_ChannelIntentContext(t *testing.T) {
	t.Parallel()

	task := channelIntentTask(t, "00000000-0000-0000-0000-000000000123")
	svc := &TaskService{}

	if got := svc.ResolveTaskWorkspaceID(context.Background(), task); got != "00000000-0000-0000-0000-000000000123" {
		t.Fatalf("workspace id = %q", got)
	}
}

func TestChannelIntentTaskSkipsTaskBroadcasts(t *testing.T) {
	t.Parallel()

	task := channelIntentTask(t, "00000000-0000-0000-0000-000000000123")
	svc := &TaskService{}

	svc.broadcastTaskDispatch(context.Background(), task)
	svc.broadcastTaskEvent(context.Background(), "task:completed", task)
}

func TestResolveTaskWorkspaceID_ChannelTurnContext(t *testing.T) {
	t.Parallel()

	task := channelTurnTask(t, "00000000-0000-0000-0000-000000000456")
	svc := &TaskService{}

	if got := svc.ResolveTaskWorkspaceID(context.Background(), task); got != "00000000-0000-0000-0000-000000000456" {
		t.Fatalf("workspace id = %q", got)
	}
}

func TestChannelTurnTaskSkipsTaskBroadcasts(t *testing.T) {
	t.Parallel()

	task := channelTurnTask(t, "00000000-0000-0000-0000-000000000456")
	svc := &TaskService{}

	svc.broadcastTaskDispatch(context.Background(), task)
	svc.broadcastTaskEvent(context.Background(), "task:completed", task)
}

func channelIntentTask(t *testing.T, workspaceID string) db.AgentTaskQueue {
	t.Helper()

	payload, err := json.Marshal(ChannelIntentContext{
		Type:        ChannelIntentContextType,
		WorkspaceID: workspaceID,
		Prompt:      `{"intent":"Unknown","confidence":0,"params":{}}`,
		Message:     "帮我建个任务",
	})
	if err != nil {
		t.Fatal(err)
	}
	return db.AgentTaskQueue{Context: payload}
}

func channelTurnTask(t *testing.T, workspaceID string) db.AgentTaskQueue {
	t.Helper()

	payload, err := json.Marshal(ChannelTurnContext{
		Type:        ChannelTurnContextType,
		WorkspaceID: workspaceID,
		Prompt:      "handle channel turn",
		Message:     "各项目进展怎么样？",
	})
	if err != nil {
		t.Fatal(err)
	}
	return db.AgentTaskQueue{Context: payload}
}
