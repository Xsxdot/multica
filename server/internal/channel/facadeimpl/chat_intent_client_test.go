package facadeimpl

import (
	"encoding/json"
	"testing"

	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestChannelIntentOutput(t *testing.T) {
	t.Parallel()

	got, err := channelIntentOutput(db.AgentTaskQueue{
		Result: []byte(`{"output":"{\"intent\":\"CreateIssue\",\"confidence\":0.9,\"params\":{\"title\":\"x\"}}"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"intent":"CreateIssue","confidence":0.9,"params":{"title":"x"}}` {
		t.Fatalf("output = %q", got)
	}
}

func TestChannelIntentOutput_EmptyIsError(t *testing.T) {
	t.Parallel()

	if _, err := channelIntentOutput(db.AgentTaskQueue{Result: []byte(`{"output":""}`)}); err == nil {
		t.Fatal("expected empty output error")
	}
}

func TestChannelIntentTaskMessage(t *testing.T) {
	t.Parallel()

	contextJSON, err := json.Marshal(service.ChannelIntentContext{
		Type:    service.ChannelIntentContextType,
		Message: "sta-1 这个 issue 怎么样了",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := channelIntentTaskMessage(db.AgentTaskQueue{Context: contextJSON})
	if got != "sta-1 这个 issue 怎么样了" {
		t.Fatalf("message = %q", got)
	}
}

func TestRuntimeSupportsChannelIntent(t *testing.T) {
	t.Parallel()

	if !runtimeSupportsChannelIntent(db.AgentRuntime{
		Metadata: []byte(`{"capabilities":["channel_intent"]}`),
	}) {
		t.Fatal("expected capability to be accepted")
	}
	if runtimeSupportsChannelIntent(db.AgentRuntime{
		Metadata: []byte(`{"capabilities":["other"]}`),
	}) {
		t.Fatal("unexpected support without channel_intent capability")
	}
	if runtimeSupportsChannelIntent(db.AgentRuntime{
		Metadata: []byte(`{"cli_version":"999.0.0"}`),
	}) {
		t.Fatal("version alone must not imply channel_intent support")
	}
}
