package facadeimpl

import (
	"testing"

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
