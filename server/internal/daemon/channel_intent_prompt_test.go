package daemon

import "testing"

func TestBuildPrompt_ChannelIntentUsesClassifierPrompt(t *testing.T) {
	t.Parallel()

	const prompt = `{"intent":"Unknown","confidence":0,"params":{}}`
	got := BuildPrompt(Task{
		ChannelIntentPrompt:  prompt,
		ChannelIntentMessage: "帮我建个任务",
	}, "codex")
	if got != prompt {
		t.Fatalf("prompt = %q, want classifier prompt", got)
	}
}

func TestBuildPrompt_ChannelTurnUsesAgentPrompt(t *testing.T) {
	t.Parallel()

	const prompt = `Use multica issue list --output json, then answer naturally.`
	got := BuildPrompt(Task{
		ChannelTurnPrompt:  prompt,
		ChannelTurnMessage: "各项目进展怎么样？",
	}, "codex")
	if got != prompt {
		t.Fatalf("prompt = %q, want channel turn prompt", got)
	}
}
