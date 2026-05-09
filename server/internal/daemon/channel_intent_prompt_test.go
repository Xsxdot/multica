package daemon

import "testing"

func TestBuildPrompt_ChannelIntentUsesClassifierPrompt(t *testing.T) {
	t.Parallel()

	const prompt = `{"intent":"Unknown","confidence":0,"params":{}}`
	got := BuildPrompt(Task{
		ChannelIntentPrompt:  prompt,
		ChannelIntentMessage: "帮我建个任务",
	})
	if got != prompt {
		t.Fatalf("prompt = %q, want classifier prompt", got)
	}
}
