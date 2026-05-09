package intent_test

import (
	"context"
	"errors"
	"testing"

	in "github.com/multica-ai/multica/server/internal/channel/intent"
)

type fakeChatClient struct {
	raw string
	err error
}

func (f fakeChatClient) CompleteIntent(context.Context, in.IntentRequest) (string, error) {
	return f.raw, f.err
}

func TestRuleResolver_CommandSourceHint(t *testing.T) {
	t.Parallel()

	resolver := in.NewRuleResolver(in.NewRuleMatcher())
	got, err := resolver.Resolve(context.Background(), in.IntentRequest{
		Text:       "帮我记一个 登录优化",
		SourceHint: in.SourceCommand,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Matched {
		t.Fatal("expected rule match")
	}
	if got.Intent.Source != in.SourceCommand {
		t.Fatalf("source = %q, want command", got.Intent.Source)
	}
}

func TestChatIntentResolver_ValidJSON(t *testing.T) {
	t.Parallel()

	resolver := in.NewChatIntentResolver(in.ChatIntentResolverConfig{
		Client: fakeChatClient{raw: `{"intent":"CreateIssue","confidence":0.92,"params":{"title":"登录页白屏"}}`},
	})
	got, err := resolver.Resolve(context.Background(), in.IntentRequest{WorkspaceID: "ws-1", Text: "登录页白屏，帮我建个任务"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Matched {
		t.Fatal("expected chat match")
	}
	if got.Intent.Kind != in.IntentCreateIssue {
		t.Fatalf("kind = %q, want CreateIssue", got.Intent.Kind)
	}
	if got.Intent.Source != in.SourceChat {
		t.Fatalf("source = %q, want chat", got.Intent.Source)
	}
	if got.Intent.Params["title"] != "登录页白屏" {
		t.Fatalf("title = %q", got.Intent.Params["title"])
	}
}

func TestChatIntentResolver_SafeDowngrades(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		err  error
		want in.IntentKind
	}{
		{name: "invalid json", raw: `not json`, want: in.IntentUnknown},
		{name: "unknown intent", raw: `{"intent":"CloseWorkspace","confidence":0.9,"params":{}}`, want: in.IntentUnknown},
		{name: "missing params", raw: `{"intent":"CreateIssue","confidence":0.9,"params":{}}`, want: in.IntentASKClarify},
		{name: "low confidence", raw: `{"intent":"CreateIssue","confidence":0.4,"params":{"title":"x"}}`, want: in.IntentASKClarify},
		{name: "client error", err: errors.New("boom"), want: in.IntentUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resolver := in.NewChatIntentResolver(in.ChatIntentResolverConfig{
				Client: fakeChatClient{raw: tc.raw, err: tc.err},
			})
			got, err := resolver.Resolve(context.Background(), in.IntentRequest{WorkspaceID: "ws-1", Text: "free text"})
			if err != nil {
				t.Fatal(err)
			}
			if !got.Matched {
				t.Fatal("expected safe matched fallback")
			}
			if got.Intent.Kind != tc.want {
				t.Fatalf("kind = %q, want %q", got.Intent.Kind, tc.want)
			}
			if got.Intent.Source != in.SourceChat {
				t.Fatalf("source = %q, want chat", got.Intent.Source)
			}
		})
	}
}

func TestChatIntentResolver_DestructiveDeleteBecomesUnsupported(t *testing.T) {
	t.Parallel()

	resolver := in.NewChatIntentResolver(in.ChatIntentResolverConfig{
		Client: fakeChatClient{raw: `{"intent":"Delete","confidence":0.97,"params":{"issue_key":"STA-2"}}`},
	})
	got, err := resolver.Resolve(context.Background(), in.IntentRequest{WorkspaceID: "ws-1", Text: "删除 STA-2"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Matched {
		t.Fatal("expected chat match")
	}
	if got.Intent.Kind != in.IntentUnsupported {
		t.Fatalf("kind = %q, want Unsupported", got.Intent.Kind)
	}
}

func TestChatIntentResolver_NilClientNoMatch(t *testing.T) {
	t.Parallel()

	got, err := in.NewChatIntentResolver(in.ChatIntentResolverConfig{}).Resolve(context.Background(), in.IntentRequest{Text: "anything"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Matched {
		t.Fatal("nil chat client should not match")
	}
}

func TestChatIntentResolver_EmptyWorkspaceNoMatch(t *testing.T) {
	t.Parallel()

	got, err := in.NewChatIntentResolver(in.ChatIntentResolverConfig{
		Client: fakeChatClient{raw: `{"intent":"CreateIssue","confidence":0.9,"params":{"title":"x"}}`},
	}).Resolve(context.Background(), in.IntentRequest{Text: "anything"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Matched {
		t.Fatal("chat resolver should not run without workspace context")
	}
}
