package inbound

import (
	"net/url"
	"testing"
)

func TestChannelBindURLIncludesProviderAndConnection(t *testing.T) {
	t.Setenv("MULTICA_APP_URL", "https://app.example")

	raw := ChannelBindURL("chat", "plain token", "slack", "slack-team-a")
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse bind url: %v", err)
	}
	if parsed.Scheme != "https" || parsed.Host != "app.example" || parsed.Path != "/bind" {
		t.Fatalf("url = %s", raw)
	}
	query := parsed.Query()
	for key, want := range map[string]string{
		"kind":          "chat",
		"token":         "plain token",
		"provider":      "slack",
		"connection_id": "slack-team-a",
	} {
		if got := query.Get(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}
