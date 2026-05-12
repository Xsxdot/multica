package manager

import (
	"context"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/channel/provider"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type fakeFactory struct {
	providerName string
	displayName  string
	env          provider.ConnectionConfig
}

func (f fakeFactory) Provider() string {
	return f.providerName
}

func (f fakeFactory) DisplayName() string {
	return f.displayName
}

func (f fakeFactory) EnvConfig() provider.ConnectionConfig {
	return f.env
}

func (f fakeFactory) ConfigSchema() []provider.ConfigField {
	return nil
}

func (f fakeFactory) Build(context.Context, provider.ConnectionConfig) (provider.Bundle, error) {
	return provider.Bundle{}, nil
}

func (f fakeFactory) LeaderLockID(provider.ConnectionConfig) (int64, bool) {
	return 0, false
}

func (f fakeFactory) ReconnectDelay(provider.ConnectionConfig) time.Duration {
	return time.Second
}

func TestDBConnectionsSupportsMultipleConnectionsForSameProvider(t *testing.T) {
	factory := fakeFactory{
		providerName: "feishu",
		displayName:  "Feishu",
		env: provider.ConnectionConfig{
			Provider: "feishu",
			Values: map[string]string{
				"app_id":     "env-app",
				"app_secret": "env-secret",
				"env_only":   "must-not-leak",
			},
		},
	}
	mgr := New(Config{Factories: []provider.Factory{factory}})

	got := mgr.dbConnections(context.Background(), []db.ChannelConnection{
		{
			ID:           "feishu-a",
			Provider:     "feishu",
			DisplayName:  "Feishu A",
			Enabled:      true,
			Config:       []byte(`{"app_id":"app-a"}`),
			SecretConfig: []byte(`{"app_secret":"secret-a"}`),
		},
		{
			ID:           "feishu-b",
			Provider:     "feishu",
			DisplayName:  "Feishu B",
			Enabled:      true,
			Config:       []byte(`{"app_id":"app-b"}`),
			SecretConfig: []byte(`{"app_secret":"secret-b"}`),
		},
	})

	if len(got) != 2 {
		t.Fatalf("dbConnections returned %d entries, want 2", len(got))
	}
	if got[0].config.ConnectionID != "feishu-a" || got[1].config.ConnectionID != "feishu-b" {
		t.Fatalf("connection ids = %q, %q; want feishu-a, feishu-b", got[0].config.ConnectionID, got[1].config.ConnectionID)
	}
	if got[0].config.Value("app_id") != "app-a" || got[0].config.Value("app_secret") != "secret-a" {
		t.Fatalf("first config = %#v, want row config and secret", got[0].config.Values)
	}
	if got[1].config.Value("app_id") != "app-b" || got[1].config.Value("app_secret") != "secret-b" {
		t.Fatalf("second config = %#v, want row config and secret", got[1].config.Values)
	}
	if _, ok := got[0].config.Values["env_only"]; ok {
		t.Fatalf("dbConnections leaked env-only config into DB-backed connection: %#v", got[0].config.Values)
	}
}

func TestEnvConnectionsDefaultsConnectionIDToProvider(t *testing.T) {
	factory := fakeFactory{
		providerName: "slack",
		displayName:  "Slack",
		env: provider.ConnectionConfig{
			Provider: "slack",
			Enabled:  true,
		},
	}
	mgr := New(Config{Factories: []provider.Factory{factory}})

	got := mgr.envConnections()
	if len(got) != 1 {
		t.Fatalf("envConnections returned %d entries, want 1", len(got))
	}
	if got[0].config.ConnectionID != "slack" {
		t.Fatalf("connection id = %q, want provider key fallback", got[0].config.ConnectionID)
	}
	if got[0].config.DisplayName != "Slack" {
		t.Fatalf("display name = %q, want factory display name fallback", got[0].config.DisplayName)
	}
}

func TestConnectionChannelUsesConnectionIDAsRegistryName(t *testing.T) {
	base := fakeChannel{name: "feishu"}
	wrapped := newConnectionChannel("feishu-prod", base)

	if wrapped.Name() != "feishu-prod" {
		t.Fatalf("Name() = %q, want connection id", wrapped.Name())
	}
	if wrapped.ProviderName() != "feishu" {
		t.Fatalf("ProviderName() = %q, want base provider name", wrapped.ProviderName())
	}
	if wrapped.ConnectionID() != "feishu-prod" {
		t.Fatalf("ConnectionID() = %q, want configured connection id", wrapped.ConnectionID())
	}
}

type fakeChannel struct {
	name string
}

func (f fakeChannel) Name() string {
	return f.name
}

func (f fakeChannel) Connect(context.Context) error {
	return nil
}

func (f fakeChannel) Disconnect(context.Context) error {
	return nil
}

func (f fakeChannel) Events() <-chan port.InboundEvent {
	return nil
}

func (f fakeChannel) Send(context.Context, port.OutboundMessage) (port.SendResult, error) {
	return port.SendResult{}, nil
}

func (f fakeChannel) SendCard(context.Context, port.OutboundCardMessage) (port.SendResult, error) {
	return port.SendResult{}, nil
}

func (f fakeChannel) GetChatInfo(context.Context, string) (port.ChatInfo, error) {
	return port.ChatInfo{}, nil
}

func (f fakeChannel) GetUserInfo(context.Context, string) (port.UserInfo, error) {
	return port.UserInfo{}, nil
}
