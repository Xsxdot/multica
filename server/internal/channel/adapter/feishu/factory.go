package feishu

import (
	"context"
	"os"
	"time"

	"github.com/multica-ai/multica/server/internal/channel/leader"
	"github.com/multica-ai/multica/server/internal/channel/provider"
)

const defaultReconnectDelay = 5 * time.Second

type Factory struct{}

func NewFactory() *Factory {
	return &Factory{}
}

func (*Factory) Provider() string {
	return channelName
}

func (*Factory) DisplayName() string {
	return "Feishu"
}

func (*Factory) EnvConfig() provider.ConnectionConfig {
	appID := os.Getenv("FEISHU_APP_ID")
	appSecret := os.Getenv("FEISHU_APP_SECRET")
	encryptKey := os.Getenv("FEISHU_ENCRYPT_KEY")
	verifyToken := os.Getenv("FEISHU_VERIFY_TOKEN")
	return provider.ConnectionConfig{
		Provider:     channelName,
		ConnectionID: channelName,
		DisplayName:  "Feishu",
		Enabled:      appID != "" && appSecret != "",
		Values: map[string]string{
			"app_id":       appID,
			"app_secret":   appSecret,
			"encrypt_key":  encryptKey,
			"verify_token": verifyToken,
		},
	}
}

func (*Factory) ConfigSchema() []provider.ConfigField {
	return []provider.ConfigField{
		{Key: "app_id", Label: "App ID", Required: true},
		{Key: "app_secret", Label: "App Secret", Required: true, Secret: true},
		{Key: "encrypt_key", Label: "Encrypt Key", Secret: true},
		{Key: "verify_token", Label: "Verify Token", Secret: true},
	}
}

func (*Factory) Build(_ context.Context, cfg provider.ConnectionConfig) (provider.Bundle, error) {
	appID := cfg.Value("app_id")
	appSecret := cfg.Value("app_secret")
	encryptKey := cfg.Value("encrypt_key")
	verifyToken := cfg.Value("verify_token")

	client := NewRealClient(appID, appSecret, encryptKey, verifyToken)
	adapter := NewAdapter(client, Config{
		AppID:       appID,
		AppSecret:   appSecret,
		EncryptKey:  encryptKey,
		VerifyToken: verifyToken,
	})
	return provider.Bundle{
		Channel:        adapter,
		FileDownloader: NewRealFileDownloader(client.APIClient()),
	}, nil
}

func (*Factory) LeaderLockID(provider.ConnectionConfig) (int64, bool) {
	return leader.ChannelFeishuLockID, true
}

func (*Factory) ReconnectDelay(provider.ConnectionConfig) time.Duration {
	return defaultReconnectDelay
}

var _ provider.Factory = (*Factory)(nil)
