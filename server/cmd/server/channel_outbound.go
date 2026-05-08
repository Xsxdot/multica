package main

import (
	"context"
	"fmt"

	"github.com/multica-ai/multica/server/internal/channel"
	feishucard "github.com/multica-ai/multica/server/internal/channel/adapter/feishu/card"
	"github.com/multica-ai/multica/server/internal/channel/outbound"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

type registryChannel struct {
	registry *channel.Registry
	provider string
}

func newRegistryChannel(registry *channel.Registry, provider string) *registryChannel {
	return &registryChannel{registry: registry, provider: provider}
}

func (c *registryChannel) Name() string { return c.provider }

func (c *registryChannel) Connect(context.Context) error { return nil }

func (c *registryChannel) Disconnect(context.Context) error { return nil }

func (c *registryChannel) Events() <-chan port.InboundEvent { return nil }

func (c *registryChannel) Send(ctx context.Context, msg port.OutboundMessage) (port.SendResult, error) {
	ch, err := c.registry.Get(c.provider)
	if err != nil {
		return port.SendResult{Retryable: true}, fmt.Errorf("registry channel: get %s: %w", c.provider, err)
	}
	return ch.Send(ctx, msg)
}

func (c *registryChannel) SendCard(ctx context.Context, msg port.OutboundCardMessage) (port.SendResult, error) {
	ch, err := c.registry.Get(c.provider)
	if err != nil {
		return port.SendResult{Retryable: true}, fmt.Errorf("registry channel: get %s: %w", c.provider, err)
	}
	rendered, err := renderFeishuCard(msg.Title, msg.Body)
	if err != nil {
		return port.SendResult{Retryable: false}, err
	}
	msg.Body = rendered
	return ch.SendCard(ctx, msg)
}

func (c *registryChannel) GetChatInfo(ctx context.Context, chatID string) (port.ChatInfo, error) {
	ch, err := c.registry.Get(c.provider)
	if err != nil {
		return port.ChatInfo{}, fmt.Errorf("registry channel: get %s: %w", c.provider, err)
	}
	return ch.GetChatInfo(ctx, chatID)
}

func (c *registryChannel) GetUserInfo(ctx context.Context, userID string) (port.UserInfo, error) {
	ch, err := c.registry.Get(c.provider)
	if err != nil {
		return port.UserInfo{}, fmt.Errorf("registry channel: get %s: %w", c.provider, err)
	}
	return ch.GetUserInfo(ctx, userID)
}

type registryRetrySender struct {
	registry *channel.Registry
}

func newRegistryRetrySender(registry *channel.Registry) *registryRetrySender {
	return &registryRetrySender{registry: registry}
}

func (s *registryRetrySender) SendCard(ctx context.Context, provider string, externalUserID string, payload outbound.RetryPayload) error {
	ch, err := s.registry.Get(provider)
	if err != nil {
		return outbound.WrapRetryable(fmt.Errorf("retry sender: get %s: %w", provider, err))
	}
	rendered, err := renderFeishuCard(payload.Title, payload.Body)
	if err != nil {
		return err
	}
	result, err := ch.SendCard(ctx, port.OutboundCardMessage{
		Target: port.TargetUser(externalUserID),
		ChatID: externalUserID,
		Title:  payload.Title,
		Body:   rendered,
	})
	if err != nil && result.Retryable {
		return outbound.WrapRetryable(err)
	}
	return err
}

func renderFeishuCard(title, body string) (string, error) {
	card := feishucard.NewCard(title, "blue")
	if body != "" {
		card.AddMarkdown(body)
	}
	return card.Render()
}

var (
	_ port.Channel         = (*registryChannel)(nil)
	_ outbound.RetrySender = (*registryRetrySender)(nil)
)
