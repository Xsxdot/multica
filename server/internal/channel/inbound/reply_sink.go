package inbound

import (
	"context"
	"fmt"

	"github.com/multica-ai/multica/server/internal/channel"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

type ChannelReplySink interface {
	Send(ctx context.Context, evt port.InboundEvent, text string) error
}

type RegistryReplySink struct {
	registry *channel.Registry
}

func NewRegistryReplySink(registry *channel.Registry) *RegistryReplySink {
	return &RegistryReplySink{registry: registry}
}

func (s *RegistryReplySink) Send(ctx context.Context, evt port.InboundEvent, text string) error {
	if s == nil || s.registry == nil || text == "" {
		return nil
	}
	ch, err := s.registry.Get(evt.ConnectionID())
	if err != nil {
		return fmt.Errorf("channel connection %q not in registry: %w", evt.ConnectionID(), err)
	}
	target := port.TargetChat(evt.ChatID)
	if evt.ChatType == port.ChatTypeDirect {
		target = port.TargetUser(evt.SenderID)
	}
	_, err = ch.Send(ctx, port.OutboundMessage{
		Target: target,
		ChatID: evt.ChatID,
		Text:   text,
	})
	return err
}

var _ ChannelReplySink = (*RegistryReplySink)(nil)
