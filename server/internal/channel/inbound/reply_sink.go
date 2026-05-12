package inbound

import (
	"context"

	"github.com/multica-ai/multica/server/internal/channel/port"
)

type ChannelReplySink interface {
	Send(ctx context.Context, evt port.InboundEvent, text string) error
}

type GatewayReplySink struct {
	gateway port.ChannelGateway
}

func NewGatewayReplySink(gateway port.ChannelGateway) *GatewayReplySink {
	return &GatewayReplySink{gateway: gateway}
}

func (s *GatewayReplySink) Send(ctx context.Context, evt port.InboundEvent, text string) error {
	if s == nil || s.gateway == nil || text == "" {
		return nil
	}
	target := port.TargetChat(evt.ChatID)
	if evt.ChatType == port.ChatTypeDirect {
		target = port.TargetUser(evt.SenderID)
	}
	_, err := s.gateway.SendText(ctx, evt.ConnectionID(), port.OutboundMessage{
		Target: target,
		ChatID: evt.ChatID,
		Text:   text,
	})
	return err
}

var _ ChannelReplySink = (*GatewayReplySink)(nil)
