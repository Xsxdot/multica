package inbound

import (
	"context"

	"github.com/multica-ai/multica/server/internal/channel/port"
)

type ChannelReplySink interface {
	SendText(ctx context.Context, evt port.InboundEvent, msg port.OutboundMessage) error
	SendRich(ctx context.Context, evt port.InboundEvent, msg port.OutboundRichMessage) error
}

type GatewayReplySink struct {
	gateway port.ChannelGateway
}

func NewGatewayReplySink(gateway port.ChannelGateway) *GatewayReplySink {
	return &GatewayReplySink{gateway: gateway}
}

func (s *GatewayReplySink) SendText(ctx context.Context, evt port.InboundEvent, msg port.OutboundMessage) error {
	if s == nil || s.gateway == nil || msg.Text == "" {
		return nil
	}
	msg.Target = defaultReplyTarget(evt, msg.Target)
	if msg.ChatID == "" {
		msg.ChatID = evt.ChatID
	}
	_, err := s.gateway.SendText(ctx, evt.ConnectionID(), msg)
	return err
}

func (s *GatewayReplySink) SendRich(ctx context.Context, evt port.InboundEvent, msg port.OutboundRichMessage) error {
	if s == nil || s.gateway == nil || (msg.Title == "" && msg.Body == "") {
		return nil
	}
	msg.Target = defaultReplyTarget(evt, msg.Target)
	if msg.ChatID == "" {
		msg.ChatID = evt.ChatID
	}
	_, err := s.gateway.SendRich(ctx, evt.ConnectionID(), msg)
	return err
}

func defaultReplyTarget(evt port.InboundEvent, target port.OutboundTarget) port.OutboundTarget {
	if target.ID != "" {
		return target
	}
	if evt.ChatType == port.ChatTypeDirect {
		return port.TargetUser(evt.SenderID)
	}
	return port.TargetChat(evt.ChatID)
}

var _ ChannelReplySink = (*GatewayReplySink)(nil)
