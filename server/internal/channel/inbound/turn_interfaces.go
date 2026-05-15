package inbound

import (
	"context"

	"github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

type ChannelTurnExecution struct {
	Intent port.InboundIntent
	Reply  string
	Rich   *port.OutboundRichMessage
}

type ChannelTurnExecutor interface {
	ExecuteTurn(ctx context.Context, evt port.InboundEvent, plan intent.ChannelTurnPlan) (ChannelTurnExecution, error)
}

type ChannelReplyComposer interface {
	ComposeReply(ctx context.Context, req intent.ChannelComposeRequest) (string, error)
}
