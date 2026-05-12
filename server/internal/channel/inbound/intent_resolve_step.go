package inbound

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"

	chintent "github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/channel/port"
	"github.com/multica-ai/multica/server/internal/util"
)

type IntentWorkspaceLookup interface {
	LookupPrimaryWorkspaceID(ctx context.Context, channelName, chatID string) (pgtype.UUID, error)
}

type intentResolveStep struct {
	resolvers []chintent.IntentResolver
	workspace IntentWorkspaceLookup
}

func NewIntentResolveStep(resolvers ...chintent.IntentResolver) Step {
	return NewIntentResolveStepWithWorkspace(nil, resolvers...)
}

func NewIntentResolveStepWithWorkspace(workspace IntentWorkspaceLookup, resolvers ...chintent.IntentResolver) Step {
	cp := make([]chintent.IntentResolver, 0, len(resolvers))
	for _, r := range resolvers {
		if r != nil {
			cp = append(cp, r)
		}
	}
	return &intentResolveStep{resolvers: cp, workspace: workspace}
}

func (*intentResolveStep) Name() string { return "intent-recog" }

func (s *intentResolveStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	req := chintent.IntentRequest{
		WorkspaceID:  s.lookupWorkspaceID(ctx, evt),
		Text:         evt.Text,
		Channel:      evt.ChannelName,
		ConnectionID: evt.ConnectionID(),
		ChatID:       evt.ChatID,
		ChatType:     string(evt.ChatType),
		SenderID:     evt.SenderID,
		SenderName:   evt.SenderName,
		SourceHint:   chintent.IntentSource(evt.Intent.Source),
	}

	for _, r := range s.resolvers {
		result, err := r.Resolve(ctx, req)
		if err != nil {
			return evt, DecisionContinue, err
		}
		if !result.Matched {
			continue
		}
		evt.Intent = toPortIntent(result.Intent)
		return evt, DecisionContinue, nil
	}

	evt.Intent = port.InboundIntent{
		Kind:   port.IntentUnknown,
		Source: port.SourceRule,
		Params: map[string]string{},
	}
	return evt, DecisionContinue, nil
}

func (s *intentResolveStep) lookupWorkspaceID(ctx context.Context, evt port.InboundEvent) string {
	if s.workspace == nil {
		return ""
	}
	wsID, err := s.workspace.LookupPrimaryWorkspaceID(ctx, evt.ConnectionID(), evt.ChatID)
	if err != nil || !wsID.Valid {
		return ""
	}
	return util.UUIDToString(wsID)
}

func toPortIntent(in chintent.Intent) port.InboundIntent {
	params := in.Params
	if params == nil {
		params = map[string]string{}
	}
	return port.InboundIntent{
		Kind:       port.IntentKind(in.Kind),
		Confidence: in.Confidence,
		Params:     params,
		Source:     port.IntentSource(in.Source),
	}
}

var _ Step = (*intentResolveStep)(nil)
