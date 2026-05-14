package outbound

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/channel/port"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// FailureRecordingCardSender adapts a port.Channel to the Aggregator's
// CardSender contract and records retryable send failures.
type FailureRecordingCardSender struct {
	channel  port.Channel
	failures FailureRecorder
	active   func() bool
}

func NewFailureRecordingCardSender(ch port.Channel, failures FailureRecorder) *FailureRecordingCardSender {
	return &FailureRecordingCardSender{channel: ch, failures: failures}
}

func (s *FailureRecordingCardSender) SetActiveFunc(active func() bool) {
	s.active = active
}

func (s *FailureRecordingCardSender) isActive() bool {
	return s.active == nil || s.active()
}

func (s *FailureRecordingCardSender) SendCard(externalUserID string, card port.OutboundCardMessage, meta AggregationMeta) error {
	if !s.isActive() {
		return nil
	}
	if card.Target.ID == "" {
		card.Target = port.TargetUser(externalUserID)
	}
	if card.ChatID == "" {
		card.ChatID = externalUserID
	}

	result, err := s.channel.SendCard(context.Background(), card)
	if err != nil && result.Retryable {
		s.recordFailure(externalUserID, card, meta, err)
	}
	return err
}

func (s *FailureRecordingCardSender) recordFailure(externalUserID string, card port.OutboundCardMessage, meta AggregationMeta, sendErr error) {
	if s.failures == nil {
		return
	}
	payload, err := json.Marshal(RetryPayload{
		Title:      card.Title,
		Body:       card.Body,
		TargetType: string(card.Target.Type),
		Mentions:   card.Mentions,
	})
	if err != nil {
		slog.Error("outbound aggregator: marshal retry payload", "external_user_id", externalUserID, "error", err)
		return
	}
	eventKind := meta.EventKind
	if eventKind == "" {
		eventKind = "aggregated"
	}
	if _, err := s.failures.InsertOutboundFailure(context.Background(), db.InsertOutboundFailureParams{
		Provider:             channelProviderName(s.channel),
		ConnectionID:         firstNonEmpty(meta.ConnectionID, s.channel.Name()),
		EventKind:            eventKind,
		TargetUserID:         meta.TargetUserID,
		TargetExternalUserID: pgtype.Text{String: card.Target.ID, Valid: card.Target.ID != ""},
		Payload:              payload,
		MaxAttempts:          3,
	}); err != nil {
		slog.Error("outbound aggregator: insert failure",
			"external_user_id", externalUserID,
			"send_error", sendErr,
			"error", err,
		)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ CardSender = (*FailureRecordingCardSender)(nil)
