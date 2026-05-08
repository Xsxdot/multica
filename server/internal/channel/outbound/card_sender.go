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
}

func NewFailureRecordingCardSender(ch port.Channel, failures FailureRecorder) *FailureRecordingCardSender {
	return &FailureRecordingCardSender{channel: ch, failures: failures}
}

func (s *FailureRecordingCardSender) SendCard(externalUserID string, card port.OutboundCardMessage) error {
	if card.Target.ID == "" {
		card.Target = port.TargetUser(externalUserID)
	}
	if card.ChatID == "" {
		card.ChatID = externalUserID
	}

	result, err := s.channel.SendCard(context.Background(), card)
	if err != nil && result.Retryable {
		s.recordFailure(externalUserID, card, err)
	}
	return err
}

func (s *FailureRecordingCardSender) recordFailure(externalUserID string, card port.OutboundCardMessage, sendErr error) {
	if s.failures == nil {
		return
	}
	payload, err := json.Marshal(RetryPayload{
		Title: card.Title,
		Body:  card.Body,
	})
	if err != nil {
		slog.Error("outbound aggregator: marshal retry payload", "external_user_id", externalUserID, "error", err)
		return
	}
	if _, err := s.failures.InsertOutboundFailure(context.Background(), db.InsertOutboundFailureParams{
		Provider:             s.channel.Name(),
		EventKind:            "aggregated",
		TargetUserID:         pgtype.UUID{},
		TargetExternalUserID: pgtype.Text{String: externalUserID, Valid: externalUserID != ""},
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

var _ CardSender = (*FailureRecordingCardSender)(nil)
