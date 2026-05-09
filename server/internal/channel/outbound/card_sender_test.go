package outbound

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/channel/port"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type retryableCardChannel struct{}

func (retryableCardChannel) Name() string                     { return "feishu" }
func (retryableCardChannel) Connect(context.Context) error    { return nil }
func (retryableCardChannel) Disconnect(context.Context) error { return nil }
func (retryableCardChannel) Events() <-chan port.InboundEvent { return nil }
func (retryableCardChannel) Send(context.Context, port.OutboundMessage) (port.SendResult, error) {
	return port.SendResult{}, nil
}
func (retryableCardChannel) SendCard(context.Context, port.OutboundCardMessage) (port.SendResult, error) {
	return port.SendResult{Retryable: true}, errors.New("temporary send failure")
}
func (retryableCardChannel) GetChatInfo(context.Context, string) (port.ChatInfo, error) {
	return port.ChatInfo{}, nil
}
func (retryableCardChannel) GetUserInfo(context.Context, string) (port.UserInfo, error) {
	return port.UserInfo{}, nil
}

type recordingFailureStore struct {
	calls []db.InsertOutboundFailureParams
}

func (s *recordingFailureStore) InsertOutboundFailure(_ context.Context, arg db.InsertOutboundFailureParams) (db.ChannelOutboundFailure, error) {
	s.calls = append(s.calls, arg)
	return db.ChannelOutboundFailure{}, nil
}

func TestFailureRecordingCardSender_UsesAggregationMetadata(t *testing.T) {
	store := &recordingFailureStore{}
	sender := NewFailureRecordingCardSender(retryableCardChannel{}, store)
	targetUserID := pgtype.UUID{
		Bytes: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Valid: true,
	}

	err := sender.SendCard("ou_user_1", port.OutboundCardMessage{
		Title: "Issue updated",
		Body:  "Done",
	}, AggregationMeta{
		EventKind:    "status_done",
		TargetUserID: targetUserID,
	})

	if err == nil {
		t.Fatal("expected send error")
	}
	if len(store.calls) != 1 {
		t.Fatalf("expected 1 failure record, got %d", len(store.calls))
	}
	got := store.calls[0]
	if got.EventKind != "status_done" {
		t.Fatalf("EventKind = %q, want status_done", got.EventKind)
	}
	if got.TargetUserID != targetUserID {
		t.Fatalf("TargetUserID = %v, want %v", got.TargetUserID, targetUserID)
	}
	if !got.TargetExternalUserID.Valid || got.TargetExternalUserID.String != "ou_user_1" {
		t.Fatalf("TargetExternalUserID = %#v, want ou_user_1", got.TargetExternalUserID)
	}
}
