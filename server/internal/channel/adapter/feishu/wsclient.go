// Package feishu implements the port.Channel adapter for Feishu (Lark).
//
// Architecture (DESIGN §3.1):
//
//	+------------------+      +-----------------+      +------------------+
//	| dispatcher (T11) | <--- |  Adapter (this) | ---> |  Client (seam)   |
//	+------------------+      +-----------------+      +------------------+
//	                                                            |
//	                                                            v
//	                                                  +-------------------+
//	                                                  |  Feishu OAPI SDK  |
//	                                                  |  (wired in T7 +)  |
//	                                                  +-------------------+
//
// The Adapter never imports the Feishu SDK directly. All platform interaction
// goes through the Client interface defined in this file, and the SDK-backed
// concrete implementation is wired up by a follow-up task (the M1-T7 leader-
// election + bot wiring task). For unit tests, a fakeFeishuClient sits behind
// the same interface, which is why TC-adapt-1 / TC-adapt-2 do not need a
// running Feishu account.
package feishu

import (
	"context"
	"encoding/json"
)

// Client is the seam between the adapter and the Feishu OpenAPI / WebSocket
// SDK. Every platform-touching call the adapter makes goes through this
// interface — so swapping in a fake (or, later, a different SDK version) is a
// single dependency-injection change.
//
// Lifecycle:
//
//   - Start opens the WebSocket long connection and begins delivering events
//     on Subscribe(). It must return only after the initial handshake
//     succeeds (or fail fast on auth errors). Reconnect / replay is the
//     concrete implementation's responsibility (PRD AC2.1: 30s outage with
//     no message loss is delivered by the SDK's replay buffer; T7 wires
//     reconnect alarms).
//   - Stop closes the events channel returned by Subscribe so downstream
//     `for range` consumers terminate cleanly.
//   - Subscribe returns the same channel across calls; emitting on a closed
//     channel after Stop is a programming error.
//   - SendMessage is synchronous — the caller (Adapter.Send) translates the
//     response into a port.SendResult, including the Retryable judgement.
//
// All DTOs (RawEvent, SendRequest, …) are defined alongside the interface so
// test doubles only need to import this one package.
type Client interface {
	// Start establishes the platform connection. Idempotent: calling Start
	// twice on the same Client returns nil the second time.
	Start(ctx context.Context) error

	// Stop tears down the platform connection. After Stop returns,
	// Subscribe()'s channel is closed.
	Stop(ctx context.Context) error

	// Subscribe returns the receive-only event stream. The same channel
	// must be returned across calls (callers may cache the reference).
	Subscribe() <-chan RawEvent

	// BotUserID returns the open_id of the bot account. The adapter uses
	// this to recognise and strip @-mentions of itself from incoming text
	// (Issue 关键实现要点 §4: "@_user_<bot_id>" must not survive into
	// InboundEvent.Text).
	BotUserID() string

	// SendMessage POSTs an im.v1.messages.create request to the Feishu
	// OpenAPI. The error returned must be classifiable by IsRetryable so
	// the adapter can populate SendResult.Retryable correctly.
	SendMessage(ctx context.Context, req SendRequest) (SendResponse, error)

	// GetChatInfo / GetUserInfo fetch metadata used by downstream commands
	// (e.g. binding flow, intent disambiguation). MVP only requires the
	// minimum surface; richer fields can be added without breaking tests
	// because the adapter projects this DTO into port.ChatInfo / UserInfo.
	GetChatInfo(ctx context.Context, chatID string) (ChatInfoResponse, error)
	GetUserInfo(ctx context.Context, userID string) (UserInfoResponse, error)
}

// RawEvent is the SDK-neutral envelope every event the Client emits. Payload
// holds the original event JSON verbatim; the adapter parses it into a
// port.InboundEvent. Keeping the raw bytes (rather than a typed struct) means
// the SDK can evolve its schema without forcing an interface change.
type RawEvent struct {
	// EventID is the platform-assigned event id. Used as the de-duplication
	// key by the inbound dedup table (T6) — adapters MUST NOT mint their
	// own id here, otherwise SDK replay after a 30s outage will deliver
	// the same logical event twice with different ids and bypass dedup.
	EventID string

	// EventType is the Feishu schema name (e.g. "im.message.receive_v1").
	// The adapter translates it into a typed port.EventType.
	EventType string

	// Payload is the raw event JSON, exactly as the platform delivered it.
	// Stored on InboundEvent.RawPayload so on-call engineers can replay
	// arbitrary event shapes during incident debugging.
	Payload json.RawMessage
}

// SendRequest is the input to Client.SendMessage. Field names mirror the
// Feishu OpenAPI body so concrete clients can marshal directly without an
// extra translation step.
type SendRequest struct {
	// ReceiveIDType is one of "chat_id", "open_id", "union_id", "email",
	// "user_id". The adapter always sets this to "chat_id" for outbound
	// replies (we always know the chat) so downstream platform code does
	// not need to resolve user identifiers.
	ReceiveIDType string
	// ReceiveID is the destination identifier matching ReceiveIDType.
	ReceiveID string
	// MsgType is one of "text", "post", "image", "interactive", … MVP only
	// uses "text"; cards (msg_type="interactive") land in T16.
	MsgType string
	// Content is the JSON-encoded message body — Feishu wraps even plain
	// text in {"text": "..."}. Storing it pre-marshalled keeps the seam
	// platform-shaped (the SDK consumes a JSON string here, not a struct).
	Content string
}

// SendResponse is the output of Client.SendMessage. MessageID is the
// platform-assigned message id, surfaced as port.SendResult.PlatformMessageID
// so reactions / edits can be correlated back to the originating outbound
// log row (T8).
type SendResponse struct {
	MessageID string
}

// ChatInfoResponse and UserInfoResponse are minimum-surface metadata DTOs
// used by GetChatInfo / GetUserInfo. The adapter projects them into the
// platform-neutral port.ChatInfo / port.UserInfo so callers never need to
// know which fields the SDK populated.
type ChatInfoResponse struct {
	ID   string
	Name string
	Type string // "group" | "p2p" — projected to port.ChatType
}

type UserInfoResponse struct {
	OpenID string
	Name   string
}
