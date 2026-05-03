package port

import "context"

// InboundEvent represents an event received from an external channel (e.g. Slack,
// Discord, Telegram). It is the common envelope consumed by the channel layer.
type InboundEvent struct {
	// Type identifies the event category (e.g. "message", "reaction", "join").
	Type string
	// Payload carries the raw event data.
	Payload any
}

// ChatInfo holds metadata about a chat room / channel / thread.
type ChatInfo struct {
	ID   string
	Name string
}

// UserInfo holds metadata about a user on the external channel.
type UserInfo struct {
	ID       string
	Name     string
	Username string
}

// OutboundMessage is a plain text message to be sent to the external channel.
type OutboundMessage struct {
	ChatID  string
	Content string
}

// OutboundCardMessage is a structured (rich) message to be sent to the external
// channel. Adapters that do not support cards may fall back to text.
type OutboundCardMessage struct {
	ChatID  string
	Title   string
	Content string
}

// SendResult carries the outcome of a Send or SendCard call.
type SendResult struct {
	MessageID string
}

// Channel is the abstraction over an external messaging platform. Each adapter
// (Slack, Discord, Telegram, …) implements this interface so the rest of the
// server can treat channels uniformly.
type Channel interface {
	// Name returns the human-readable channel identifier (e.g. "slack").
	Name() string

	// Connect establishes the connection to the external platform.
	Connect(ctx context.Context) error

	// Events returns a receive-only channel of inbound events. The channel must
	// be closed after Disconnect returns so that downstream consumers terminate
	// cleanly.
	Events() <-chan InboundEvent

	// GetChatInfo fetches metadata for a chat room.
	GetChatInfo(ctx context.Context, chatID string) (ChatInfo, error)

	// GetUserInfo fetches metadata for a user.
	GetUserInfo(ctx context.Context, userID string) (UserInfo, error)

	// Send delivers a plain text message.
	Send(ctx context.Context, msg OutboundMessage) (SendResult, error)

	// SendCard delivers a structured / rich message.
	SendCard(ctx context.Context, msg OutboundCardMessage) (SendResult, error)

	// Disconnect tears down the connection. After Disconnect returns, the
	// channel returned by Events() must be closed.
	Disconnect(ctx context.Context) error
}
