package intent

import (
	"fmt"
	"strings"
)

// ChannelAgentUnavailableError carries a safe user-facing message while keeping
// the operational reason available in logs through Error().
type ChannelAgentUnavailableError struct {
	Message string
	Reason  string
}

func (e *ChannelAgentUnavailableError) Error() string {
	if e == nil {
		return "channel agent unavailable"
	}
	msg := strings.TrimSpace(e.Message)
	if msg == "" {
		msg = "channel agent unavailable"
	}
	reason := strings.TrimSpace(e.Reason)
	if reason == "" {
		return msg
	}
	return fmt.Sprintf("%s (%s)", msg, reason)
}

func (e *ChannelAgentUnavailableError) UserMessage() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Message)
}
