package feishu

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/multica-ai/multica/server/internal/channel/port"
)

// normaliseEvent converts a SDK-neutral RawEvent into the platform-neutral
// port.InboundEvent the rest of the channel layer consumes.
//
// The function is intentionally pure (no I/O, no logging) so it is trivial to
// unit test and so a future T6 dedup wrapper can call it without worrying
// about side effects.
//
// Errors:
//   - If EventType is unknown the function returns (zero, false, nil) — an
//     unsupported event is dropped, not surfaced as an error, because the
//     SDK delivers a long tail of event types we explicitly do not handle in
//     M1 (e.g. reaction, recall, button click — see PRD §1.3 non-goals).
//   - If the payload is structurally malformed (invalid JSON, missing
//     required nodes) we return an error so the adapter can record it and
//     keep consuming. Dropping malformed events silently would mask SDK
//     schema drift.
func normaliseEvent(channelName, botUserID string, raw RawEvent) (port.InboundEvent, bool, error) {
	switch raw.EventType {
	case "im.message.receive_v1":
		return normaliseMessageReceive(channelName, botUserID, raw)
	default:
		return port.InboundEvent{}, false, nil
	}
}

// feishuMessageReceive mirrors the (subset of the) im.message.receive_v1
// schema the adapter actually reads. We deliberately decode only the fields
// we use — Feishu adds new optional fields regularly, and a strict full-shape
// struct would make the adapter fragile.
type feishuMessageReceive struct {
	Header struct {
		EventID string `json:"event_id"`
	} `json:"header"`
	Event struct {
		Sender struct {
			SenderID struct {
				OpenID string `json:"open_id"`
			} `json:"sender_id"`
			SenderType string `json:"sender_type"`
		} `json:"sender"`
		Message struct {
			MessageID   string `json:"message_id"`
			ChatID      string `json:"chat_id"`
			ChatType    string `json:"chat_type"`
			MessageType string `json:"message_type"`
			Content     string `json:"content"`
			Mentions    []struct {
				Key  string `json:"key"`
				ID   struct {
					OpenID string `json:"open_id"`
				} `json:"id"`
				Name string `json:"name"`
			} `json:"mentions"`
		} `json:"message"`
	} `json:"event"`
}

// feishuTextContent is the inner content struct Feishu uses for plain-text
// messages: {"text": "..."}. Other message types use different shapes
// (post is structured, image carries an image_key, etc.) — those land in
// future tasks; for MVP we only normalise text.
type feishuTextContent struct {
	Text string `json:"text"`
}

func normaliseMessageReceive(channelName, botUserID string, raw RawEvent) (port.InboundEvent, bool, error) {
	var ev feishuMessageReceive
	if err := json.Unmarshal(raw.Payload, &ev); err != nil {
		return port.InboundEvent{}, false, fmt.Errorf("feishu: decode im.message.receive_v1: %w", err)
	}

	// MVP: only text messages are normalised. Other message types are
	// silently dropped — they belong to future M2/M3 features (e.g. image
	// upload as Issue attachment is a P1 line item).
	if ev.Event.Message.MessageType != "text" {
		return port.InboundEvent{}, false, nil
	}

	var content feishuTextContent
	if err := json.Unmarshal([]byte(ev.Event.Message.Content), &content); err != nil {
		return port.InboundEvent{}, false, fmt.Errorf("feishu: decode text content: %w", err)
	}

	text := stripBotMentions(content.Text, ev.Event.Message.Mentions, botUserID)

	chatType := mapChatType(ev.Event.Message.ChatType)

	// Prefer the header.event_id (canonical platform id) but fall back to
	// the RawEvent.EventID we already captured at the SDK seam — both are
	// the same value in practice; the dual read just makes the adapter
	// robust against a future SDK that fills only one of them.
	eventID := ev.Header.EventID
	if eventID == "" {
		eventID = raw.EventID
	}

	return port.InboundEvent{
		ChannelName: channelName,
		EventID:     eventID,
		Type:        port.EventTypeMessageReceived,
		ChatID:      ev.Event.Message.ChatID,
		ChatType:    chatType,
		SenderID:    ev.Event.Sender.SenderID.OpenID,
		SenderName:  "", // user name resolution happens via GetUserInfo on demand; PRD does not require eager resolution.
		Text:        text,
		MessageID:   ev.Event.Message.MessageID,
		RawPayload:  append(json.RawMessage(nil), raw.Payload...),
	}, true, nil
}

// stripBotMentions removes the literal "@_user_<key>" placeholder Feishu
// inserts whenever the bot is @-mentioned. The Feishu schema delivers the
// mention text inside Content (e.g. "@_user_1 hi") *and* a parallel mentions
// array describing what each placeholder resolves to. We must compare each
// mention's open_id against the bot's own id and remove only those — leaving
// other users' mentions intact so dispatcher can later resolve them.
//
// The leftover whitespace after removal is collapsed (multiple spaces →
// single space) and trimmed at both ends so commonly-typed messages like
// "@Bot   hi" produce "hi" rather than "   hi" or "  hi".
func stripBotMentions(text string, mentions []struct {
	Key  string `json:"key"`
	ID   struct {
		OpenID string `json:"open_id"`
	} `json:"id"`
	Name string `json:"name"`
}, botUserID string) string {
	if botUserID == "" {
		// Defensive: if we don't know who the bot is, removing every
		// "@_user_*" marker would also strip mentions of real users.
		// Better to leave the text alone and let downstream code see the
		// markers (and log a warning at the wire-up site).
		return strings.TrimSpace(text)
	}
	out := text
	for _, m := range mentions {
		if m.ID.OpenID != botUserID {
			continue
		}
		// Defensive: accept either the explicit "key" form Feishu sends
		// (e.g. "@_user_1") or the raw "@_user_<openid>" fallback some
		// older SDK versions used.
		if m.Key != "" {
			out = strings.ReplaceAll(out, m.Key, "")
		}
		out = strings.ReplaceAll(out, "@_user_"+m.ID.OpenID, "")
	}
	out = collapseSpaces(out)
	return strings.TrimSpace(out)
}

// collapseSpaces is a tiny helper kept inline rather than pulling in
// regexp — the adapter is on the hot path of every inbound event and a
// 6-line manual loop is dramatically faster than a regexp compile.
func collapseSpaces(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' {
			if prevSpace {
				continue
			}
			prevSpace = true
			b.WriteRune(' ')
			continue
		}
		prevSpace = false
		b.WriteRune(r)
	}
	return b.String()
}

func mapChatType(s string) port.ChatType {
	switch s {
	case "group":
		return port.ChatTypeGroup
	case "p2p":
		return port.ChatTypeDirect
	default:
		// Unknown chat types fall back to group — strictly safer than
		// direct (group rules in PRD §F7 require workspace membership;
		// direct opens the binding flow). If a future SDK ships a new
		// chat_type we'd rather over-restrict than over-permit.
		return port.ChatTypeGroup
	}
}
