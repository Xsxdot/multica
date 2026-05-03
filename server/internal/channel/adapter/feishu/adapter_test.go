package feishu_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/channel/adapter/feishu"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

// fakeFeishuClient is a test double for the WebSocket / OpenAPI client the
// adapter delegates to. It lives in this _test.go file so the SDK seam stays
// invisible to production callers (DESIGN §3.1: SDK details are an
// implementation detail of the adapter, not a public type).
//
// PushEvent simulates an upstream platform delivery; SendCalls records
// outbound POSTs so tests can assert on the exact payload the adapter built.
type fakeFeishuClient struct {
	botUserID string

	mu        sync.Mutex
	sendCalls []sendCall
	sendResp  feishu.SendResponse
	sendErr   error

	events chan feishu.RawEvent
}

type sendCall struct {
	method      string // "im.v1.messages.create"
	receiveID   string
	receiveType string
	msgType     string
	body        string
}

func newFakeFeishuClient(botUserID string) *fakeFeishuClient {
	return &fakeFeishuClient{
		botUserID: botUserID,
		events:    make(chan feishu.RawEvent, 16),
		sendResp: feishu.SendResponse{
			MessageID: "om_test_msg_001",
		},
	}
}

func (f *fakeFeishuClient) BotUserID() string { return f.botUserID }

func (f *fakeFeishuClient) Subscribe() <-chan feishu.RawEvent { return f.events }

// Start is a no-op for the fake; the real client opens a WebSocket here.
func (f *fakeFeishuClient) Start(ctx context.Context) error { return nil }

func (f *fakeFeishuClient) Stop(ctx context.Context) error {
	close(f.events)
	return nil
}

func (f *fakeFeishuClient) SendMessage(ctx context.Context, req feishu.SendRequest) (feishu.SendResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendCalls = append(f.sendCalls, sendCall{
		method:      "im.v1.messages.create",
		receiveID:   req.ReceiveID,
		receiveType: req.ReceiveIDType,
		msgType:     req.MsgType,
		body:        req.Content,
	})
	if f.sendErr != nil {
		return feishu.SendResponse{}, f.sendErr
	}
	return f.sendResp, nil
}

func (f *fakeFeishuClient) GetChatInfo(ctx context.Context, chatID string) (feishu.ChatInfoResponse, error) {
	return feishu.ChatInfoResponse{ID: chatID, Name: "stub"}, nil
}

func (f *fakeFeishuClient) GetUserInfo(ctx context.Context, userID string) (feishu.UserInfoResponse, error) {
	return feishu.UserInfoResponse{OpenID: userID, Name: "stub"}, nil
}

func (f *fakeFeishuClient) push(t *testing.T, ev feishu.RawEvent) {
	t.Helper()
	select {
	case f.events <- ev:
	case <-time.After(time.Second):
		t.Fatal("fakeFeishuClient: events channel blocked for >1s while pushing event")
	}
}

func (f *fakeFeishuClient) snapshotSendCalls() []sendCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]sendCall, len(f.sendCalls))
	copy(out, f.sendCalls)
	return out
}

// TC-adapt-1 — text message normalisation + @Bot mention stripping.
//
// Per Issue STA-7 出口测试 §1: pushing one im.message.receive_v1 event through
// the FakeFeishuClient must surface a port.InboundEvent on Events() with
// Type=EventTypeMessageReceived, Text="hi" (the @_user_<bot> marker fully
// stripped — not just trimmed, the substring must be absent), ChatType=Group,
// and a non-nil RawPayload preserving the original JSON for incident
// debugging.
func TestAdapter_NormalisesTextMessage_AndStripsBotMention(t *testing.T) {
	t.Parallel()

	const botID = "ou_bot_xxx"
	fake := newFakeFeishuClient(botID)

	adapter := feishu.NewAdapter(fake, feishu.Config{AppID: "cli_test"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = adapter.Disconnect(context.Background())
	})

	// im.message.receive_v1 payload mimicking what the Feishu SDK delivers:
	// the user typed "@Bot hi", which the platform represents as "@_user_1 hi"
	// (the literal "@_user_<bot>" placeholder is what arrives over the wire).
	rawJSON := []byte(`{
        "schema": "2.0",
        "header": {
            "event_id": "evt_001",
            "event_type": "im.message.receive_v1",
            "create_time": "1700000000"
        },
        "event": {
            "sender": {
                "sender_id": {"open_id": "ou_user_001"},
                "sender_type": "user"
            },
            "message": {
                "message_id": "om_msg_001",
                "chat_id": "oc_001",
                "chat_type": "group",
                "message_type": "text",
                "create_time": "1700000000",
                "content": "{\"text\":\"@_user_1 hi\"}",
                "mentions": [
                    {"key": "@_user_1", "id": {"open_id": "ou_bot_xxx"}, "name": "Bot"}
                ]
            }
        }
    }`)

	fake.push(t, feishu.RawEvent{
		EventID:   "evt_001",
		EventType: "im.message.receive_v1",
		Payload:   rawJSON,
	})

	select {
	case ev, ok := <-adapter.Events():
		if !ok {
			t.Fatal("Events() channel closed before delivering inbound event")
		}
		if ev.ChannelName != "feishu" {
			t.Errorf("ChannelName = %q, want %q", ev.ChannelName, "feishu")
		}
		if ev.Type != port.EventTypeMessageReceived {
			t.Errorf("Type = %q, want %q", ev.Type, port.EventTypeMessageReceived)
		}
		if ev.EventID != "evt_001" {
			t.Errorf("EventID = %q, want %q", ev.EventID, "evt_001")
		}
		if ev.Text != "hi" {
			t.Errorf("Text = %q, want %q (mention must be stripped)", ev.Text, "hi")
		}
		// Belt-and-braces: even if a future normalisation rule changes the
		// exact whitespace handling, the @_user_xxx marker must NEVER survive
		// into Text — that is the contract downstream intent parsing relies
		// on (Issue STA-7 §关键实现要点).
		if strings.Contains(ev.Text, "@_user_") {
			t.Errorf("Text contains residual @_user_ marker: %q", ev.Text)
		}
		if ev.ChatID != "oc_001" {
			t.Errorf("ChatID = %q, want %q", ev.ChatID, "oc_001")
		}
		if ev.ChatType != port.ChatTypeGroup {
			t.Errorf("ChatType = %q, want %q", ev.ChatType, port.ChatTypeGroup)
		}
		if ev.SenderID != "ou_user_001" {
			t.Errorf("SenderID = %q, want %q", ev.SenderID, "ou_user_001")
		}
		if ev.MessageID != "om_msg_001" {
			t.Errorf("MessageID = %q, want %q", ev.MessageID, "om_msg_001")
		}
		if len(ev.RawPayload) == 0 {
			t.Error("RawPayload is empty; expected the original event JSON for debugging")
		} else {
			// Round-trip the raw payload to confirm it really is valid JSON
			// (i.e. the adapter did not silently truncate it).
			var anyJSON any
			if err := json.Unmarshal(ev.RawPayload, &anyJSON); err != nil {
				t.Errorf("RawPayload is not valid JSON: %v", err)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive inbound event within 2s")
	}
}

// TC-adapt-2 — outbound text reply via Send.
//
// Per Issue STA-7 出口测试 §2: Send(OutboundMessage{ChatID:"oc_001",
// Text:"ok"}) must trigger exactly one im.v1.messages.create call on the
// fake, with receive_id=oc_001, receive_id_type=chat_id, msg_type=text, and
// the body containing the literal text "ok". The returned SendResult must
// expose a non-empty PlatformMessageID and Retryable=false.
func TestAdapter_Send_TextReply(t *testing.T) {
	t.Parallel()

	fake := newFakeFeishuClient("ou_bot_xxx")
	adapter := feishu.NewAdapter(fake, feishu.Config{AppID: "cli_test"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = adapter.Disconnect(context.Background())
	})

	res, err := adapter.Send(ctx, port.OutboundMessage{ChatID: "oc_001", Text: "ok"})
	if err != nil {
		t.Fatalf("Send returned error: %v", err)
	}

	if res.PlatformMessageID == "" {
		t.Error("SendResult.PlatformMessageID is empty; the adapter must surface the platform-assigned id")
	}
	if res.Retryable {
		t.Error("SendResult.Retryable is true on a successful send; expected false")
	}

	calls := fake.snapshotSendCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d SendMessage calls, want 1", len(calls))
	}
	c := calls[0]
	if c.method != "im.v1.messages.create" {
		t.Errorf("method = %q, want %q", c.method, "im.v1.messages.create")
	}
	if c.receiveID != "oc_001" {
		t.Errorf("receive_id = %q, want %q", c.receiveID, "oc_001")
	}
	if c.receiveType != "chat_id" {
		t.Errorf("receive_id_type = %q, want %q", c.receiveType, "chat_id")
	}
	if c.msgType != "text" {
		t.Errorf("msg_type = %q, want %q", c.msgType, "text")
	}
	// Feishu wraps text content in {"text":"..."}; assert we built it
	// rather than passing the bare string (which the platform rejects).
	if !strings.Contains(c.body, `"text"`) || !strings.Contains(c.body, "ok") {
		t.Errorf("content body = %q, want JSON containing \"text\" key and the literal payload \"ok\"", c.body)
	}
}
