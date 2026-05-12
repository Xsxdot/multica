package inbound

import (
	"context"
	"testing"
	"time"

	chintent "github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

func TestConversationKey_GroupIncludesSender(t *testing.T) {
	a := port.InboundEvent{ChannelName: "feishu", ChatType: port.ChatTypeGroup, ChatID: "oc_1", SenderID: "ou_a"}
	b := port.InboundEvent{ChannelName: "feishu", ChatType: port.ChatTypeGroup, ChatID: "oc_1", SenderID: "ou_b"}
	if ConversationKey(a) == ConversationKey(b) {
		t.Fatalf("group conversation key must include sender: %q", ConversationKey(a))
	}
}

func TestConversationKey_DirectIgnoresChatID(t *testing.T) {
	a := port.InboundEvent{ChannelName: "feishu", ChatType: port.ChatTypeDirect, ChatID: "oc_1", SenderID: "ou_a"}
	b := port.InboundEvent{ChannelName: "feishu", ChatType: port.ChatTypeDirect, ChatID: "oc_2", SenderID: "ou_a"}
	if ConversationKey(a) != ConversationKey(b) {
		t.Fatalf("direct conversation key should be user-scoped: %q != %q", ConversationKey(a), ConversationKey(b))
	}
}

func TestRuntimeAccept_UserACKs(t *testing.T) {
	cases := []struct {
		name string
		res  AcceptResult
		want string
	}{
		{
			name: "starts immediately",
			res:  AcceptResult{EventID: "row-1", Accepted: true},
			want: "好的，开始处理。",
		},
		{
			name: "queued behind existing work",
			res:  AcceptResult{EventID: "row-1", Accepted: true, QueueDepth: 2},
			want: "已收到，前面还有 2 条，我会按顺序处理。",
		},
		{
			name: "backpressure",
			res:  AcceptResult{EventID: "row-1", RejectedBackpressure: true, QueueDepth: 3},
			want: "我现在忙不过来了，当前会话还有 3 条在排队，请稍后再发。",
		},
		{
			name: "duplicate has no ack",
			res:  AcceptResult{EventID: "row-1", Duplicate: true},
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeRuntimeStore{accept: tc.res}
			sink := &recordingReplySink{}
			rt := NewRuntime(RuntimeConfig{Store: store, ReplySink: sink})
			_, err := rt.Accept(context.Background(), port.InboundEvent{
				ChannelName: "feishu",
				EventID:     "evt-1",
				ChatID:      "oc_1",
				ChatType:    port.ChatTypeGroup,
				SenderID:    "ou_1",
			}, AcceptOptions{ConversationLimit: 3})
			if err != nil {
				t.Fatalf("Accept: %v", err)
			}
			if got := sink.last(); got != tc.want {
				t.Fatalf("ack = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRuntimeProcessRecord_RuleIntentDoesNotWaitForAgent(t *testing.T) {
	store := &fakeRuntimeStore{}
	post := NewPipeline(fnStep{
		name: "post",
		run: func(_ context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
			if evt.Intent.Kind != port.IntentCreateIssue {
				t.Fatalf("intent = %q, want CreateIssue", evt.Intent.Kind)
			}
			return evt, DecisionContinue, nil
		},
	})
	rt := NewRuntime(RuntimeConfig{
		Store: store,
		RuleResolvers: []chintent.IntentResolver{fakeResolver{
			result: chintent.IntentResult{
				Matched: true,
				Intent: chintent.Intent{
					Kind:       chintent.IntentCreateIssue,
					Confidence: 1,
					Params:     map[string]string{"title": "from channel"},
					Source:     chintent.SourceRule,
				},
			},
		}},
		PostPipeline: post,
	})

	err := rt.processRecord(context.Background(), &InboundEventRecord{
		ID:    "row-1",
		Phase: InboundPhaseIntent,
		Event: port.InboundEvent{
			ChannelName: "feishu",
			EventID:     "evt-1",
			Type:        port.EventTypeMessageReceived,
			ChatID:      "oc_1",
			ChatType:    port.ChatTypeGroup,
			SenderID:    "ou_1",
			Text:        "create issue",
		},
	})
	if err != nil {
		t.Fatalf("processRecord: %v", err)
	}
	if store.waitingAgent {
		t.Fatal("rule intent should not enter waiting_agent")
	}
	if !store.processed {
		t.Fatal("post pipeline completion should mark event processed")
	}
}

func TestRuntimeApplyIntentResult_WaitingUserSetsExpiry(t *testing.T) {
	store := &fakeRuntimeStore{}
	sink := &recordingReplySink{}
	rt := NewRuntime(RuntimeConfig{
		Store:                store,
		ReplySink:            sink,
		ClarificationTimeout: 5 * time.Minute,
	})
	rec := &InboundEventRecord{
		ID:    "row-1",
		Phase: InboundPhaseIntent,
		Event: port.InboundEvent{ChannelName: "feishu", EventID: "evt-1", ChatID: "oc_1", SenderID: "ou_1"},
	}
	before := time.Now().Add(4 * time.Minute)
	waiting, err := rt.applyIntentResult(context.Background(), rec, chintent.IntentResult{
		Matched: true,
		Intent: chintent.Intent{
			Kind:   chintent.IntentASKClarify,
			Params: map[string]string{},
			Source: chintent.SourceChat,
		},
	}, ChatBindingContext{}, false)
	if err != nil {
		t.Fatalf("applyIntentResult: %v", err)
	}
	if !waiting {
		t.Fatal("ASKClarify should enter waiting_user")
	}
	if !store.waitingUserExpiresAt.After(before) {
		t.Fatalf("waiting_user expires_at = %s, want after %s", store.waitingUserExpiresAt, before)
	}
	if sink.last() == "" {
		t.Fatal("clarification reply was not sent")
	}
}

func TestRuntimeWorker_DeadRetryNotifiesUser(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeRuntimeStore{
		claim: InboundEventRecord{
			ID:    "row-1",
			Phase: InboundPhasePost,
			Event: port.InboundEvent{ChannelName: "feishu", EventID: "evt-1", ChatID: "oc_1", SenderID: "ou_1"},
		},
		retry:   RetryResult{Dead: true},
		onRetry: cancel,
	}
	sink := &recordingReplySink{}
	rt := NewRuntime(RuntimeConfig{
		Store:     store,
		ReplySink: sink,
		PostPipeline: NewPipeline(fnStep{
			name: "post",
			run: func(context.Context, port.InboundEvent) (port.InboundEvent, Decision, error) {
				return port.InboundEvent{}, DecisionContinue, context.DeadlineExceeded
			},
		}),
	})

	rt.workerLoop(ctx, "worker-1")
	if got := sink.last(); got != "处理失败了，这条消息我先停止处理，请稍后重试。" {
		t.Fatalf("dead retry reply = %q", got)
	}
}

type fakeRuntimeStore struct {
	accept               AcceptResult
	claim                InboundEventRecord
	claimed              bool
	waitingAgent         bool
	waitingUserExpiresAt time.Time
	processed            bool
	retry                RetryResult
	onRetry              func()
}

func (s *fakeRuntimeStore) AcceptEvent(context.Context, port.InboundEvent, AcceptOptions) (AcceptResult, error) {
	return s.accept, nil
}

func (s *fakeRuntimeStore) Load(context.Context, string) (*InboundEventRecord, error) {
	return nil, nil
}

func (s *fakeRuntimeStore) ClaimNext(context.Context, string) (*InboundEventRecord, error) {
	if s.claim.ID != "" && !s.claimed {
		s.claimed = true
		rec := s.claim
		return &rec, nil
	}
	return nil, nil
}

func (s *fakeRuntimeStore) SaveEvent(_ context.Context, _ string, _ port.InboundEvent, _ string, _ ChatBindingContext) error {
	return nil
}

func (s *fakeRuntimeStore) MarkQueued(context.Context, string, port.InboundEvent, string, ChatBindingContext) error {
	return nil
}

func (s *fakeRuntimeStore) MarkWaitingAgent(context.Context, string, port.InboundEvent, string, ChatBindingContext) error {
	s.waitingAgent = true
	return nil
}

func (s *fakeRuntimeStore) MarkWaitingUser(_ context.Context, _ string, _ port.InboundEvent, _ string, _ ChatBindingContext, expiresAt time.Time) error {
	s.waitingUserExpiresAt = expiresAt
	return nil
}

func (s *fakeRuntimeStore) MarkProcessed(context.Context, string) error {
	s.processed = true
	return nil
}

func (s *fakeRuntimeStore) MarkRetry(context.Context, string, error) (RetryResult, error) {
	if s.onRetry != nil {
		s.onRetry()
	}
	return s.retry, nil
}

func (s *fakeRuntimeStore) MarkDead(context.Context, string, error) error {
	return nil
}

func (s *fakeRuntimeStore) ListWaitingAgent(context.Context, int) ([]WaitingAgentEvent, error) {
	return nil, nil
}

func (s *fakeRuntimeStore) LookupChatContext(context.Context, string, string) (ChatBindingContext, error) {
	return ChatBindingContext{}, nil
}

func (s *fakeRuntimeStore) RequeueStaleProcessing(context.Context, time.Duration) (int64, error) {
	return 0, nil
}

func (s *fakeRuntimeStore) ExpireWaitingUser(context.Context, int) ([]ExpiredWaitingUserEvent, error) {
	return nil, nil
}

type recordingReplySink struct {
	replies []string
}

func (s *recordingReplySink) SendText(_ context.Context, _ port.InboundEvent, msg port.OutboundMessage) error {
	s.replies = append(s.replies, msg.Text)
	return nil
}

func (s *recordingReplySink) SendRich(_ context.Context, _ port.InboundEvent, msg port.OutboundRichMessage) error {
	s.replies = append(s.replies, msg.Body)
	return nil
}

func (s *recordingReplySink) last() string {
	if len(s.replies) == 0 {
		return ""
	}
	return s.replies[len(s.replies)-1]
}

type fakeResolver struct {
	result chintent.IntentResult
	err    error
}

func (r fakeResolver) Name() string { return "fake" }

func (r fakeResolver) Resolve(context.Context, chintent.IntentRequest) (chintent.IntentResult, error) {
	return r.result, r.err
}

type fnStep struct {
	name string
	run  func(context.Context, port.InboundEvent) (port.InboundEvent, Decision, error)
}

func (s fnStep) Name() string { return s.name }

func (s fnStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
	return s.run(ctx, evt)
}
