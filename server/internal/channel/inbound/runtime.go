package inbound

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	chintent "github.com/multica-ai/multica/server/internal/channel/intent"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

const (
	defaultInboundWorkers              = 16
	defaultInboundClaimBatch           = 32
	defaultInboundPollInterval         = 250 * time.Millisecond
	defaultInboundIntentTaskTimeout    = 15 * time.Minute
	defaultInboundActionTaskTimeout    = 30 * time.Minute
	defaultInboundClarificationTimeout = 30 * time.Minute
	defaultInboundProcessingLease      = 5 * time.Minute
)

type RuntimeConfig struct {
	Store                InboundEventStore
	PrePipeline          *Pipeline
	PostPipeline         *Pipeline
	RuleResolvers        []chintent.IntentResolver
	ChatIntent           chintent.AsyncChatIntentClient
	ReplySink            ChannelReplySink
	Workers              int
	ClaimBatch           int
	PollInterval         time.Duration
	IntentTaskTimeout    time.Duration
	ActionTaskTimeout    time.Duration
	ClarificationTimeout time.Duration
	ProcessingLease      time.Duration
}

type Runtime struct {
	cfg RuntimeConfig
}

func NewRuntime(cfg RuntimeConfig) *Runtime {
	if cfg.Workers <= 0 {
		cfg.Workers = defaultInboundWorkers
	}
	if cfg.ClaimBatch <= 0 {
		cfg.ClaimBatch = defaultInboundClaimBatch
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultInboundPollInterval
	}
	if cfg.IntentTaskTimeout <= 0 {
		cfg.IntentTaskTimeout = defaultInboundIntentTaskTimeout
	}
	if cfg.ActionTaskTimeout <= 0 {
		cfg.ActionTaskTimeout = defaultInboundActionTaskTimeout
	}
	if cfg.ClarificationTimeout <= 0 {
		cfg.ClarificationTimeout = defaultInboundClarificationTimeout
	}
	if cfg.ProcessingLease <= 0 {
		cfg.ProcessingLease = defaultInboundProcessingLease
	}
	return &Runtime{cfg: cfg}
}

func (r *Runtime) Run(ctx context.Context) {
	if r == nil || r.cfg.Store == nil {
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Workers; i++ {
		workerID := fmt.Sprintf("channel-inbound-%d", i+1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.workerLoop(ctx, workerID)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.resumeLoop(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.sweeperLoop(ctx)
	}()
	<-ctx.Done()
	wg.Wait()
}

func (r *Runtime) Accept(ctx context.Context, evt port.InboundEvent, opts AcceptOptions) (AcceptResult, error) {
	if opts.BypassLimit || ControlMessageBypassesBackpressure(evt) {
		opts.BypassLimit = true
	}
	result, err := r.cfg.Store.AcceptEvent(ctx, evt, opts)
	if err != nil {
		return result, err
	}
	if result.Duplicate {
		return result, nil
	}
	switch {
	case result.RejectedBackpressure:
		r.send(ctx, evt, fmt.Sprintf("我现在忙不过来了，当前会话还有 %d 条在排队，请稍后再发。", result.QueueDepth))
	case result.ClarificationConsumed:
		r.send(ctx, evt, "收到补充，继续处理。")
	case result.Accepted && result.QueueDepth == 0:
		r.send(ctx, evt, "好的，开始处理。")
	case result.Accepted:
		r.send(ctx, evt, fmt.Sprintf("已收到，前面还有 %d 条，我会按顺序处理。", result.QueueDepth))
	}
	return result, nil
}

func (r *Runtime) workerLoop(ctx context.Context, workerID string) {
	for {
		if ctx.Err() != nil {
			return
		}
		rec, err := r.cfg.Store.ClaimNext(ctx, workerID)
		if err != nil {
			slog.Error("channel inbound runtime: claim failed", "worker", workerID, "error", err)
			sleepWithContext(ctx, r.cfg.PollInterval)
			continue
		}
		if rec == nil {
			sleepWithContext(ctx, r.cfg.PollInterval)
			continue
		}
		if err := r.processRecord(ctx, rec); err != nil {
			slog.Error("channel inbound runtime: process failed",
				"event_row_id", rec.ID,
				"event_id", rec.Event.EventID,
				"phase", rec.Phase,
				"error", err,
			)
			result, markErr := r.cfg.Store.MarkRetry(ctx, rec.ID, err)
			if markErr != nil {
				slog.Error("channel inbound runtime: mark retry failed", "event_row_id", rec.ID, "error", markErr)
			} else if result.Dead {
				r.send(ctx, rec.Event, "处理失败了，这条消息我先停止处理，请稍后重试。")
			}
		}
	}
}

func (r *Runtime) processRecord(ctx context.Context, rec *InboundEventRecord) error {
	for {
		switch rec.Phase {
		case InboundPhasePre:
			next, outcome, err := r.runPipeline(ctx, r.cfg.PrePipeline, rec.Event)
			if err != nil {
				return err
			}
			if outcome.Decision == DecisionSkip {
				return r.cfg.Store.MarkProcessed(ctx, rec.ID)
			}
			chatCtx := r.lookupChatContext(ctx, next)
			if err := r.cfg.Store.SaveEvent(ctx, rec.ID, next, InboundPhaseIntent, chatCtx); err != nil {
				return err
			}
			rec.Event = next
			rec.Phase = InboundPhaseIntent
			rec.WorkspaceID = chatCtx.WorkspaceID
			rec.DefaultProjectID = chatCtx.DefaultProjectID

		case InboundPhaseIntent:
			waiting, err := r.resolveIntent(ctx, rec)
			if err != nil || waiting {
				return err
			}

		case InboundPhasePost:
			_, outcome, err := r.runPipeline(ctx, r.cfg.PostPipeline, rec.Event)
			if err != nil {
				return err
			}
			if outcome.Decision == DecisionSkip || outcome.Decision == DecisionContinue {
				return r.cfg.Store.MarkProcessed(ctx, rec.ID)
			}

		case InboundPhaseDone:
			return r.cfg.Store.MarkProcessed(ctx, rec.ID)

		default:
			return fmt.Errorf("unknown inbound phase %q", rec.Phase)
		}
	}
}

func (r *Runtime) resolveIntent(ctx context.Context, rec *InboundEventRecord) (bool, error) {
	evt := rec.Event
	chatCtx := ChatBindingContext{WorkspaceID: rec.WorkspaceID, DefaultProjectID: rec.DefaultProjectID}
	if chatCtx.WorkspaceID == "" {
		chatCtx = r.lookupChatContext(ctx, evt)
	}
	if evt.Type != port.EventTypeMessageReceived {
		if err := r.cfg.Store.SaveEvent(ctx, rec.ID, evt, InboundPhasePost, chatCtx); err != nil {
			return false, err
		}
		rec.Phase = InboundPhasePost
		return false, nil
	}

	req := chintent.IntentRequest{
		WorkspaceID:      chatCtx.WorkspaceID,
		DefaultProjectID: chatCtx.DefaultProjectID,
		Text:             evt.Text,
		Channel:          evt.ChannelName,
		ConnectionID:     evt.ConnectionID(),
		ChatID:           evt.ChatID,
		ChatType:         string(evt.ChatType),
		SenderID:         evt.SenderID,
		SenderName:       evt.SenderName,
		InboundEventID:   rec.ID,
		SourceHint:       chintent.IntentSource(evt.Intent.Source),
	}

	for _, resolver := range r.cfg.RuleResolvers {
		if resolver == nil {
			continue
		}
		result, err := resolver.Resolve(ctx, req)
		if err != nil {
			return false, err
		}
		if !result.Matched {
			continue
		}
		return r.applyIntentResult(ctx, rec, result, chatCtx, false)
	}

	if r.cfg.ChatIntent == nil || chatCtx.WorkspaceID == "" {
		result := chintent.IntentResult{
			Matched: true,
			Intent: chintent.Intent{
				Kind:       chintent.IntentUnknown,
				Confidence: 0,
				Params:     map[string]string{},
				Source:     chintent.SourceRule,
			},
		}
		return r.applyIntentResult(ctx, rec, result, chatCtx, false)
	}

	taskID, err := r.cfg.ChatIntent.StartIntent(ctx, req)
	if err != nil {
		return false, err
	}
	if err := r.cfg.Store.MarkWaitingAgent(ctx, rec.ID, evt, taskID, chatCtx); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Runtime) applyIntentResult(ctx context.Context, rec *InboundEventRecord, result chintent.IntentResult, chatCtx ChatBindingContext, requeue bool) (bool, error) {
	evt := rec.Event
	evt.Intent = toPortIntent(result.Intent)
	applyDefaultProject(&evt, chatCtx)
	if evt.Intent.Kind == port.IntentASKClarify {
		reply := "[ASK_CLARIFY] 我还不确定你要我做什么，请补充 Issue 标题、编号或要执行的动作。"
		if err := r.cfg.Store.MarkWaitingUser(ctx, rec.ID, evt, reply, chatCtx, time.Now().Add(r.cfg.ClarificationTimeout)); err != nil {
			return false, err
		}
		r.send(ctx, evt, reply)
		return true, nil
	}
	if requeue {
		if err := r.cfg.Store.MarkQueued(ctx, rec.ID, evt, InboundPhasePost, chatCtx); err != nil {
			return false, err
		}
	} else {
		if err := r.cfg.Store.SaveEvent(ctx, rec.ID, evt, InboundPhasePost, chatCtx); err != nil {
			return false, err
		}
	}
	rec.Event = evt
	rec.Phase = InboundPhasePost
	return false, nil
}

func (r *Runtime) resumeLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.resumeWaitingAgents(ctx)
		}
	}
}

func (r *Runtime) resumeWaitingAgents(ctx context.Context) {
	items, err := r.cfg.Store.ListWaitingAgent(ctx, r.cfg.ClaimBatch)
	if err != nil {
		slog.Error("channel inbound runtime: list waiting agents failed", "error", err)
		return
	}
	for _, item := range items {
		if item.WaitTaskID == "" {
			_ = r.cfg.Store.MarkDead(ctx, item.ID, errors.New("waiting agent event has no task id"))
			continue
		}
		timeout := r.cfg.IntentTaskTimeout
		if item.WaitKind == WaitKindAction {
			timeout = r.cfg.ActionTaskTimeout
		}
		if time.Since(item.UpdatedAt) > timeout {
			rec, _ := r.cfg.Store.Load(ctx, item.ID)
			err := fmt.Errorf("channel %s task timed out after %s", item.WaitKind, timeout)
			if markErr := r.cfg.Store.MarkDead(ctx, item.ID, err); markErr != nil {
				slog.Error("channel inbound runtime: mark timed-out event dead failed", "event_row_id", item.ID, "error", markErr)
			}
			if rec != nil {
				r.send(ctx, rec.Event, "处理超时了，这条消息我先停止处理，请稍后重试。")
			}
			continue
		}
		if item.WaitKind != WaitKindIntent {
			continue
		}
		if r.cfg.ChatIntent == nil {
			continue
		}
		result, done, err := r.cfg.ChatIntent.ParseIntentResult(ctx, item.WaitTaskID)
		if !done {
			continue
		}
		rec, loadErr := r.cfg.Store.Load(ctx, item.ID)
		if loadErr != nil {
			slog.Error("channel inbound runtime: load waiting event failed", "event_row_id", item.ID, "error", loadErr)
			continue
		}
		if err != nil {
			if markErr := r.cfg.Store.MarkDead(ctx, item.ID, err); markErr != nil {
				slog.Error("channel inbound runtime: mark failed intent dead failed", "event_row_id", item.ID, "error", markErr)
			}
			r.send(ctx, rec.Event, "语义理解失败了，这条消息我先停止处理，请稍后重试。")
			continue
		}
		chatCtx := ChatBindingContext{WorkspaceID: rec.WorkspaceID, DefaultProjectID: rec.DefaultProjectID}
		if chatCtx.WorkspaceID == "" {
			chatCtx = r.lookupChatContext(ctx, rec.Event)
		}
		if _, err := r.applyIntentResult(ctx, rec, result, chatCtx, true); err != nil {
			slog.Error("channel inbound runtime: resume intent failed", "event_row_id", item.ID, "error", err)
		}
	}
}

func (r *Runtime) sweeperLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := r.cfg.Store.RequeueStaleProcessing(ctx, r.cfg.ProcessingLease)
			if err != nil {
				slog.Error("channel inbound runtime: requeue stale processing failed", "error", err)
			} else if n > 0 {
				slog.Warn("channel inbound runtime: requeued stale processing events", "count", n)
			}
			expired, err := r.cfg.Store.ExpireWaitingUser(ctx, r.cfg.ClaimBatch)
			if err != nil {
				slog.Error("channel inbound runtime: expire waiting_user failed", "error", err)
			}
			for _, item := range expired {
				r.send(ctx, item.Event, "长时间没有补充信息，已停止处理，请重新发送完整需求。")
			}
		}
	}
}

func (r *Runtime) runPipeline(ctx context.Context, pipeline *Pipeline, evt port.InboundEvent) (port.InboundEvent, Outcome, error) {
	if pipeline == nil {
		return evt, Outcome{Decision: DecisionContinue}, nil
	}
	return pipeline.RunEvent(ctx, evt)
}

func (r *Runtime) lookupChatContext(ctx context.Context, evt port.InboundEvent) ChatBindingContext {
	chatCtx, err := r.cfg.Store.LookupChatContext(ctx, evt.ConnectionID(), evt.ChatID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		slog.Error("channel inbound runtime: lookup chat context failed",
			"channel", evt.ChannelName,
			"chat_id", evt.ChatID,
			"error", err,
		)
	}
	return chatCtx
}

func (r *Runtime) send(ctx context.Context, evt port.InboundEvent, text string) {
	if r.cfg.ReplySink == nil || text == "" {
		return
	}
	if err := r.cfg.ReplySink.Send(ctx, evt, text); err != nil {
		slog.Error("channel inbound runtime: send reply failed",
			"channel", evt.ChannelName,
			"chat_id", evt.ChatID,
			"event_id", evt.EventID,
			"error", err,
		)
	}
}

func applyDefaultProject(evt *port.InboundEvent, chatCtx ChatBindingContext) {
	if evt == nil || evt.Intent.Kind != port.IntentCreateIssue || chatCtx.DefaultProjectID == "" {
		return
	}
	if evt.Intent.Params == nil {
		evt.Intent.Params = map[string]string{}
	}
	if evt.Intent.Params["project_id"] == "" {
		evt.Intent.Params["project_id"] = chatCtx.DefaultProjectID
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) {
	if d <= 0 {
		d = defaultInboundPollInterval
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
