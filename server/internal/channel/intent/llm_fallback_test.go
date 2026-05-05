package intent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// CompositeClassifier: rule-first, LLM-fallback with quota gate.
//
// This is the orchestration layer that wires RuleMatcher + QuotaLimiter +
// LLMClassifier into a single IntentClassifier.  It is deliberately thin:
// no business logic, just the precedence rule (rule → quota → llm) and
// the fallback on error / exhaustion.
// ---------------------------------------------------------------------------

// CompositeClassifier implements IntentClassifier with rule-first,
// LLM-fallback behaviour.
type CompositeClassifier struct {
	ruleMatcher RuleMatcher
	quota       QuotaLimiter
	llm         *LLMClassifier
	workspaceID string
}

// NewCompositeClassifier creates a composite classifier.
func NewCompositeClassifier(rule RuleMatcher, quota QuotaLimiter, llm *LLMClassifier, workspaceID string) *CompositeClassifier {
	return &CompositeClassifier{
		ruleMatcher: rule,
		quota:       quota,
		llm:         llm,
		workspaceID: workspaceID,
	}
}

// Classify tries rule matcher first, then LLM if quota allows.
// On LLM error or quota exhaustion it falls back to the rule matcher result
// (which may be Unknown).
func (c *CompositeClassifier) Classify(ctx context.Context, text string) (Intent, error) {
	// 1. Try rule matcher first (zero-token, deterministic).
	if intent, ok := c.ruleMatcher.Match(text); ok {
		return intent, nil
	}

	// 2. No rule hit — try LLM if quota allows.
	if c.quota != nil {
		allowed, err := c.quota.AllowCtx(ctx, c.workspaceID)
		if err != nil {
			return Intent{}, err
		}
		if !allowed {
			// Quota exhausted: return Unknown so upstream can decide.
			return Intent{Kind: IntentUnknown, Source: SourceRule, Params: map[string]string{}}, nil
		}
	}

	// 3. LLM call.
	intent, err := c.llm.Classify(ctx, text)
	if err != nil {
		// LLM failure: fall back to Unknown (caller may retry or escalate).
		return Intent{Kind: IntentUnknown, Source: SourceRule, Params: map[string]string{}}, nil
	}
	return intent, nil
}

// ---------------------------------------------------------------------------
// TC-risk-1 (AC5.1): quota exhausted → composite falls back to rule result.
//
// We use a text that does NOT match any rule (so the composite reaches the
// LLM path), set quota limit=1, exhaust it with one call, then on the
// second call assert the composite returns IntentUnknown with SourceRule
// and makes zero LLM requests.
// ---------------------------------------------------------------------------

func TestComposite_QuotaExhausted_FallsBackToRule(t *testing.T) {
	t.Parallel()

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		resp := LLMResponse{Intent: "CreateIssue", Confidence: 0.9, Params: map[string]string{"title": "x"}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ChatCompletionResponse{
			Choices: []Choice{{Message: Message{Content: mustMarshal(resp)}}},
			Usage:   Usage{TotalTokens: 50},
		})
	}))
	defer srv.Close()

	quota := NewSlidingWindowQuota(QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       1,
		Window:      time.Hour,
	})

	llm := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 1000,
	})

	composite := NewCompositeClassifier(NewRuleMatcher(), quota, llm, "ws-1")
	ctx := context.Background()

	// Use text that does NOT match any rule so composite reaches LLM path.
	nonRuleText := "this text does not match any rule"

	// First call: quota allows, LLM is called.
	intent1, err := composite.Classify(ctx, nonRuleText)
	if err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}
	if intent1.Kind != IntentCreateIssue {
		t.Errorf("first call: Kind = %q, want CreateIssue", intent1.Kind)
	}
	if intent1.Source != SourceLLM {
		t.Errorf("first call: Source = %q, want LLM", intent1.Source)
	}
	if callCount != 1 {
		t.Fatalf("first call: expected 1 LLM call, got %d", callCount)
	}

	// Second call: quota exhausted → fallback to rule result (Unknown).
	intent2, err := composite.Classify(ctx, nonRuleText)
	if err != nil {
		t.Fatalf("second call: unexpected error: %v", err)
	}
	if intent2.Kind != IntentUnknown {
		t.Errorf("second call: Kind = %q, want Unknown", intent2.Kind)
	}
	if intent2.Source != SourceRule {
		t.Errorf("second call: Source = %q, want Rule", intent2.Source)
	}
	if callCount != 1 {
		t.Fatalf("second call: expected no additional LLM calls, got %d", callCount)
	}
}

// ---------------------------------------------------------------------------
// TC-risk-1b: quota exhausted with a rule-matching text → rule result
// is returned directly, LLM is never consulted.
// ---------------------------------------------------------------------------

func TestComposite_QuotaExhausted_RuleHit_BypassesLLM(t *testing.T) {
	t.Parallel()

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		resp := LLMResponse{Intent: "CreateIssue", Confidence: 0.9, Params: map[string]string{"title": "x"}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ChatCompletionResponse{
			Choices: []Choice{{Message: Message{Content: mustMarshal(resp)}}},
			Usage:   Usage{TotalTokens: 50},
		})
	}))
	defer srv.Close()

	quota := NewSlidingWindowQuota(QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       0, // exhausted from the start
		Window:      time.Hour,
	})

	llm := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 1000,
	})

	composite := NewCompositeClassifier(NewRuleMatcher(), quota, llm, "ws-1")
	ctx := context.Background()

	// Text that matches a rule — should return rule result even with quota=0.
	intent, err := composite.Classify(ctx, "帮我记一个 登录页白屏")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if intent.Kind != IntentCreateIssue {
		t.Errorf("Kind = %q, want CreateIssue", intent.Kind)
	}
	if intent.Source != SourceRule {
		t.Errorf("Source = %q, want Rule", intent.Source)
	}
	if callCount != 0 {
		t.Fatalf("expected 0 LLM calls (rule hit bypasses LLM), got %d", callCount)
	}
}

// ---------------------------------------------------------------------------
// TC-risk-2 (AC5.3): token quota cap ≤ 1k → input truncated to exactly
// MaxTokens*4 runes.
// ---------------------------------------------------------------------------

func TestComposite_TokenCap_TruncatesInput(t *testing.T) {
	t.Parallel()

	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ChatCompletionRequest
		json.NewDecoder(r.Body).Decode(&req)
		if len(req.Messages) >= 2 {
			receivedBody = req.Messages[1].Content // user message
		}
		resp := LLMResponse{Intent: "Unknown", Confidence: 0.5, Params: map[string]string{}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ChatCompletionResponse{
			Choices: []Choice{{Message: Message{Content: mustMarshal(resp)}}},
			Usage:   Usage{TotalTokens: 10},
		})
	}))
	defer srv.Close()

	llm := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 10, // budget → maxChars = 40
	})

	composite := NewCompositeClassifier(NewRuleMatcher(), nil, llm, "ws-1")

	longText := strings.Repeat("这是一个很长的输入文本，用于测试截断功能。", 10) // > 40 chars
	_, err := composite.Classify(context.Background(), longText)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}

	receivedRunes := []rune(receivedBody)
	if len(receivedRunes) != 40 {
		t.Fatalf("expected exactly 40 runes (MaxTokens*4), got %d", len(receivedRunes))
	}
}

// ---------------------------------------------------------------------------
// TC-risk-3 (PRD E7): LLM unavailable (HTTP 503) → composite falls back
// to rule result (Unknown for non-matching text).
// ---------------------------------------------------------------------------

func TestComposite_LLMUnavailable_FallsBackToRule(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error": "overloaded"}`))
	}))
	defer srv.Close()

	llm := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 1000,
	})

	composite := NewCompositeClassifier(NewRuleMatcher(), nil, llm, "ws-1")

	// Non-matching text → reaches LLM → 503 → fallback to Unknown.
	intent, err := composite.Classify(context.Background(), "this text does not match any rule")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if intent.Kind != IntentUnknown {
		t.Errorf("Kind = %q, want Unknown", intent.Kind)
	}
	if intent.Source != SourceRule {
		t.Errorf("Source = %q, want Rule", intent.Source)
	}
}

// ---------------------------------------------------------------------------
// TC-risk-3b: LLM network timeout → composite falls back to rule result.
// ---------------------------------------------------------------------------

func TestComposite_LLMTimeout_FallsBackToRule(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	llm := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 1000,
		Timeout:   50 * time.Millisecond,
	})

	composite := NewCompositeClassifier(NewRuleMatcher(), nil, llm, "ws-1")

	intent, err := composite.Classify(context.Background(), "this text does not match any rule")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if intent.Kind != IntentUnknown {
		t.Errorf("Kind = %q, want Unknown", intent.Kind)
	}
	if intent.Source != SourceRule {
		t.Errorf("Source = %q, want Rule", intent.Source)
	}
}

// ---------------------------------------------------------------------------
// TC-risk-3c: LLM returns invalid JSON → composite falls back to rule.
// ---------------------------------------------------------------------------

func TestComposite_LLMInvalidJSON_FallsBackToRule(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ChatCompletionResponse{
			Choices: []Choice{{Message: Message{Content: "not valid json"}}},
			Usage:   Usage{TotalTokens: 100},
		})
	}))
	defer srv.Close()

	llm := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 1000,
	})

	composite := NewCompositeClassifier(NewRuleMatcher(), nil, llm, "ws-1")

	intent, err := composite.Classify(context.Background(), "this text does not match any rule")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if intent.Kind != IntentUnknown {
		t.Errorf("Kind = %q, want Unknown", intent.Kind)
	}
	if intent.Source != SourceRule {
		t.Errorf("Source = %q, want Rule", intent.Source)
	}
}

// ---------------------------------------------------------------------------
// TC-risk-4 (Recommended): concurrent Allow calls are race-safe.
// ---------------------------------------------------------------------------

func TestSlidingWindowQuota_ConcurrentAllow(t *testing.T) {
	t.Parallel()

	quota := NewSlidingWindowQuota(QuotaConfig{
		WorkspaceID: "ws-1",
		Limit:       100,
		Window:      time.Hour,
	})

	const workers = 50
	var wg sync.WaitGroup
	allowed := make(chan bool, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed <- quota.Allow("ws-1")
		}()
	}

	wg.Wait()
	close(allowed)

	var allowedCount int
	for a := range allowed {
		if a {
			allowedCount++
		}
	}

	if allowedCount != workers {
		t.Errorf("allowed count = %d, want %d (all concurrent workers)", allowedCount, workers)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mustMarshal(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
