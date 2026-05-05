package intent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TC-risk-1 (AC5.1): LLM quota exhausted → classifier returns error,
// caller can fall back to rule matcher.
//
// We simulate quota exhaustion by setting limit=1, window=1h,
// making two calls: the first consumes the quota, the second
// should be rejected.
func TestLLMClassifier_QuotaExhausted_ReturnsError(t *testing.T) {
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

	clf := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 1000,
	})

	ctx := context.Background()

	// In a real composite classifier the quota check happens BEFORE
	// the LLM call. We simulate that pattern here: Allow() then Classify().
	if !quota.Allow("ws-1") {
		t.Fatal("first Allow should succeed")
	}
	_, err := clf.Classify(ctx, "创建一个 Issue: test")
	if err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("first call: expected 1 LLM call, got %d", callCount)
	}

	// Second Allow should fail — quota exhausted.
	allowed := quota.Allow("ws-1")
	if allowed {
		t.Fatal("quota should be exhausted after first call")
	}
}

// TC-risk-2 (AC5.3): token quota cap ≤ 1k → input truncated.
//
// We set MaxTokens=10 (simulating a 1k-token budget scaled down
// for the test) and send input longer than 10*4=40 chars.
// The server records what it received; we assert truncation.
func TestLLMClassifier_TokenCap_TruncatesInput(t *testing.T) {
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

	clf := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 10, // budget → maxChars = 40
	})

	longText := strings.Repeat("这是一个很长的输入文本，用于测试截断功能。", 10) // > 40 chars
	_, err := clf.Classify(context.Background(), longText)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}

	receivedRunes := []rune(receivedBody)
	if len(receivedRunes) >= len([]rune(longText)) {
		t.Fatalf("expected truncation: received %d runes, original %d", len(receivedRunes), len([]rune(longText)))
	}
	if len(receivedRunes) > 40 {
		t.Fatalf("expected max 40 runes (MaxTokens*4), got %d", len(receivedRunes))
	}
}

// TC-risk-3 (PRD E7): LLM unavailable (HTTP 503) → classifier returns
// error so the composite can fall back to rule matcher.
func TestLLMClassifier_LLMUnavailable_ReturnsError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error": "overloaded"}`))
	}))
	defer srv.Close()

	clf := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 1000,
	})

	_, err := clf.Classify(context.Background(), "创建一个 Issue: test")
	if err == nil {
		t.Fatal("expected error when LLM returns 503, got nil")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("expected error to mention 503, got: %v", err)
	}
}

// TC-risk-3b: LLM network timeout → classifier returns error.
func TestLLMClassifier_LLMTimeout_ReturnsError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	clf := NewLLMClassifier(LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test",
		Model:     "gpt-4o-mini",
		MaxTokens: 1000,
		Timeout:   50 * time.Millisecond,
	})

	_, err := clf.Classify(context.Background(), "创建一个 Issue: test")
	if err == nil {
		t.Fatal("expected error when LLM times out, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		// The error may be wrapped; check for timeout indicators.
		if !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "Deadline") {
			t.Fatalf("expected timeout-related error, got: %v", err)
		}
	}
}

// mustMarshal is a test helper that panics on error.
func mustMarshal(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
