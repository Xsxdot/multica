package intent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	in "github.com/multica-ai/multica/server/internal/channel/intent"
)

// --- IntentClassifier interface tests ---

func TestIntentClassifier_Interface(t *testing.T) {
	// Compile-time check: LLMClassifier satisfies IntentClassifier.
	var _ in.IntentClassifier = (*in.LLMClassifier)(nil)
}

// --- LLMClassifier.Classify happy path ---

func TestLLMClassifier_Classify_HappyPath(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := in.LLMResponse{
			Intent:     "CreateIssue",
			Confidence: 0.92,
			Params:     map[string]string{"title": "登录页白屏"},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(in.ChatCompletionResponse{
			Choices: []in.Choice{{Message: in.Message{Content: mustMarshal(resp)}}},
			Usage:   in.Usage{TotalTokens: 320},
		})
	}))
	defer srv.Close()

	clf := in.NewLLMClassifier(in.LLMClassifierConfig{
		APIURL: srv.URL,
		APIKey: "test-key",
		Model:  "gpt-4o-mini",
	})

	got, err := clf.Classify(context.Background(), "帮我记一个登录页白屏的问题")
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if got.Kind != in.IntentCreateIssue {
		t.Errorf("Kind = %q, want CreateIssue", got.Kind)
	}
	if got.Confidence != 0.92 {
		t.Errorf("Confidence = %v, want 0.92", got.Confidence)
	}
	if got.Source != in.SourceLLM {
		t.Errorf("Source = %q, want %q", got.Source, in.SourceLLM)
	}
	if got.Params["title"] != "登录页白屏" {
		t.Errorf("title = %q, want 登录页白屏", got.Params["title"])
	}
}

func TestLLMClassifier_Classify_FencedJSON(t *testing.T) {
	t.Parallel()
	resp := in.LLMResponse{
		Intent:     "CreateIssue",
		Confidence: 0.88,
		Params:     map[string]string{"title": "demo"},
	}
	raw := mustMarshal(resp)

	for name, fenced := range map[string]string{
		"json_fence":  "```json\n" + raw + "\n```",
		"plain_fence": "```\n" + raw + "\n```",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(in.ChatCompletionResponse{
					Choices: []in.Choice{{Message: in.Message{Content: fenced}}},
					Usage:   in.Usage{TotalTokens: 120},
				})
			}))
			defer srv.Close()

			clf := in.NewLLMClassifier(in.LLMClassifierConfig{
				APIURL: srv.URL,
				APIKey: "test-key",
				Model:  "gpt-4o-mini",
			})

			got, err := clf.Classify(context.Background(), "开一个 demo issue")
			if err != nil {
				t.Fatalf("Classify: %v", err)
			}
			if got.Kind != in.IntentCreateIssue {
				t.Errorf("Kind = %q, want CreateIssue", got.Kind)
			}
			if got.Confidence != 0.88 {
				t.Errorf("Confidence = %v, want 0.88", got.Confidence)
			}
			if got.Params["title"] != "demo" {
				t.Errorf(`title = %q, want "demo"`, got.Params["title"])
			}
		})
	}
}

// --- Confidence < 0.7 → ASK_CLARIFY ---

func TestLLMClassifier_Classify_LowConfidence_ASK_CLARIFY(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := in.LLMResponse{
			Intent:     "Unknown",
			Confidence: 0.3,
			Params:     map[string]string{},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(in.ChatCompletionResponse{
			Choices: []in.Choice{{Message: in.Message{Content: mustMarshal(resp)}}},
			Usage:   in.Usage{TotalTokens: 200},
		})
	}))
	defer srv.Close()

	clf := in.NewLLMClassifier(in.LLMClassifierConfig{
		APIURL: srv.URL,
		APIKey: "test-key",
		Model:  "gpt-4o-mini",
	})

	got, err := clf.Classify(context.Background(), "随便聊聊")
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if got.Kind != in.IntentASKClarify {
		t.Errorf("Kind = %q, want ASK_CLARIFY", got.Kind)
	}
	if got.Source != in.SourceLLM {
		t.Errorf("Source = %q, want %q", got.Source, in.SourceLLM)
	}
}

// --- Input > 1k tokens → truncated + warn (we simulate by counting tokens) ---

func TestLLMClassifier_Classify_InputTruncation(t *testing.T) {
	t.Parallel()
	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req in.ChatCompletionRequest
		json.NewDecoder(r.Body).Decode(&req)
		receivedBody = req.Messages[1].Content // user message
		resp := in.LLMResponse{
			Intent:     "AddComment",
			Confidence: 0.8,
			Params:     map[string]string{"issue_key": "STA-1", "comment": "test"},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(in.ChatCompletionResponse{
			Choices: []in.Choice{{Message: in.Message{Content: mustMarshal(resp)}}},
			Usage:   in.Usage{TotalTokens: 500},
		})
	}))
	defer srv.Close()

	clf := in.NewLLMClassifier(in.LLMClassifierConfig{
		APIURL:    srv.URL,
		APIKey:    "test-key",
		Model:     "gpt-4o-mini",
		MaxTokens: 1000, // budget
	})

	// Generate text > 1000 tokens (~4000 chars for English, ~2000 for Chinese)
	longText := strings.Repeat("这是一个很长的输入文本，用于测试截断功能。", 300)
	_, err := clf.Classify(context.Background(), longText)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	// The received body should be truncated (shorter than original)
	if len(receivedBody) >= len(longText) {
		t.Errorf("expected truncation: received %d chars, original %d", len(receivedBody), len(longText))
	}
}

// --- LLM returns invalid JSON → error ---

func TestLLMClassifier_Classify_InvalidJSON(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(in.ChatCompletionResponse{
			Choices: []in.Choice{{Message: in.Message{Content: "not valid json"}}},
			Usage:   in.Usage{TotalTokens: 100},
		})
	}))
	defer srv.Close()

	clf := in.NewLLMClassifier(in.LLMClassifierConfig{
		APIURL: srv.URL,
		APIKey: "test-key",
		Model:  "gpt-4o-mini",
	})

	_, err := clf.Classify(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error for invalid JSON response")
	}
}

// --- LLM returns unknown intent kind → error ---

func TestLLMClassifier_Classify_UnknownIntentKind(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := in.LLMResponse{
			Intent:     "BogusIntent",
			Confidence: 0.9,
			Params:     map[string]string{},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(in.ChatCompletionResponse{
			Choices: []in.Choice{{Message: in.Message{Content: mustMarshal(resp)}}},
			Usage:   in.Usage{TotalTokens: 100},
		})
	}))
	defer srv.Close()

	clf := in.NewLLMClassifier(in.LLMClassifierConfig{
		APIURL: srv.URL,
		APIKey: "test-key",
		Model:  "gpt-4o-mini",
	})

	_, err := clf.Classify(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error for unknown intent kind")
	}
}

// --- API timeout → error ---

func TestLLMClassifier_Classify_Timeout(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer srv.Close()

	clf := in.NewLLMClassifier(in.LLMClassifierConfig{
		APIURL:  srv.URL,
		APIKey:  "test-key",
		Model:   "gpt-4o-mini",
		Timeout: 100 * time.Millisecond,
	})

	_, err := clf.Classify(context.Background(), "test")
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// --- API returns non-200 → error ---

func TestLLMClassifier_Classify_APIError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte("rate limited"))
	}))
	defer srv.Close()

	clf := in.NewLLMClassifier(in.LLMClassifierConfig{
		APIURL: srv.URL,
		APIKey: "test-key",
		Model:  "gpt-4o-mini",
	})

	_, err := clf.Classify(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error for non-200 response")
	}
}

// --- System prompt ≤ 300 tokens (verified via request capture) ---

func TestLLMClassifier_SystemPrompt_TokenBudget(t *testing.T) {
	t.Parallel()
	var systemPrompt string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req in.ChatCompletionRequest
		json.NewDecoder(r.Body).Decode(&req)
		if len(req.Messages) > 0 && req.Messages[0].Role == "system" {
			systemPrompt = req.Messages[0].Content
		}
		resp := in.LLMResponse{Intent: "Unknown", Confidence: 0.5, Params: map[string]string{}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(in.ChatCompletionResponse{
			Choices: []in.Choice{{Message: in.Message{Content: mustMarshal(resp)}}},
			Usage:   in.Usage{TotalTokens: 100},
		})
	}))
	defer srv.Close()

	clf := in.NewLLMClassifier(in.LLMClassifierConfig{
		APIURL: srv.URL,
		APIKey: "test-key",
		Model:  "gpt-4o-mini",
	})

	clf.Classify(context.Background(), "test")

	// Rough token estimate: 1 token ≈ 4 chars for English, ~2 chars for Chinese
	// 300 tokens ≈ 600 Chinese chars as upper bound
	if len(systemPrompt) > 900 { // generous upper bound
		t.Errorf("system prompt too long: %d chars (expected ≤ ~900 for 300 tokens)", len(systemPrompt))
	}
	if systemPrompt == "" {
		t.Fatal("system prompt should not be empty")
	}
}

// --- Metrics: token usage reported ---

func TestLLMClassifier_Metrics_TokenUsage(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := in.LLMResponse{Intent: "CreateIssue", Confidence: 0.9, Params: map[string]string{"title": "test"}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(in.ChatCompletionResponse{
			Choices: []in.Choice{{Message: in.Message{Content: mustMarshal(resp)}}},
			Usage:   in.Usage{TotalTokens: 456},
		})
	}))
	defer srv.Close()

	collector := &fakeCollector{}
	clf := in.NewLLMClassifier(in.LLMClassifierConfig{
		APIURL:           srv.URL,
		APIKey:           "test-key",
		Model:            "gpt-4o-mini",
		MetricsCollector: collector,
	})

	clf.Classify(context.Background(), "帮我记一个问题")

	if collector.tokens != 456 {
		t.Errorf("reported tokens = %d, want 456", collector.tokens)
	}
	if collector.source != in.SourceLLM {
		t.Errorf("source = %q, want %q", collector.source, in.SourceLLM)
	}
}

// --- FakeCollector for testing ---

type fakeCollector struct {
	tokens int64
	source in.IntentSource
}

func (c *fakeCollector) RecordTokenUsed(tokens int64, source in.IntentSource) {
	atomic.StoreInt64(&c.tokens, tokens)
	c.source = source
}

func (c *fakeCollector) RecordQuotaExhausted() {}

// --- Helpers ---

func mustMarshal(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
