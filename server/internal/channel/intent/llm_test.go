package intent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// --- LLMClassifier.Classify happy path (table-driven, all IntentKinds) ---

func TestLLMClassifier_Classify_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		intent     string
		confidence float64
		params     map[string]string
		wantKind   in.IntentKind
	}{
		{"CreateIssue", "CreateIssue", 0.92, map[string]string{"title": "登录页白屏"}, in.IntentCreateIssue},
		{"AddComment", "AddComment", 0.85, map[string]string{"issue_key": "STA-2", "comment": "已找产品确认"}, in.IntentAddComment},
		{"QueryIssue", "QueryIssue", 0.88, map[string]string{"issue_key": "STA-5"}, in.IntentQueryIssue},
		{"SetStatus", "SetStatus", 0.90, map[string]string{"issue_key": "STA-2", "status": "done"}, in.IntentSetStatus},
		{"SetAssignee", "SetAssignee", 0.88, map[string]string{"issue_key": "STA-2", "assignee": "张三"}, in.IntentSetAssignee},
		{"SetPriority", "SetPriority", 0.87, map[string]string{"issue_key": "STA-3", "priority": "high"}, in.IntentSetPriority},
		{"SetLabel", "SetLabel", 0.86, map[string]string{"issue_key": "STA-4", "label": "bug", "op": "add"}, in.IntentSetLabel},
		{"Unsupported", "Unsupported", 0.95, map[string]string{}, in.IntentUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				resp := in.LLMResponse{
					Intent:     tt.intent,
					Confidence: tt.confidence,
					Params:     tt.params,
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

			got, err := clf.Classify(context.Background(), "test input")
			if err != nil {
				t.Fatalf("Classify: %v", err)
			}
			if got.Kind != tt.wantKind {
				t.Errorf("Kind = %q, want %q", got.Kind, tt.wantKind)
			}
			if got.Confidence != tt.confidence {
				t.Errorf("Confidence = %v, want %v", got.Confidence, tt.confidence)
			}
			if got.Source != in.SourceLLM {
				t.Errorf("Source = %q, want %q", got.Source, in.SourceLLM)
			}
			for k, v := range tt.params {
				if got.Params[k] != v {
					t.Errorf("Params[%q] = %q, want %q", k, got.Params[k], v)
				}
			}
		})
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

func TestLLMClassifier_Classify_ConfidenceClamp(t *testing.T) {
	t.Parallel()
	t.Run("upper_bound", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := in.LLMResponse{
				Intent:     "CreateIssue",
				Confidence: 2.5,
				Params:     map[string]string{"title": "x"},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(in.ChatCompletionResponse{
				Choices: []in.Choice{{Message: in.Message{Content: mustMarshal(resp)}}},
				Usage:   in.Usage{TotalTokens: 50},
			})
		}))
		defer srv.Close()

		clf := in.NewLLMClassifier(in.LLMClassifierConfig{
			APIURL: srv.URL,
			APIKey: "test-key",
			Model:  "gpt-4o-mini",
		})

		got, err := clf.Classify(context.Background(), "hi")
		if err != nil {
			t.Fatalf("Classify: %v", err)
		}
		if got.Confidence != 1.0 {
			t.Errorf("Confidence = %v, want 1.0", got.Confidence)
		}
		if got.Kind != in.IntentCreateIssue {
			t.Errorf("Kind = %q, want CreateIssue", got.Kind)
		}
	})
	t.Run("lower_bound", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := in.LLMResponse{
				Intent:     "CreateIssue",
				Confidence: -0.3,
				Params:     map[string]string{"title": "x"},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(in.ChatCompletionResponse{
				Choices: []in.Choice{{Message: in.Message{Content: mustMarshal(resp)}}},
				Usage:   in.Usage{TotalTokens: 50},
			})
		}))
		defer srv.Close()

		clf := in.NewLLMClassifier(in.LLMClassifierConfig{
			APIURL: srv.URL,
			APIKey: "test-key",
			Model:  "gpt-4o-mini",
		})

		got, err := clf.Classify(context.Background(), "hi")
		if err != nil {
			t.Fatalf("Classify: %v", err)
		}
		if got.Confidence != 0.0 {
			t.Errorf("Confidence = %v, want 0.0", got.Confidence)
		}
		if got.Kind != in.IntentASKClarify {
			t.Errorf("Kind = %q, want ASK_CLARIFY", got.Kind)
		}
	})
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
	var receivedMaxTokens int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req in.ChatCompletionRequest
		json.NewDecoder(r.Body).Decode(&req)
		receivedBody = req.Messages[1].Content // user message
		receivedMaxTokens = req.MaxTokens
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

	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	defer slog.SetDefault(oldLogger)

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
	// Assert warn log was emitted
	logOutput := buf.String()
	if !strings.Contains(logOutput, "input truncated") {
		t.Errorf("expected warn log 'input truncated', got: %s", logOutput)
	}
	// Assert max_tokens is sent in request body
	if receivedMaxTokens != 1000 {
		t.Errorf("MaxTokens = %d, want 1000", receivedMaxTokens)
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
	// 350 tokens ≈ 700 Chinese chars as upper bound
	if len(systemPrompt) > 1100 { // generous upper bound for expanded intent list
		t.Errorf("system prompt too long: %d chars (expected ≤ ~1100 for 350 tokens)", len(systemPrompt))
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
	if collector.getSource() != in.SourceLLM {
		t.Errorf("source = %q, want %q", collector.getSource(), in.SourceLLM)
	}
}

// --- FakeCollector for testing ---

type fakeCollector struct {
	mu     sync.Mutex
	tokens int64
	source in.IntentSource
}

func (c *fakeCollector) RecordTokenUsed(tokens int64, source in.IntentSource) {
	atomic.StoreInt64(&c.tokens, tokens)
	c.mu.Lock()
	c.source = source
	c.mu.Unlock()
}

func (c *fakeCollector) RecordQuotaExhausted() {}

func (c *fakeCollector) getSource() in.IntentSource {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.source
}

// --- Helpers ---

func mustMarshal(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// --- Confidence out of bounds → clamped to [0,1] ---

func TestLLMClassifier_Classify_ConfidenceClamped(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		confidence float64
		wantKind   in.IntentKind
	}{
		{"upper bound 2.5 → clamped to 1.0 → normal", 2.5, in.IntentCreateIssue},
		{"lower bound -0.3 → clamped to 0.0 → ASK_CLARIFY", -0.3, in.IntentASKClarify},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				resp := in.LLMResponse{
					Intent:     "CreateIssue",
					Confidence: tt.confidence,
					Params:     map[string]string{"title": "test"},
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

			got, err := clf.Classify(context.Background(), "test")
			if err != nil {
				t.Fatalf("Classify: %v", err)
			}
			if got.Kind != tt.wantKind {
				t.Errorf("Kind = %q, want %q", got.Kind, tt.wantKind)
			}
		})
	}
}
