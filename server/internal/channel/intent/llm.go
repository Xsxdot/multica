package intent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	defaultModel   = "gpt-4o-mini"
	defaultTimeout = 3 * time.Second
	maxInputChars  = 4000 // ~1k tokens for mixed CJK/ASCII
)

// MetricsCollector abstracts prometheus-style metrics for testability.
type MetricsCollector interface {
	RecordTokenUsed(tokens int64, source IntentSource)
	RecordQuotaExhausted()
}

// IntentClassifier is the abstraction for LLM-based intent classification.
type IntentClassifier interface {
	Classify(ctx context.Context, text string) (Intent, error)
}

// LLMClassifierConfig holds configuration for LLMClassifier.
type LLMClassifierConfig struct {
	APIURL           string
	APIKey           string
	Model            string
	Timeout          time.Duration
	MaxTokens        int // per-call token budget; 0 = default 1000
	MetricsCollector MetricsCollector
}

// LLMClassifier sends user text to an LLM with a constrained system prompt
// and parses the structured JSON response into an Intent.
type LLMClassifier struct {
	apiURL    string
	apiKey    string
	model     string
	timeout   time.Duration
	maxTokens int
	collector MetricsCollector
	client    *http.Client
}

// NewLLMClassifier creates an LLMClassifier with the given config.
func NewLLMClassifier(cfg LLMClassifierConfig) *LLMClassifier {
	model := cfg.Model
	if model == "" {
		model = defaultModel
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	maxTokens := cfg.MaxTokens
	if maxTokens == 0 {
		maxTokens = 1000
	}
	return &LLMClassifier{
		apiURL:    cfg.APIURL,
		apiKey:    cfg.APIKey,
		model:     model,
		timeout:   timeout,
		maxTokens: maxTokens,
		collector: cfg.MetricsCollector,
		client:    &http.Client{Timeout: timeout},
	}
}

// systemPrompt is ≤300 tokens and forces JSON Schema output.
const systemPrompt = `You are an intent classifier for a project management chatbot.
Classify the user message into exactly one intent and extract parameters.

Valid intents: CreateIssue, AddComment, QueryIssue, SetStatus, Unsupported, Unknown

Respond with ONLY valid JSON:
{"intent":"<IntentKind>","confidence":0.0-1.0,"params":{<key-value pairs>}}

Rules:
- CreateIssue: params must include "title"
- AddComment: params must include "issue_key" and "comment"
- QueryIssue: params must include "issue_key" (or empty for "my todos")
- SetStatus: params must include "issue_key" and "status"
- Unsupported: destructive operations (delete, upload media)
- Unknown: greetings, ambiguous, off-topic
- confidence < 0.7 will trigger ASK_CLARIFY fallback
- issue_key format: LETTERS-NUMBERS (e.g. STA-2, BUG-42)`

// ResponseFormat requests a structured model output shape (OpenAI-compatible).
type ResponseFormat struct {
	Type string `json:"type"`
}

// ChatCompletionRequest is the OpenAI-compatible chat completion request.
type ChatCompletionRequest struct {
	Model          string          `json:"model"`
	Messages       []Message       `json:"messages"`
	MaxTokens      int             `json:"max_tokens,omitempty"`
	ResponseFormat *ResponseFormat `json:"response_format,omitempty"`
}

// Message is a chat message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatCompletionResponse is the OpenAI-compatible chat completion response.
type ChatCompletionResponse struct {
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

// Choice is a single completion choice.
type Choice struct {
	Message Message `json:"message"`
}

// Usage reports token consumption.
type Usage struct {
	TotalTokens int `json:"total_tokens"`
}

// LLMResponse is the structured JSON we expect from the LLM.
type LLMResponse struct {
	Intent     string            `json:"intent"`
	Confidence float64           `json:"confidence"`
	Params     map[string]string `json:"params"`
}

// Classify sends text to the LLM and returns the parsed intent.
func (c *LLMClassifier) Classify(ctx context.Context, text string) (Intent, error) {
	text = c.truncateInput(text)

	reqBody := ChatCompletionRequest{
		Model: c.model,
		Messages: []Message{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: text},
		},
		MaxTokens:      c.maxTokens,
		ResponseFormat: &ResponseFormat{Type: "json_object"},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return Intent{}, fmt.Errorf("intent: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return Intent{}, fmt.Errorf("intent: create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return Intent{}, fmt.Errorf("intent: llm request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return Intent{}, fmt.Errorf("intent: llm api error %d: %s", resp.StatusCode, string(respBody))
	}

	var chatResp ChatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return Intent{}, fmt.Errorf("intent: decode response: %w", err)
	}

	if len(chatResp.Choices) == 0 {
		return Intent{}, fmt.Errorf("intent: empty choices in response")
	}

	// Report token metrics
	if c.collector != nil && chatResp.Usage.TotalTokens > 0 {
		c.collector.RecordTokenUsed(int64(chatResp.Usage.TotalTokens), SourceLLM)
	}

	var llmResp LLMResponse
	content := stripMarkdownFences(chatResp.Choices[0].Message.Content)
	if err := json.Unmarshal([]byte(content), &llmResp); err != nil {
		return Intent{}, fmt.Errorf("intent: parse llm json: %w", err)
	}

	if llmResp.Confidence < 0 {
		llmResp.Confidence = 0
	}
	if llmResp.Confidence > 1 {
		llmResp.Confidence = 1
	}

	// Validate intent kind
	kind := IntentKind(llmResp.Intent)
	if !isValidIntentKind(kind) {
		return Intent{}, fmt.Errorf("intent: unknown intent kind %q", llmResp.Intent)
	}

	// Low confidence → ASK_CLARIFY
	if llmResp.Confidence < 0.7 {
		kind = IntentASKClarify
	}

	params := llmResp.Params
	if params == nil {
		params = map[string]string{}
	}

	return Intent{
		Kind:       kind,
		Confidence: llmResp.Confidence,
		Params:     params,
		Source:     SourceLLM,
	}, nil
}

// stripMarkdownFences removes ```json / ``` wrappers some models emit around JSON payloads.
func stripMarkdownFences(content string) string {
	s := strings.TrimSpace(content)
	if strings.HasPrefix(s, "```json") {
		s = strings.TrimPrefix(s, "```json")
	} else if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```")
	}
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "```") {
		s = strings.TrimSuffix(s, "```")
	}
	return strings.TrimSpace(s)
}

func (c *LLMClassifier) truncateInput(text string) string {
	maxChars := c.maxTokens * 4
	runes := []rune(text)
	if len(runes) <= maxChars {
		return text
	}
	slog.Warn("intent: input truncated", "original_chars", len(runes), "truncated_to", maxChars)
	return string(runes[:maxChars])
}

func isValidIntentKind(k IntentKind) bool {
	switch k {
	case IntentCreateIssue, IntentAddComment, IntentQueryIssue,
		IntentSetStatus, IntentUnsupported, IntentUnknown:
		return true
	default:
		return false
	}
}
