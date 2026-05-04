# T10: Intent LLM Fallback + Quota - Detailed Implementation Plan

## Overview

Implement LLM-based intent classification as fallback when T9 rule matcher doesn't match, with quota control to prevent cost explosion.

## Current State

- **Branch**: `feat/sta-38-intent-llm` (already created from T9 commit)
- **T9 Status**: Completed on `feat/sta-37-intent-rule` branch
- **Existing Code**: `server/internal/channel/intent/` directory exists with:
  - `rule.go` - RuleMatcher interface and implementation
  - `patterns.go` - Rule patterns and Intent types
  - `rule_test.go` - Comprehensive tests

## Implementation Plan

### Phase 1: Core Types and Interfaces

#### 1.1 Update `patterns.go` - Add SourceLLM Constant

```go
// IntentSource identifies how an intent was recognised.
type IntentSource string

const (
    // SourceRule means the regex rule engine produced this intent.
    SourceRule IntentSource = "rule"
    // SourceLLM means the LLM fallback produced this intent.
    SourceLLM IntentSource = "llm"
)
```

#### 1.2 Create `quota.go` - QuotaManager Interface and Implementation

**Interface:**
```go
type QuotaManager interface {
    Allow(workspaceID string) bool
    Record(workspaceID string, tokens int)
    Remaining(workspaceID string) int
}
```

**Implementation:**
```go
type SlidingWindowQuota struct {
    mu          sync.RWMutex
    windows     map[string]*window
    maxTokens   int
    windowSize  time.Duration
    quotaExhausted *prometheus.CounterVec
}

type window struct {
    tokens    int
    resetAt   time.Time
}
```

**Metrics:**
```go
var quotaExhausted = prometheus.NewCounterVec(prometheus.CounterOpts{
    Namespace: "multica",
    Subsystem: "channel_intent",
    Name:      "quota_exhausted_total",
    Help:      "Total number of LLM quota exhaustion events.",
}, []string{"workspace_id"})
```

#### 1.3 Create `llm.go` - IntentClassifier Interface and LLMClassifier

**Interface:**
```go
type IntentClassifier interface {
    Classify(ctx context.Context, text string) (Intent, error)
}
```

**LLMClassifier Implementation:**
```go
type LLMClassifier struct {
    client      *http.Client
    logger      *slog.Logger
    apiKey      string
    endpoint    string
    model       string
    tokenBudget int
    quotaMgr    QuotaManager
    
    // Metrics
    intentTotal    *prometheus.CounterVec
    tokenUsed      prometheus.Histogram
}
```

**System Prompt (≤ 300 tokens):**
```
You are an intent classifier for a project management chatbot.
Analyze the user message and return JSON with this schema:
{
  "intent": "CreateIssue|AddComment|QueryIssue|SetStatus|Unsupported|Unknown",
  "confidence": 0.0-1.0,
  "params": {
    "issue_key": "if applicable",
    "title": "if CreateIssue",
    "comment": "if AddComment",
    "status": "if SetStatus"
  }
}
Rules:
- CreateIssue: user wants to create a new issue/task
- AddComment: user wants to add a comment to existing issue
- QueryIssue: user wants to check issue status/progress
- SetStatus: user wants to change issue status
- Unsupported: destructive operations (delete, upload media)
- Unknown: greetings, unclear intent
Return ONLY valid JSON, no explanation.
```

**Classify Method Logic:**
```go
func (c *LLMClassifier) Classify(ctx context.Context, text string) (Intent, error) {
    // 1. Validate input length (≤ 1k tokens, truncate if exceeded)
    // 2. Check quota via QuotaManager.Allow()
    // 3. Call LLM API with system prompt + user text
    // 4. Parse JSON response
    // 5. Map to Intent struct
    // 6. Record token usage in QuotaManager
    // 7. Update Prometheus metrics
}
```

### Phase 2: Tests

#### 2.1 Create `quota_test.go`

**Test Cases:**
```go
func TestSlidingWindowQuota_Allow(t *testing.T) {
    // Test quota available returns true
}

func TestSlidingWindowQuota_Exhausted(t *testing.T) {
    // Test quota exhausted returns false
}

func TestSlidingWindowQuota_WindowReset(t *testing.T) {
    // Test quota resets after 1 hour window
}

func TestSlidingWindowQuota_Concurrent(t *testing.T) {
    // Test thread-safety under concurrent access
}
```

#### 2.2 Create `llm_test.go`

**Test Cases:**
```go
func TestLLMClassifier_Success(t *testing.T) {
    // Mock HTTP server returns valid JSON
}

func TestLLMClassifier_InvalidJSON(t *testing.T) {
    // Handle malformed response
}

func TestLLMClassifier_Timeout(t *testing.T) {
    // Respect timeout configuration
}

func TestLLMClassifier_TokenBudget(t *testing.T) {
    // Enforce ≤ 1k token limit
}

func TestLLMClassifier_InputTruncation(t *testing.T) {
    // >1k token input forced truncation + warn log
}

func TestLLMClassifier_LowConfidence(t *testing.T) {
    // Confidence < 0.7 triggers ASK_CLARIFY
}

func TestLLMClassifier_QuotaExhausted(t *testing.T) {
    // Mock quota exhausted → rule still processes
    // LLM-only input returns LLM_QUOTA_EXHAUSTED template
}
```

**Mock Strategy:**
```go
func newMockLLMServer(response string) *httptest.Server {
    return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        w.Write([]byte(response))
    }))
}
```

### Phase 3: Integration

#### 3.1 Update `steps_placeholder.go` - Integrate LLM Fallback

**Current:**
```go
func NewIntentRecogStep() Step { return passthroughStep{name: "intent-recog"} }
```

**Updated:**
```go
func NewIntentRecogStep(ruleMatcher intent.RuleMatcher, classifier intent.IntentClassifier, quotaMgr intent.QuotaManager) Step {
    return &intentRecogStep{
        ruleMatcher: ruleMatcher,
        classifier:  classifier,
        quotaMgr:    quotaMgr,
    }
}

type intentRecogStep struct {
    ruleMatcher intent.RuleMatcher
    classifier  intent.IntentClassifier
    quotaMgr    intent.QuotaManager
}

func (s *intentRecogStep) Name() string { return "intent-recog" }

func (s *intentRecogStep) Run(ctx context.Context, evt port.InboundEvent) (port.InboundEvent, Decision, error) {
    // 1. Try RuleMatcher.Match(text)
    // 2. If no hit, check QuotaManager.Allow(workspaceID)
    // 3. If quota allows, call IntentClassifier.Classify(ctx, text)
    // 4. If quota exhausted, return IntentUnknown with source="rule" (degraded mode)
}
```

#### 3.2 Update `cmd/server/main.go` - Wire Dependencies

```go
// Create LLM dependencies
quotaMgr := intent.NewSlidingWindowQuota(100000, time.Hour)
classifier := intent.NewLLMClassifier(
    os.Getenv("OPENAI_API_KEY"),
    os.Getenv("LLM_MODEL"),
    quotaMgr,
)

// Update pipeline
pipeline := inbound.NewPipeline(
    inbound.NewNormalizeStep(),
    inbound.NewDedupStep(dedupStore),
    inbound.NewIdentityBindStep(),
    inbound.NewIntentRecogStep(ruleMatcher, classifier, quotaMgr),
    inbound.NewDispatchStep(),
    inbound.NewReplyStep(),
)
```

### Phase 4: Configuration

#### 4.1 Update `.env.example`

```bash
# LLM Configuration for Intent Classification (T10)
# OpenAI API Key (for gpt-4o-mini)
OPENAI_API_KEY=
# Anthropic API Key (for claude-3-5-haiku)
ANTHROPIC_API_KEY=
# LLM Model to use (gpt-4o-mini or claude-3-5-haiku)
LLM_MODEL=gpt-4o-mini
# LLM API Endpoint (optional, defaults to provider's endpoint)
LLM_API_ENDPOINT=
# LLM Token Budget per call (default: 1000)
LLM_TOKEN_BUDGET=1000
# LLM Quota per workspace per hour (default: 100000 tokens)
LLM_QUOTA_PER_HOUR=100000
```

## Exit Test Compliance

### TC-intent-1 (LLM part)
- Implement FakeClassifier with testdata/corpus.json
- Test rows #3, #4, #5, #8, #9, #10, #14, #15, #19, #20 (source=llm)

### TC-intent-3
- Single input token ≤ 1k
- Implement token counting and truncation
- Log warning when truncation occurs

### TC-risk-1
- Mock quota exhausted → rule still processes
- LLM-only input returns LLM_QUOTA_EXHAUSTED template

### TC-risk-2
- >1k token input forced truncation + warn log
- Implement in Classify method

## Dependencies

- T9 (STA-37) - Rule matcher (already completed)
- Prometheus client library (already in go.mod)
- slog for logging (already used)

## Risk Mitigation

1. **LLM API Failures**: Graceful degradation to rule-only mode
2. **Cost Control**: Strict quota enforcement + token budget
3. **Performance**: Timeout enforcement (p95 ≤ 1.5s)
4. **Thread Safety**: RWMutex for quota manager

## Testing Strategy

1. **Unit Tests**: Mock HTTP server for LLM API
2. **Integration Tests**: Test with real pipeline (using FakeClassifier)
3. **Concurrency Tests**: Verify thread-safety of quota manager

## File Changes Summary

| File | Action | Description |
|------|--------|-------------|
| `server/internal/channel/intent/patterns.go` | Modify | Add SourceLLM constant |
| `server/internal/channel/intent/quota.go` | Create | QuotaManager interface and implementation |
| `server/internal/channel/intent/llm.go` | Create | IntentClassifier interface and LLMClassifier |
| `server/internal/channel/intent/quota_test.go` | Create | QuotaManager tests |
| `server/internal/channel/intent/llm_test.go` | Create | LLMClassifier tests |
| `server/internal/channel/inbound/steps_placeholder.go` | Modify | Integrate LLM fallback |
| `server/cmd/server/main.go` | Modify | Wire dependencies |
| `.env.example` | Modify | Add LLM configuration |
