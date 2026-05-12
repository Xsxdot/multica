package inbound_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/channel"
	"github.com/multica-ai/multica/server/internal/channel/facade"
	"github.com/multica-ai/multica/server/internal/channel/gateway"
	"github.com/multica-ai/multica/server/internal/channel/inbound"
	"github.com/multica-ai/multica/server/internal/channel/port"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

type fakeChatBinding struct {
	wsID pgtype.UUID
	err  error
}

func (f *fakeChatBinding) LookupWorkspaceID(_ context.Context, _, _ string) (pgtype.UUID, error) {
	return f.wsID, f.err
}

type fakeUserResolver struct {
	user inbound.ResolvedUser
	err  error
}

func (f *fakeUserResolver) Resolve(_ context.Context, _, _ string) (inbound.ResolvedUser, error) {
	return f.user, f.err
}

type fakeProjectValidator struct {
	calls []struct {
		WorkspaceID pgtype.UUID
		ProjectID   pgtype.UUID
	}
	err error
}

func (f *fakeProjectValidator) ValidateProjectInWorkspace(_ context.Context, workspaceID, projectID pgtype.UUID) error {
	f.calls = append(f.calls, struct {
		WorkspaceID pgtype.UUID
		ProjectID   pgtype.UUID
	}{WorkspaceID: workspaceID, ProjectID: projectID})
	return f.err
}

type fakeIssueService struct {
	created []facade.CreateIssueReq
	gotByID []struct {
		WorkspaceID pgtype.UUID
		Identifier  string
	}
	setStatus []struct {
		ID      pgtype.UUID
		ActorID pgtype.UUID
		Status  string
	}
	setAssignee []struct {
		ID                 pgtype.UUID
		ActorID            pgtype.UUID
		AssigneeIdentifier string
	}
	setPriority []struct {
		ID       pgtype.UUID
		ActorID  pgtype.UUID
		Priority string
	}
	addLabel []struct {
		ID        pgtype.UUID
		ActorID   pgtype.UUID
		LabelName string
	}
	removeLabel []struct {
		ID        pgtype.UUID
		ActorID   pgtype.UUID
		LabelName string
	}
	listTodos []struct {
		WorkspaceID pgtype.UUID
		UserID      pgtype.UUID
	}
	createReturn       facade.Issue
	getByIdentifierRet facade.Issue
	listTodosReturn    []facade.Issue
	createErr          error
	getByIdentifierErr error
	setStatusErr       error
	setAssigneeErr     error
	setPriorityErr     error
	addLabelErr        error
	removeLabelErr     error
	listTodosErr       error
}

func (f *fakeIssueService) CreateIssue(_ context.Context, req facade.CreateIssueReq) (facade.Issue, error) {
	f.created = append(f.created, req)
	return f.createReturn, f.createErr
}

func (f *fakeIssueService) GetIssue(_ context.Context, _ pgtype.UUID) (facade.Issue, error) {
	return facade.Issue{}, nil
}

func (f *fakeIssueService) GetIssueByIdentifier(_ context.Context, wsID pgtype.UUID, identifier string) (facade.Issue, error) {
	f.gotByID = append(f.gotByID, struct {
		WorkspaceID pgtype.UUID
		Identifier  string
	}{wsID, identifier})
	return f.getByIdentifierRet, f.getByIdentifierErr
}

func (f *fakeIssueService) SetIssueStatus(_ context.Context, id pgtype.UUID, actorID pgtype.UUID, status string, _ facade.ChannelMutationContext) error {
	f.setStatus = append(f.setStatus, struct {
		ID      pgtype.UUID
		ActorID pgtype.UUID
		Status  string
	}{id, actorID, status})
	return f.setStatusErr
}

func (f *fakeIssueService) SetIssueAssignee(_ context.Context, id pgtype.UUID, actorID pgtype.UUID, assigneeIdentifier string, _ facade.ChannelMutationContext) error {
	f.setAssignee = append(f.setAssignee, struct {
		ID                 pgtype.UUID
		ActorID            pgtype.UUID
		AssigneeIdentifier string
	}{id, actorID, assigneeIdentifier})
	return f.setAssigneeErr
}

func (f *fakeIssueService) SetIssuePriority(_ context.Context, id pgtype.UUID, actorID pgtype.UUID, priority string, _ facade.ChannelMutationContext) error {
	f.setPriority = append(f.setPriority, struct {
		ID       pgtype.UUID
		ActorID  pgtype.UUID
		Priority string
	}{id, actorID, priority})
	return f.setPriorityErr
}

func (f *fakeIssueService) AddIssueLabel(_ context.Context, id pgtype.UUID, actorID pgtype.UUID, labelName string, _ facade.ChannelMutationContext) error {
	f.addLabel = append(f.addLabel, struct {
		ID        pgtype.UUID
		ActorID   pgtype.UUID
		LabelName string
	}{id, actorID, labelName})
	return f.addLabelErr
}

func (f *fakeIssueService) RemoveIssueLabel(_ context.Context, id pgtype.UUID, actorID pgtype.UUID, labelName string, _ facade.ChannelMutationContext) error {
	f.removeLabel = append(f.removeLabel, struct {
		ID        pgtype.UUID
		ActorID   pgtype.UUID
		LabelName string
	}{id, actorID, labelName})
	return f.removeLabelErr
}

func (f *fakeIssueService) ListMyTodos(_ context.Context, wsID, userID pgtype.UUID) ([]facade.Issue, error) {
	f.listTodos = append(f.listTodos, struct {
		WorkspaceID pgtype.UUID
		UserID      pgtype.UUID
	}{wsID, userID})
	return f.listTodosReturn, f.listTodosErr
}

type fakeCommentService struct {
	added     []facade.AddCommentReq
	addReturn facade.Comment
	addErr    error
}

func (f *fakeCommentService) AddComment(_ context.Context, req facade.AddCommentReq) (facade.Comment, error) {
	f.added = append(f.added, req)
	return f.addReturn, f.addErr
}

type recordingChannel struct {
	name    string
	sends   []port.OutboundMessage
	sendErr error
}

func (r *recordingChannel) Name() string                       { return r.name }
func (r *recordingChannel) Connect(_ context.Context) error    { return nil }
func (r *recordingChannel) Disconnect(_ context.Context) error { return nil }
func (r *recordingChannel) Events() <-chan port.InboundEvent   { return nil }
func (r *recordingChannel) GetChatInfo(_ context.Context, _ string) (port.ChatInfo, error) {
	return port.ChatInfo{}, nil
}
func (r *recordingChannel) GetUserInfo(_ context.Context, _ string) (port.UserInfo, error) {
	return port.UserInfo{}, nil
}
func (r *recordingChannel) Send(_ context.Context, msg port.OutboundMessage) (port.SendResult, error) {
	r.sends = append(r.sends, msg)
	return port.SendResult{PlatformMessageID: "msg-1"}, r.sendErr
}
func (r *recordingChannel) SendCard(_ context.Context, _ port.OutboundCardMessage) (port.SendResult, error) {
	return port.SendResult{}, channel.ErrNotImplemented
}

func uuid(tag byte) pgtype.UUID {
	var u pgtype.UUID
	for i := range u.Bytes {
		u.Bytes[i] = tag
	}
	u.Valid = true
	return u
}

func buildDispatchConfig() (inbound.DispatchConfig, *fakeIssueService, *fakeCommentService, *recordingChannel) {
	issueSvc := &fakeIssueService{}
	commentSvc := &fakeCommentService{}
	recCh := &recordingChannel{name: "feishu"}
	reg := channel.NewRegistry()
	_ = reg.Register(recCh)

	cfg := inbound.DispatchConfig{
		IssueFacade:   facade.NewIssueFacade(issueSvc),
		CommentFacade: facade.NewCommentFacade(commentSvc),
		Gateway:       gateway.NewRegistryGateway(reg),
		ChatBinding:   &fakeChatBinding{wsID: uuid(0x01)},
		UserResolver:  &fakeUserResolver{user: inbound.ResolvedUser{MulticaUserID: uuid(0x02), DisplayName: "测试用户"}},
	}
	return cfg, issueSvc, commentSvc, recCh
}

func makeEvt(intentKind port.IntentKind, params map[string]string) port.InboundEvent {
	return port.InboundEvent{
		ChannelName: "feishu",
		EventID:     "evt-1",
		ChatID:      "chat-1",
		SenderID:    "ou_sender1",
		Text:        "some text",
		Intent: port.InboundIntent{
			Kind:       intentKind,
			Confidence: 1,
			Params:     params,
			Source:     port.SourceRule,
		},
	}
}

// ---------------------------------------------------------------------------
// TC-intent-2: Unsupported ops return UNSUPPORTED_OP template
// ---------------------------------------------------------------------------

func TestDispatchStep_UnsupportedOp_ReturnsUnsupportedTemplate(t *testing.T) {
	t.Parallel()

	cfg, _, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentUnsupported, map[string]string{"issue_key": "STA-2"})
	out, d, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if d != inbound.DecisionContinue {
		t.Errorf("decision = %v, want Continue", d)
	}
	if out.EventID != "evt-1" {
		t.Error("event was not returned unchanged")
	}
	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(recCh.sends))
	}
	if !strings.Contains(recCh.sends[0].Text, "UNSUPPORTED_OP") {
		t.Errorf("reply missing UNSUPPORTED_OP key: %q", recCh.sends[0].Text)
	}
	if !strings.Contains(recCh.sends[0].Text, "Web 端") {
		t.Errorf("reply should mention Web 端: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// TC-int-2: Create issue happy path
// ---------------------------------------------------------------------------

func TestDispatchStep_CreateIssue_HappyPath(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.createReturn = facade.Issue{
		ID:          uuid(0xAA),
		WorkspaceID: uuid(0x01),
		Identifier:  "STA-39",
		Title:       "登录页加载慢",
		Status:      "todo",
	}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{"title": "登录页加载慢"})
	_, d, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if d != inbound.DecisionContinue {
		t.Errorf("decision = %v, want Continue", d)
	}

	if len(issueSvc.created) != 1 {
		t.Fatalf("expected 1 CreateIssue call, got %d", len(issueSvc.created))
	}
	call := issueSvc.created[0]
	if call.Title != "登录页加载慢" {
		t.Errorf("title = %q, want %q", call.Title, "登录页加载慢")
	}
	if call.WorkspaceID != uuid(0x01) {
		t.Error("workspace ID mismatch")
	}
	if call.ActorID != uuid(0x02) {
		t.Error("actor ID mismatch")
	}

	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(recCh.sends))
	}
	if !strings.Contains(recCh.sends[0].Text, "ISSUE_CREATED") {
		t.Errorf("reply missing ISSUE_CREATED: %q", recCh.sends[0].Text)
	}
	if !strings.Contains(recCh.sends[0].Text, "STA-39") {
		t.Errorf("reply should contain identifier STA-39: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// TC-intent-4: Missing param
// ---------------------------------------------------------------------------

func TestDispatchStep_CreateIssue_MissingTitle_ReturnsMissingParam(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(issueSvc.created) != 0 {
		t.Errorf("CreateIssue called %d times, want 0", len(issueSvc.created))
	}

	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(recCh.sends))
	}
	if !strings.Contains(recCh.sends[0].Text, "MISSING_PARAM") {
		t.Errorf("reply missing MISSING_PARAM: %q", recCh.sends[0].Text)
	}
}

func TestDispatchStep_CreateIssue_ProjectOutsideWorkspaceRejected(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	cfg.ProjectValidator = &fakeProjectValidator{err: pgx.ErrNoRows}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{
		"title":      "登录页加载慢",
		"project_id": "11111111-1111-1111-1111-111111111111",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(issueSvc.created) != 0 {
		t.Fatalf("CreateIssue called %d times, want 0", len(issueSvc.created))
	}
	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(recCh.sends))
	}
	if !strings.Contains(recCh.sends[0].Text, "MISSING_PARAM") {
		t.Errorf("reply missing MISSING_PARAM: %q", recCh.sends[0].Text)
	}
}

func TestDispatchStep_CreateIssue_ProjectIDRequiresValidator(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{
		"title":      "登录页加载慢",
		"project_id": "11111111-1111-1111-1111-111111111111",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return infrastructure error")
	}
	if !strings.Contains(err.Error(), "project validator is not configured") {
		t.Fatalf("error = %q, want missing project validator", err.Error())
	}
	if len(issueSvc.created) != 0 {
		t.Fatalf("CreateIssue called %d times, want 0", len(issueSvc.created))
	}
	if len(recCh.sends) != 0 {
		t.Fatalf("expected no send on retryable error, got %d", len(recCh.sends))
	}
}

func TestDispatchStep_CreateIssue_TypedNilProjectValidatorReturnsInfrastructureError(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	var validator *inbound.DBProjectWorkspaceValidator
	cfg.ProjectValidator = validator
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{
		"title":      "登录页加载慢",
		"project_id": "11111111-1111-1111-1111-111111111111",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return infrastructure error")
	}
	if !strings.Contains(err.Error(), "project validator is not configured") {
		t.Fatalf("error = %q, want missing project validator", err.Error())
	}
	if len(issueSvc.created) != 0 {
		t.Fatalf("CreateIssue called %d times, want 0", len(issueSvc.created))
	}
	if len(recCh.sends) != 0 {
		t.Fatalf("expected no send on retryable error, got %d", len(recCh.sends))
	}
}

func TestDispatchStep_CreateIssue_ProjectIDValidatedAndPassedThrough(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	validator := &fakeProjectValidator{}
	cfg.ProjectValidator = validator
	issueSvc.createReturn = facade.Issue{
		ID:         uuid(0xBC),
		Identifier: "STA-41",
		Title:      "登录页加载慢",
		Status:     "todo",
	}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{
		"title":      "登录页加载慢",
		"project_id": "11111111-1111-1111-1111-111111111111",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(issueSvc.created) != 1 {
		t.Fatalf("CreateIssue called %d times, want 1", len(issueSvc.created))
	}
	if len(validator.calls) != 1 {
		t.Fatalf("ValidateProjectInWorkspace called %d times, want 1", len(validator.calls))
	}
	if got := validator.calls[0].WorkspaceID; got != uuid(0x01) {
		t.Fatalf("validated workspace ID = %v, want chat workspace id", got)
	}
	if got := validator.calls[0].ProjectID; got != uuid(0x11) {
		t.Fatalf("validated project ID = %v, want parsed project id", got)
	}
	if got := issueSvc.created[0].ProjectID; got != uuid(0x11) {
		t.Fatalf("ProjectID = %v, want parsed project id", got)
	}
	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(recCh.sends))
	}
}

// ---------------------------------------------------------------------------
// TC-intent-4: Multi-intent IGNORED_SUFFIX
// ---------------------------------------------------------------------------

func TestDispatchStep_IgnoredSuffix_AppendsIgnoredTemplate(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.createReturn = facade.Issue{
		ID:         uuid(0xBB),
		Identifier: "STA-40",
		Title:      "登录页慢",
		Status:     "todo",
	}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{
		"title":           "登录页慢",
		"_ignored_suffix": "@ 老王",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(recCh.sends))
	}
	if !strings.Contains(recCh.sends[0].Text, "IGNORED_SUFFIX") {
		t.Errorf("reply missing IGNORED_SUFFIX: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// AddComment happy path
// ---------------------------------------------------------------------------

func TestDispatchStep_AddComment_HappyPath(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, commentSvc, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{ID: uuid(0x30), Identifier: "STA-2", Title: "t", Status: "todo"}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentAddComment, map[string]string{
		"issue_key": "STA-2",
		"comment":   "已找产品确认",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(issueSvc.gotByID) != 1 {
		t.Fatalf("expected 1 GetIssueByIdentifier, got %d", len(issueSvc.gotByID))
	}
	if issueSvc.gotByID[0].Identifier != "STA-2" {
		t.Errorf("identifier = %q, want STA-2", issueSvc.gotByID[0].Identifier)
	}

	if len(commentSvc.added) != 1 {
		t.Fatalf("expected 1 AddComment, got %d", len(commentSvc.added))
	}
	if commentSvc.added[0].Content != "已找产品确认" {
		t.Errorf("content = %q", commentSvc.added[0].Content)
	}

	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(recCh.sends))
	}
	if !strings.Contains(recCh.sends[0].Text, "COMMENT_ADDED") {
		t.Errorf("reply missing COMMENT_ADDED: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// AddComment missing params
// ---------------------------------------------------------------------------

func TestDispatchStep_AddComment_MissingParams(t *testing.T) {
	t.Parallel()

	cfg, _, commentSvc, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentAddComment, map[string]string{"issue_key": "STA-2"})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(commentSvc.added) != 0 {
		t.Error("AddComment should not be called with missing params")
	}
	if !strings.Contains(recCh.sends[0].Text, "MISSING_PARAM") {
		t.Errorf("reply missing MISSING_PARAM: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// AddComment: issue not found
// ---------------------------------------------------------------------------

func TestDispatchStep_AddComment_IssueNotFound(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierErr = errors.New("not found")
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentAddComment, map[string]string{
		"issue_key": "STA-999",
		"comment":   "test",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(recCh.sends[0].Text, "ISSUE_NOT_FOUND") {
		t.Errorf("reply missing ISSUE_NOT_FOUND: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// Rec-1: AddComment with invalid identifier format
// ---------------------------------------------------------------------------

func TestDispatchStep_AddComment_InvalidIdentifier_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, commentSvc, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	for _, badKey := range []string{"abc-def", "!!-??", "STA-0", "STA-", "-39", "中文-哈哈"} {
		evt := makeEvt(port.IntentAddComment, map[string]string{
			"issue_key": badKey,
			"comment":   "test",
		})
		_, _, err := step.Run(context.Background(), evt)
		if err != nil {
			t.Fatalf("Run(%q): %v", badKey, err)
		}
		if len(issueSvc.gotByID) != 0 {
			t.Errorf("GetIssueByIdentifier should not be called for %q", badKey)
		}
		if len(commentSvc.added) != 0 {
			t.Errorf("AddComment should not be called for %q", badKey)
		}
		if !strings.Contains(recCh.sends[len(recCh.sends)-1].Text, "ISSUE_NOT_FOUND") {
			t.Errorf("reply missing ISSUE_NOT_FOUND for %q: %q", badKey, recCh.sends[len(recCh.sends)-1].Text)
		}
	}
}

// ---------------------------------------------------------------------------
// SetStatus happy path
// ---------------------------------------------------------------------------

func TestDispatchStep_SetStatus_HappyPath(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{ID: uuid(0x40), Identifier: "STA-7", Title: "t", Status: "todo"}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetStatus, map[string]string{
		"issue_key": "STA-7",
		"status":    "done",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(issueSvc.setStatus) != 1 {
		t.Fatalf("expected 1 SetIssueStatus, got %d", len(issueSvc.setStatus))
	}
	call := issueSvc.setStatus[0]
	if call.Status != "done" {
		t.Errorf("status = %q, want done", call.Status)
	}
	if call.ActorID != uuid(0x02) {
		t.Error("actor ID mismatch")
	}

	if !strings.Contains(recCh.sends[0].Text, "STATUS_CHANGED") {
		t.Errorf("reply missing STATUS_CHANGED: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// SetStatus missing params
// ---------------------------------------------------------------------------

func TestDispatchStep_SetStatus_MissingParams(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetStatus, map[string]string{"issue_key": "STA-2"})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(issueSvc.setStatus) != 0 {
		t.Error("SetIssueStatus should not be called")
	}
	if !strings.Contains(recCh.sends[0].Text, "MISSING_PARAM") {
		t.Errorf("reply missing MISSING_PARAM: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// SetAssignee happy path
// ---------------------------------------------------------------------------

func TestDispatchStep_SetAssignee_HappyPath(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{ID: uuid(0x40), Identifier: "STA-7", Title: "t", Status: "todo"}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetAssignee, map[string]string{
		"issue_key": "STA-7",
		"assignee":  "@张三",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(issueSvc.setAssignee) != 1 {
		t.Fatalf("expected 1 SetIssueAssignee, got %d", len(issueSvc.setAssignee))
	}
	call := issueSvc.setAssignee[0]
	if call.AssigneeIdentifier != "@张三" {
		t.Errorf("assignee = %q, want @张三", call.AssigneeIdentifier)
	}
	if call.ActorID != uuid(0x02) {
		t.Error("actor ID mismatch")
	}

	if !strings.Contains(recCh.sends[0].Text, "ASSIGNEE_CHANGED") {
		t.Errorf("reply missing ASSIGNEE_CHANGED: %q", recCh.sends[0].Text)
	}
}

func TestDispatchStep_SetAssignee_MissingParams(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetAssignee, map[string]string{"issue_key": "STA-2"})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(issueSvc.setAssignee) != 0 {
		t.Error("SetIssueAssignee should not be called")
	}
	if !strings.Contains(recCh.sends[0].Text, "MISSING_PARAM") {
		t.Errorf("reply missing MISSING_PARAM: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// SetPriority happy path
// ---------------------------------------------------------------------------

func TestDispatchStep_SetPriority_HappyPath(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{ID: uuid(0x41), Identifier: "STA-8", Title: "t", Status: "todo"}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetPriority, map[string]string{
		"issue_key": "STA-8",
		"priority":  "high",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(issueSvc.setPriority) != 1 {
		t.Fatalf("expected 1 SetIssuePriority, got %d", len(issueSvc.setPriority))
	}
	call := issueSvc.setPriority[0]
	if call.Priority != "high" {
		t.Errorf("priority = %q, want high", call.Priority)
	}
	if call.ActorID != uuid(0x02) {
		t.Error("actor ID mismatch")
	}

	if !strings.Contains(recCh.sends[0].Text, "PRIORITY_CHANGED") {
		t.Errorf("reply missing PRIORITY_CHANGED: %q", recCh.sends[0].Text)
	}
}

func TestDispatchStep_SetPriority_MissingParams(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetPriority, map[string]string{"issue_key": "STA-2"})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(issueSvc.setPriority) != 0 {
		t.Error("SetIssuePriority should not be called")
	}
	if !strings.Contains(recCh.sends[0].Text, "MISSING_PARAM") {
		t.Errorf("reply missing MISSING_PARAM: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// SetLabel happy path (add)
// ---------------------------------------------------------------------------

func TestDispatchStep_SetLabel_Add_HappyPath(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{ID: uuid(0x42), Identifier: "STA-9", Title: "t", Status: "todo"}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetLabel, map[string]string{
		"issue_key": "STA-9",
		"label":     "bug",
		"op":        "add",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(issueSvc.addLabel) != 1 {
		t.Fatalf("expected 1 AddIssueLabel, got %d", len(issueSvc.addLabel))
	}
	call := issueSvc.addLabel[0]
	if call.LabelName != "bug" {
		t.Errorf("label = %q, want bug", call.LabelName)
	}
	if call.ActorID != uuid(0x02) {
		t.Error("actor ID mismatch")
	}

	if !strings.Contains(recCh.sends[0].Text, "LABEL_ADDED") {
		t.Errorf("reply missing LABEL_ADDED: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// SetLabel happy path (remove)
// ---------------------------------------------------------------------------

func TestDispatchStep_SetLabel_Remove_HappyPath(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{ID: uuid(0x43), Identifier: "STA-10", Title: "t", Status: "todo"}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetLabel, map[string]string{
		"issue_key": "STA-10",
		"label":     "bug",
		"op":        "remove",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(issueSvc.removeLabel) != 1 {
		t.Fatalf("expected 1 RemoveIssueLabel, got %d", len(issueSvc.removeLabel))
	}
	call := issueSvc.removeLabel[0]
	if call.LabelName != "bug" {
		t.Errorf("label = %q, want bug", call.LabelName)
	}
	if call.ActorID != uuid(0x02) {
		t.Error("actor ID mismatch")
	}

	if !strings.Contains(recCh.sends[0].Text, "LABEL_REMOVED") {
		t.Errorf("reply missing LABEL_REMOVED: %q", recCh.sends[0].Text)
	}
}

func TestDispatchStep_SetLabel_MissingParams(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetLabel, map[string]string{"issue_key": "STA-2"})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(issueSvc.addLabel) != 0 {
		t.Error("AddIssueLabel should not be called")
	}
	if !strings.Contains(recCh.sends[0].Text, "MISSING_PARAM") {
		t.Errorf("reply missing MISSING_PARAM: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// Business error propagation: facade returns error → INTERNAL_ERROR reply
// ---------------------------------------------------------------------------

func TestDispatchStep_SetAssignee_FacadeError_ReturnsInternalError(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{ID: uuid(0x40), Identifier: "STA-7", Title: "t", Status: "todo"}
	issueSvc.setAssigneeErr = errors.New("用户 @张三 不在此 workspace")
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetAssignee, map[string]string{
		"issue_key": "STA-7",
		"assignee":  "@张三",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return infrastructure error")
	}
	if len(recCh.sends) != 0 {
		t.Fatalf("expected no send on retryable error, got %d", len(recCh.sends))
	}
}

func TestDispatchStep_SetPriority_FacadeError_ReturnsInternalError(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{ID: uuid(0x41), Identifier: "STA-8", Title: "t", Status: "todo"}
	issueSvc.setPriorityErr = errors.New("优先级仅支持 urgent/high/medium/low/none")
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetPriority, map[string]string{
		"issue_key": "STA-8",
		"priority":  "invalid",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return infrastructure error")
	}
	if len(recCh.sends) != 0 {
		t.Fatalf("expected no send on retryable error, got %d", len(recCh.sends))
	}
}

func TestDispatchStep_SetLabel_FacadeError_ReturnsInternalError(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{ID: uuid(0x42), Identifier: "STA-9", Title: "t", Status: "todo"}
	issueSvc.addLabelErr = errors.New("标签 bug 不存在，请先在 Web 端创建")
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentSetLabel, map[string]string{
		"issue_key": "STA-9",
		"label":     "bug",
		"op":        "add",
	})
	_, _, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return infrastructure error")
	}
	if len(recCh.sends) != 0 {
		t.Fatalf("expected no send on retryable error, got %d", len(recCh.sends))
	}
}

// ---------------------------------------------------------------------------
// QueryIssue — specific issue
// ---------------------------------------------------------------------------

func TestDispatchStep_QueryIssue_SpecificIssue(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.getByIdentifierRet = facade.Issue{
		ID:         uuid(0x50),
		Identifier: "STA-2",
		Title:      "登录页加载慢",
		Status:     "in_progress",
	}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentQueryIssue, map[string]string{"issue_key": "STA-2"})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(recCh.sends))
	}
	text := recCh.sends[0].Text
	if !strings.Contains(text, "in_progress") {
		t.Errorf("reply should contain status: %q", text)
	}
	if !strings.Contains(text, "登录页加载慢") {
		t.Errorf("reply should contain title: %q", text)
	}
	if !strings.Contains(text, "测试用户") {
		t.Errorf("reply should contain display name: %q", text)
	}
}

// ---------------------------------------------------------------------------
// QueryIssue — "我的待办"
// ---------------------------------------------------------------------------

func TestDispatchStep_QueryIssue_MyTodos(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.listTodosReturn = []facade.Issue{
		{ID: uuid(0x60), Identifier: "STA-10", Title: "Issue A", Status: "todo"},
		{ID: uuid(0x61), Identifier: "STA-11", Title: "Issue B", Status: "in_progress"},
	}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentQueryIssue, map[string]string{})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(recCh.sends))
	}
	text := recCh.sends[0].Text
	if !strings.Contains(text, "待办") {
		t.Errorf("reply should mention 待办: %q", text)
	}
}

// ---------------------------------------------------------------------------
// QueryIssue — empty todos
// ---------------------------------------------------------------------------

func TestDispatchStep_QueryIssue_NoTodos(t *testing.T) {
	t.Parallel()

	cfg, _, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentQueryIssue, map[string]string{})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !strings.Contains(recCh.sends[0].Text, "没有待办") {
		t.Errorf("reply should say no todos: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// Unknown intent
// ---------------------------------------------------------------------------

func TestDispatchStep_UnknownIntent_ReturnsUnknownTemplate(t *testing.T) {
	t.Parallel()

	cfg, _, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentUnknown, map[string]string{})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(recCh.sends[0].Text, "UNKNOWN") {
		t.Errorf("reply missing UNKNOWN: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// ASK_CLARIFY intent
// ---------------------------------------------------------------------------

func TestDispatchStep_ASKClarify_ReturnsAskClarifyTemplate(t *testing.T) {
	t.Parallel()

	cfg, _, _, recCh := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentASKClarify, map[string]string{})
	_, _, err := step.Run(context.Background(), evt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(recCh.sends[0].Text, "ASK_CLARIFY") {
		t.Errorf("reply missing ASK_CLARIFY: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// Facade error → INTERNAL_ERROR
// ---------------------------------------------------------------------------

func TestDispatchStep_CreateIssue_FacadeError_ReturnsInternalError(t *testing.T) {
	t.Parallel()

	cfg, issueSvc, _, recCh := buildDispatchConfig()
	issueSvc.createErr = errors.New("db down")
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{"title": "test"})
	_, d, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return infrastructure error")
	}
	if d != inbound.DecisionContinue {
		t.Errorf("decision = %v, want Continue", d)
	}
	if len(recCh.sends) != 0 {
		t.Fatalf("expected no send on retryable error, got %d", len(recCh.sends))
	}
}

// ---------------------------------------------------------------------------
// Send failure does not abort pipeline
// ---------------------------------------------------------------------------

func TestDispatchStep_SendFailure_DoesNotAbortPipeline(t *testing.T) {
	t.Parallel()

	cfg, _, _, recCh := buildDispatchConfig()
	recCh.sendErr = errors.New("network timeout")
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentUnknown, map[string]string{})
	_, d, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return send error")
	}
	if d != inbound.DecisionContinue {
		t.Errorf("decision = %v, want Continue", d)
	}
}

// ---------------------------------------------------------------------------
// Channel not in registry — does not abort
// ---------------------------------------------------------------------------

func TestDispatchStep_ChannelNotInRegistry_DoesNotAbort(t *testing.T) {
	t.Parallel()

	cfg := inbound.DispatchConfig{
		IssueFacade:   facade.NewIssueFacade(&fakeIssueService{}),
		CommentFacade: facade.NewCommentFacade(&fakeCommentService{}),
		Gateway:       gateway.NewRegistryGateway(channel.NewRegistry()),
		ChatBinding:   &fakeChatBinding{wsID: uuid(0x01)},
		UserResolver:  &fakeUserResolver{user: inbound.ResolvedUser{MulticaUserID: uuid(0x02)}},
	}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentUnknown, map[string]string{})
	_, d, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return missing channel error")
	}
	if d != inbound.DecisionContinue {
		t.Errorf("decision = %v, want Continue", d)
	}
}

// ---------------------------------------------------------------------------
// ChatBindingLookup error → INTERNAL_ERROR
// ---------------------------------------------------------------------------

func TestDispatchStep_ChatBindingError_ReturnsInternalError(t *testing.T) {
	t.Parallel()

	cfg, _, _, recCh := buildDispatchConfig()
	cfg.ChatBinding = &fakeChatBinding{err: errors.New("db error")}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{"title": "test"})
	_, _, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return infrastructure error")
	}
	if len(recCh.sends) != 0 {
		t.Fatalf("expected no send on retryable error, got %d", len(recCh.sends))
	}
}

// ---------------------------------------------------------------------------
// UserResolver error → INTERNAL_ERROR
// ---------------------------------------------------------------------------

func TestDispatchStep_UserResolverError_ReturnsInternalError(t *testing.T) {
	t.Parallel()

	cfg, _, _, recCh := buildDispatchConfig()
	cfg.UserResolver = &fakeUserResolver{err: errors.New("binding not found")}
	step := inbound.NewDispatchStep(cfg)

	evt := makeEvt(port.IntentCreateIssue, map[string]string{"title": "test"})
	_, _, err := step.Run(context.Background(), evt)
	if err == nil {
		t.Fatal("Run should return infrastructure error")
	}
	if len(recCh.sends) != 0 {
		t.Fatalf("expected no send on retryable error, got %d", len(recCh.sends))
	}
}

// ---------------------------------------------------------------------------
// Pipeline integration
// ---------------------------------------------------------------------------

func TestDispatchStep_InPipeline_AllPlaceholderStepsRunInOrder(t *testing.T) {
	t.Parallel()

	store := &fakeDedupStore{responses: []dedupResp{{Inserted: true}}}
	cfg, _, _, recCh := buildDispatchConfig()

	p := inbound.NewPipeline(
		inbound.NewNormalizeStep(),
		inbound.NewDedupStep(store),
		inbound.NewIdentityBindStep(),
		inbound.NewIntentRecogStep(),
		inbound.NewDispatchStep(cfg),
		inbound.NewReplyStep(),
	)
	out, err := p.Run(context.Background(), port.InboundEvent{
		ChannelName: "feishu",
		EventID:     "evt-pipeline",
		ChatID:      "chat-1",
		SenderID:    "sender-1",
		Type:        port.EventTypeMessageReceived,
		Text:        "hello",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Terminal != "reply" {
		t.Errorf("Terminal = %q, want reply", out.Terminal)
	}
	if out.Decision != inbound.DecisionContinue {
		t.Errorf("Decision = %v, want Continue", out.Decision)
	}
	if len(recCh.sends) != 1 {
		t.Fatalf("expected 1 send from dispatch, got %d", len(recCh.sends))
	}
	if !strings.Contains(recCh.sends[0].Text, "UNKNOWN") {
		t.Errorf("pipeline reply should contain UNKNOWN: %q", recCh.sends[0].Text)
	}
}

// ---------------------------------------------------------------------------
// Rec-2: validIdentifierFormat unit tests
// ---------------------------------------------------------------------------

func TestValidIdentifierFormat(t *testing.T) {
	t.Parallel()

	valid := []string{"STA-2", "STA-39", "MUL-123", "ABCDE-1", "AB-999999"}
	for _, s := range valid {
		if !inbound.ValidIdentifierFormat(s) {
			t.Errorf("ValidIdentifierFormat(%q) = false, want true", s)
		}
	}

	invalid := []string{
		"abc-def",  // lowercase
		"!!-??",    // special chars
		"STA-0",    // leading zero
		"STA-",     // missing number
		"-39",      // missing prefix
		"中文-哈哈",    // non-ASCII
		"A-1",      // prefix too short
		"ABCDEF-1", // prefix too long
		"",         // empty
		"STA",      // no hyphen
	}
	for _, s := range invalid {
		if inbound.ValidIdentifierFormat(s) {
			t.Errorf("ValidIdentifierFormat(%q) = true, want false", s)
		}
	}
}

// ---------------------------------------------------------------------------
// Name()
// ---------------------------------------------------------------------------

func TestDispatchStep_Name(t *testing.T) {
	t.Parallel()

	cfg, _, _, _ := buildDispatchConfig()
	step := inbound.NewDispatchStep(cfg)
	if got := step.Name(); got != "dispatch" {
		t.Errorf("Name = %q, want %q", got, "dispatch")
	}
}
