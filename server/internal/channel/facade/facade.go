package facade

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

// IssueFacade is the channel-layer entry point for issue operations. It is
// the public contract every channel adapter / inbound handler / dispatcher
// uses to act on issues — no other path from channel/* into Multica's issue
// domain is permitted (DESIGN §3.2 single-direction dependency rule).
type IssueFacade interface {
	CreateIssue(ctx context.Context, req CreateIssueReq) (Issue, error)
	GetIssue(ctx context.Context, id pgtype.UUID) (Issue, error)
	GetIssueByIdentifier(ctx context.Context, workspaceID pgtype.UUID, identifier string) (Issue, error)
	SetIssueStatus(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, status string) error
	SetIssueAssignee(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, assigneeIdentifier string) error
	SetIssuePriority(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, priority string) error
	AddIssueLabel(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, labelName string) error
	RemoveIssueLabel(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, labelName string) error
	ListMyTodos(ctx context.Context, workspaceID, userID pgtype.UUID) ([]Issue, error)
}

// CommentFacade is the channel-layer entry point for comment operations.
// Same single-direction dependency contract as IssueFacade (DESIGN §3.2): the
// channel layer reaches comment behaviour exclusively through this interface
// and never touches the persistence layer directly.
type CommentFacade interface {
	AddComment(ctx context.Context, req AddCommentReq) (Comment, error)
}

// issueFacade is the unexported concrete implementation of IssueFacade. It is
// kept unexported so callers wire by the interface contract rather than the
// struct — that way the implementation can be swapped (e.g. for an
// instrumentation decorator, or a future direct-to-service binding) without
// updating any call site.
type issueFacade struct {
	svc IssueService
}

// NewIssueFacade returns an IssueFacade that delegates to svc. The return
// type is the interface, not *issueFacade, so callers depend on the contract
// rather than the struct.
func NewIssueFacade(svc IssueService) IssueFacade {
	return &issueFacade{svc: svc}
}

// CreateIssue forwards req to the underlying service verbatim. No field is
// mutated, validated, or sanitised at this layer — the service is the single
// source of truth for permission and input validation (TC-facade-1 /
// TC-facade-2).
func (f *issueFacade) CreateIssue(ctx context.Context, req CreateIssueReq) (Issue, error) {
	return f.svc.CreateIssue(ctx, req)
}

func (f *issueFacade) GetIssue(ctx context.Context, id pgtype.UUID) (Issue, error) {
	return f.svc.GetIssue(ctx, id)
}

func (f *issueFacade) GetIssueByIdentifier(ctx context.Context, workspaceID pgtype.UUID, identifier string) (Issue, error) {
	return f.svc.GetIssueByIdentifier(ctx, workspaceID, identifier)
}

func (f *issueFacade) SetIssueStatus(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, status string) error {
	return f.svc.SetIssueStatus(ctx, id, actorID, status)
}

// SetIssueAssignee forwards the assignee change to the underlying service.
// The assigneeIdentifier is resolved by the service layer (open_id or username).
func (f *issueFacade) SetIssueAssignee(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, assigneeIdentifier string) error {
	return f.svc.SetIssueAssignee(ctx, id, actorID, assigneeIdentifier)
}

// SetIssuePriority forwards the priority change to the underlying service.
// Valid values: urgent, high, medium, low, no_priority.
func (f *issueFacade) SetIssuePriority(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, priority string) error {
	return f.svc.SetIssuePriority(ctx, id, actorID, priority)
}

// AddIssueLabel forwards the label attachment to the underlying service.
// The labelName is resolved against the workspace's label library.
func (f *issueFacade) AddIssueLabel(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, labelName string) error {
	return f.svc.AddIssueLabel(ctx, id, actorID, labelName)
}

// RemoveIssueLabel forwards the label detachment to the underlying service.
func (f *issueFacade) RemoveIssueLabel(ctx context.Context, id pgtype.UUID, actorID pgtype.UUID, labelName string) error {
	return f.svc.RemoveIssueLabel(ctx, id, actorID, labelName)
}

func (f *issueFacade) ListMyTodos(ctx context.Context, workspaceID, userID pgtype.UUID) ([]Issue, error) {
	return f.svc.ListMyTodos(ctx, workspaceID, userID)
}

// commentFacade is the unexported concrete implementation of CommentFacade.
// See issueFacade for the rationale on keeping the type unexported.
type commentFacade struct {
	svc CommentService
}

// NewCommentFacade returns a CommentFacade that delegates to svc.
func NewCommentFacade(svc CommentService) CommentFacade {
	return &commentFacade{svc: svc}
}

// AddComment forwards req verbatim to the underlying service. Content is
// passed through unchanged — channel-layer code does NOT sanitise (PRD E9 /
// TC-facade-2).
func (f *commentFacade) AddComment(ctx context.Context, req AddCommentReq) (Comment, error) {
	return f.svc.AddComment(ctx, req)
}

// Compile-time interface conformance assertions. These give a clear compile
// error at the implementation site (rather than at every caller) if a method
// signature drifts from the interface.
var (
	_ IssueFacade   = (*issueFacade)(nil)
	_ CommentFacade = (*commentFacade)(nil)
)
