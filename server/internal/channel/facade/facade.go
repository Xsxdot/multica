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
	ListMyTodos(ctx context.Context, workspaceID, userID pgtype.UUID) ([]Issue, error)
}

// CommentFacade is the channel-layer entry point for comment operations.
type CommentFacade interface {
	AddComment(ctx context.Context, req AddCommentReq) (Comment, error)
}

// issueFacade is the unexported concrete implementation. Callers wire it via
// NewIssueFacade and only ever see the IssueFacade interface — see the
// Green-phase guidance in Orion's Red review (item 2: "构造函数返回 interface
// 而非具体类型").
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

func (f *issueFacade) ListMyTodos(ctx context.Context, workspaceID, userID pgtype.UUID) ([]Issue, error) {
	return f.svc.ListMyTodos(ctx, workspaceID, userID)
}

// commentFacade is the unexported concrete implementation for CommentFacade.
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
