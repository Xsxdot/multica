// Package facade is the single-direction outlet from the channel layer
// (adapter / inbound / outbound / intent / binding) into Multica's existing
// services. Per DESIGN §3.2 the channel/facade package is the ONLY place
// channel-layer code is allowed to reach toward issue / comment behaviour;
// it must NOT import the persistence layer (pkg/db). The AST-level test in
// import_test.go enforces that boundary.
//
// The package is intentionally a thin shell: it defines its own DTOs and
// service interfaces, and delegates to caller-injected implementations. No
// business logic lives here — see DESIGN §4.2 ("薄壳，调既有 service，不写业务").
package facade

import "github.com/jackc/pgx/v5/pgtype"

// Issue is the facade-layer projection of an issue. It deliberately exposes
// only the fields the channel layer needs today (DESIGN §4.2 thin shell);
// new fields are added when a real caller demands them, not preemptively.
type Issue struct {
	ID          pgtype.UUID
	WorkspaceID pgtype.UUID
	Identifier  string
	Title       string
	Status      string
}

// Comment is the facade-layer projection of a comment. Same minimalism rule
// as Issue applies.
type Comment struct {
	ID      pgtype.UUID
	IssueID pgtype.UUID
	Content string
}

// CreateIssueReq carries the inputs CreateIssue needs from the channel layer.
// ActorID is the Multica user_id resolved from `channel_user_binding`; it is
// passed through verbatim so existing service-level permission checks stay
// the single source of truth (TC-facade-1).
type CreateIssueReq struct {
	WorkspaceID pgtype.UUID
	ActorID     pgtype.UUID
	Title       string
	Description string
}

// AddCommentReq carries the inputs AddComment needs from the channel layer.
// Content is forwarded verbatim — the facade does no sanitisation; the
// existing service layer's validation is the single source of truth
// (TC-facade-2 / PRD E9).
type AddCommentReq struct {
	IssueID pgtype.UUID
	ActorID pgtype.UUID
	Content string
}
