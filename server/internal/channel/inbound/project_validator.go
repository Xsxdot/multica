package inbound

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type DBProjectWorkspaceValidator struct {
	queries *db.Queries
}

func NewDBProjectWorkspaceValidator(pool *pgxpool.Pool) *DBProjectWorkspaceValidator {
	return &DBProjectWorkspaceValidator{queries: db.New(pool)}
}

func (v *DBProjectWorkspaceValidator) ValidateProjectInWorkspace(ctx context.Context, workspaceID, projectID pgtype.UUID) error {
	if v == nil || v.queries == nil || !projectID.Valid {
		return nil
	}
	_, err := v.queries.GetProjectInWorkspace(ctx, db.GetProjectInWorkspaceParams{
		ID:          projectID,
		WorkspaceID: workspaceID,
	})
	return err
}

var _ ProjectWorkspaceValidator = (*DBProjectWorkspaceValidator)(nil)
