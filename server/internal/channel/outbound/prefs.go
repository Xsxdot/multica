package outbound

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type PrefStore interface {
	GetChannelPref(ctx context.Context, workspaceID, userID pgtype.UUID, channelName, eventKind string) (bool, error)
}

type DBPrefStore struct {
	queries *db.Queries
}

func NewDBPrefStore(q *db.Queries) *DBPrefStore {
	return &DBPrefStore{queries: q}
}

func (s *DBPrefStore) GetChannelPref(ctx context.Context, workspaceID, userID pgtype.UUID, channelName, eventKind string) (bool, error) {
	return true, nil
}
