package replyctx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultTTL is the default expiration duration for reply contexts.
const DefaultTTL = 30 * time.Minute

type Context struct {
	ConnectionID   string
	ExternalUserID string
	ChatID         string
	// TODO(STA-78-P1): wire thread_id for thread-level isolation
	ThreadID        string
	WorkspaceID     pgtype.UUID
	IssueID         pgtype.UUID
	IssueIdentifier string
	IssueTitle      string
	InboxItemID     pgtype.UUID
	ExpiresAt       time.Time
}

type Store interface {
	Upsert(ctx context.Context, item Context) error
	Lookup(ctx context.Context, connectionID, externalUserID, chatID string, now time.Time) (Context, bool, error)
	Clear(ctx context.Context, connectionID, externalUserID string) error
	DeleteExpired(ctx context.Context, before time.Time) (int64, error)
}

type DBStore struct {
	pool *pgxpool.Pool
}

func NewDBStore(pool *pgxpool.Pool) *DBStore {
	return &DBStore{pool: pool}
}

func (s *DBStore) Upsert(ctx context.Context, item Context) error {
	if s == nil || s.pool == nil {
		return errors.New("reply context store is not configured")
	}
	if item.ConnectionID == "" || item.ExternalUserID == "" || !item.WorkspaceID.Valid || !item.IssueID.Valid || item.ExpiresAt.IsZero() {
		return errors.New("reply context: invalid context")
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO channel_reply_context (
    connection_id, external_user_id, chat_id, workspace_id, issue_id,
    issue_identifier, issue_title, inbox_item_id, expires_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (connection_id, external_user_id, chat_id) DO UPDATE SET
    workspace_id = EXCLUDED.workspace_id,
    issue_id = EXCLUDED.issue_id,
    issue_identifier = EXCLUDED.issue_identifier,
    issue_title = EXCLUDED.issue_title,
    inbox_item_id = EXCLUDED.inbox_item_id,
    expires_at = EXCLUDED.expires_at,
    updated_at = now()
`, item.ConnectionID, item.ExternalUserID, item.ChatID, item.WorkspaceID, item.IssueID,
		item.IssueIdentifier, item.IssueTitle, item.InboxItemID, item.ExpiresAt)
	return err
}

func (s *DBStore) Lookup(ctx context.Context, connectionID, externalUserID, chatID string, now time.Time) (Context, bool, error) {
	if s == nil || s.pool == nil {
		return Context{}, false, errors.New("reply context store is not configured")
	}
	if now.IsZero() {
		now = time.Now()
	}
	var item Context
	err := s.pool.QueryRow(ctx, `
SELECT connection_id, external_user_id, chat_id, workspace_id, issue_id,
       issue_identifier, issue_title, inbox_item_id, expires_at
FROM channel_reply_context
WHERE connection_id = $1
  AND external_user_id = $2
  AND chat_id = $3
  AND expires_at > $4
`, connectionID, externalUserID, chatID, now).Scan(
		&item.ConnectionID,
		&item.ExternalUserID,
		&item.ChatID,
		&item.WorkspaceID,
		&item.IssueID,
		&item.IssueIdentifier,
		&item.IssueTitle,
		&item.InboxItemID,
		&item.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Context{}, false, nil
	}
	if err != nil {
		return Context{}, false, fmt.Errorf("lookup reply context: %w", err)
	}
	return item, true, nil
}

func (s *DBStore) Clear(ctx context.Context, connectionID, externalUserID string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
DELETE FROM channel_reply_context
WHERE connection_id = $1 AND external_user_id = $2
`, connectionID, externalUserID)
	return err
}

func (s *DBStore) DeleteExpired(ctx context.Context, before time.Time) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
DELETE FROM channel_reply_context
WHERE expires_at < $1
`, before)
	if err != nil {
		return 0, fmt.Errorf("delete expired reply contexts: %w", err)
	}
	return tag.RowsAffected(), nil
}

var _ Store = (*DBStore)(nil)
