package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// PrefStore abstracts notification preference lookups so the subscriber
// can be tested without a real database.
type PrefStore interface {
	// GetChannelPref returns true if the given event kind is enabled for
	// the user on the specified channel. Returns true (enabled) when no
	// preference record exists (default = enabled).
	GetChannelPref(ctx context.Context, workspaceID, userID pgtype.UUID, channelName, eventKind string) (bool, error)
}

// DBPrefStore implements PrefStore using the sqlc-generated Queries.
type DBPrefStore struct {
	queries *db.Queries
}

// NewDBPrefStore creates a PrefStore backed by the database.
func NewDBPrefStore(q *db.Queries) *DBPrefStore {
	return &DBPrefStore{queries: q}
}

// prefKeyMap maps event kinds to the JSONB key path within the
// preferences -> channel -> <channel_name> object.
// The issue spec defines:
//
//	preferences -> 'channel' -> 'feishu' -> 'comment_mention' / 'issue_assigned' / 'issue_mention'
var prefKeyMap = map[string]string{
	"comment_mention":  "comment_mention",
	"issue_assigned":   "issue_assigned",
	"issue_mention":    "issue_mention",
	"status_in_review": "status_in_review",
	"status_done":      "status_done",
	"status_blocked":   "status_blocked",
}

// GetChannelPref returns true if the given event kind is enabled for the user
// on the specified channel.
// R3: Distinguishes ErrNoRows (-> default enabled) from real DB errors (-> fail-closed).
// json.Unmarshal failure returns error instead of swallowing.
func (s *DBPrefStore) GetChannelPref(ctx context.Context, workspaceID, userID pgtype.UUID, channelName, eventKind string) (bool, error) {
	row, err := s.queries.GetNotificationPreference(ctx, db.GetNotificationPreferenceParams{
		WorkspaceID: workspaceID,
		UserID:      userID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil // no preference row -> default enabled
	}
	if err != nil {
		return false, fmt.Errorf("get notification preference: %w", err)
	}

	var prefs map[string]any
	if err := json.Unmarshal(row.Preferences, &prefs); err != nil {
		return false, fmt.Errorf("unmarshal preferences: %w", err)
	}

	// Navigate: preferences -> channel -> <channelName> -> <eventKind>
	channelObj, ok := prefs["channel"].(map[string]any)
	if !ok {
		return true, nil
	}

	chPrefs, ok := channelObj[channelName].(map[string]any)
	if !ok {
		return true, nil
	}

	jsonKey, ok := prefKeyMap[eventKind]
	if !ok {
		return true, nil // unknown event kind -> default enabled
	}

	val, ok := chPrefs[jsonKey]
	if !ok {
		return true, nil // key absent -> default enabled
	}

	// "muted" -> disabled; anything else -> enabled
	strVal, ok := val.(string)
	if !ok {
		return true, nil
	}
	return strVal != "muted", nil
}
