package outbound

import (
	"context"
	"encoding/json"
	"fmt"

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
	"comment_mention": "comment_mention",
	"issue_assigned":  "issue_assigned",
	"issue_mention":   "issue_mention",
}

func (s *DBPrefStore) GetChannelPref(ctx context.Context, workspaceID, userID pgtype.UUID, channelName, eventKind string) (bool, error) {
	row, err := s.queries.GetNotificationPreference(ctx, db.GetNotificationPreferenceParams{
		WorkspaceID: workspaceID,
		UserID:      userID,
	})
	if err != nil {
		// No preference row → default enabled
		return true, nil
	}

	var prefs map[string]any
	if err := json.Unmarshal(row.Preferences, &prefs); err != nil {
		return true, nil // malformed → default enabled
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
		return true, nil // unknown event kind → default enabled
	}

	val, ok := chPrefs[jsonKey]
	if !ok {
		return true, nil // key absent → default enabled
	}

	// "muted" → disabled; anything else → enabled
	strVal, ok := val.(string)
	if !ok {
		return true, nil
	}
	return strVal != "muted", nil
}

// parsePrefPath is a helper for extracting a nested JSONB path.
// Not used in the main flow but useful for testing.
func parsePrefPath(preferences []byte, path ...string) (any, error) {
	var m map[string]any
	if err := json.Unmarshal(preferences, &m); err != nil {
		return nil, fmt.Errorf("unmarshal preferences: %w", err)
	}
	current := any(m)
	for _, key := range path {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("path %v: not an object", path)
		}
		current = obj[key]
	}
	return current, nil
}
