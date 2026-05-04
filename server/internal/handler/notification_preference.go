package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/multica-ai/multica/server/internal/logger"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// validNotifGroups is the set of notification preference group keys that the
// API accepts. Keys not in this set are rejected.
var validNotifGroups = map[string]bool{
	"assignments":    true,
	"status_changes": true,
	"comments":       true,
	"updates":        true,
	"agent_activity": true,
}

// validNotifValues is the set of allowed preference values per group.
var validNotifValues = map[string]bool{
	"all":   true,
	"muted": true,
}

// validChannelKeys is the set of supported channel names in
// preferences.channel.*.
var validChannelKeys = map[string]bool{
	"feishu": true,
}

// validFeishuKeys is the set of boolean keys under
// preferences.channel.feishu.*.
var validFeishuKeys = map[string]bool{
	"issues":   true,
	"comments": true,
	"mentions": true,
}

// validatePreferences checks that every key in the incoming preferences map is
// valid. Flat keys must have string values ("all"/"muted"). The special
// "channel" key must be an object with recognised sub-keys.
func validatePreferences(prefs map[string]any) error {
	for k, v := range prefs {
		if k == "channel" {
			channelMap, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("channel must be an object, got %T", v)
			}
			for ck, cv := range channelMap {
				if !validChannelKeys[ck] {
					return fmt.Errorf("invalid channel: %s", ck)
				}
				if ck == "feishu" {
					feishuMap, ok := cv.(map[string]any)
					if !ok {
						return fmt.Errorf("channel.feishu must be an object, got %T", cv)
					}
					for fk := range feishuMap {
						if !validFeishuKeys[fk] {
							return fmt.Errorf("invalid channel.feishu key: %s", fk)
						}
					}
				}
			}
			continue
		}
		if !validNotifGroups[k] {
			return fmt.Errorf("invalid preference group: %s", k)
		}
		strVal, ok := v.(string)
		if !ok {
			return fmt.Errorf("preference value for %s must be a string, got %T", k, v)
		}
		if !validNotifValues[strVal] {
			return fmt.Errorf("invalid preference value for %s: %s", k, strVal)
		}
	}
	return nil
}

// mergePreferences merges an incoming partial update into the existing
// preferences stored in the DB. Flat string keys are overwritten; the "channel"
// key is deep-merged so that only the sub-keys present in the update replace
// the corresponding sub-keys in the existing map.
func mergePreferences(existing, incoming map[string]any) map[string]any {
	merged := make(map[string]any, len(existing))
	for k, v := range existing {
		merged[k] = v
	}
	for k, v := range incoming {
		if k == "channel" {
			incomingChannel, ok := v.(map[string]any)
			if !ok {
				merged[k] = v
				continue
			}
			existingChannel, _ := merged[k].(map[string]any)
			if existingChannel == nil {
				existingChannel = make(map[string]any)
			}
			for ck, cv := range incomingChannel {
				if ck == "feishu" {
					incomingFeishu, ok := cv.(map[string]any)
					if !ok {
						existingChannel[ck] = cv
						continue
					}
					existingFeishu, _ := existingChannel[ck].(map[string]any)
					if existingFeishu == nil {
						existingFeishu = make(map[string]any)
					}
					for fk, fv := range incomingFeishu {
						existingFeishu[fk] = fv
					}
					existingChannel[ck] = existingFeishu
				} else {
					existingChannel[ck] = cv
				}
			}
			merged[k] = existingChannel
		} else {
			merged[k] = v
		}
	}
	return merged
}

func (h *Handler) GetNotificationPreferences(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	workspaceID := ctxWorkspaceID(r.Context())

	pref, err := h.Queries.GetNotificationPreference(r.Context(), db.GetNotificationPreferenceParams{
		WorkspaceID: parseUUID(workspaceID),
		UserID:      parseUUID(userID),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusOK, map[string]any{
				"workspace_id": workspaceID,
				"preferences":  map[string]any{},
			})
			return
		}
		slog.Warn("GetNotificationPreference failed", append(logger.RequestAttrs(r), "error", err)...)
		writeError(w, http.StatusInternalServerError, "failed to get notification preferences")
		return
	}

	var prefs map[string]any
	if err := json.Unmarshal(pref.Preferences, &prefs); err != nil {
		prefs = map[string]any{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"workspace_id": workspaceID,
		"preferences":  prefs,
	})
}

type updateNotifPrefRequest struct {
	Preferences map[string]any `json:"preferences"`
}

func (h *Handler) UpdateNotificationPreferences(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	workspaceID := ctxWorkspaceID(r.Context())

	var req updateNotifPrefRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Preferences == nil {
		writeError(w, http.StatusBadRequest, "preferences field is required")
		return
	}

	if err := validatePreferences(req.Preferences); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Fetch existing preferences so we can merge (frontend sends partial updates).
	var merged map[string]any
	existing, err := h.Queries.GetNotificationPreference(r.Context(), db.GetNotificationPreferenceParams{
		WorkspaceID: parseUUID(workspaceID),
		UserID:      parseUUID(userID),
	})
	if err == nil {
		json.Unmarshal(existing.Preferences, &merged)
	}
	if merged == nil {
		merged = make(map[string]any)
	}
	merged = mergePreferences(merged, req.Preferences)

	prefsJSON, err := json.Marshal(merged)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to marshal preferences")
		return
	}

	_, err = h.Queries.UpsertNotificationPreference(r.Context(), db.UpsertNotificationPreferenceParams{
		WorkspaceID: parseUUID(workspaceID),
		UserID:      parseUUID(userID),
		Preferences: prefsJSON,
	})
	if err != nil {
		slog.Warn("UpsertNotificationPreference failed", append(logger.RequestAttrs(r), "error", err)...)
		writeError(w, http.StatusInternalServerError, "failed to update notification preferences")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"workspace_id": workspaceID,
		"preferences":  merged,
	})
}
