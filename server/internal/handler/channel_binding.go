package handler

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

type ChannelBindingResponse struct {
	ID               string  `json:"id"`
	Provider         string  `json:"provider"`
	ExternalChatID   string  `json:"external_chat_id"`
	ChatType         string  `json:"chat_type"`
	ExternalChatName *string `json:"external_chat_name"`
	IsPrimary        bool    `json:"is_primary"`
	BoundByUserID    string  `json:"bound_by_user_id"`
	CreatedAt        string  `json:"created_at"`
}

func bindingToResponse(b db.ChannelChatBinding) ChannelBindingResponse {
	return ChannelBindingResponse{
		ID:               uuidToString(b.ID),
		Provider:         b.Provider,
		ExternalChatID:   b.ExternalChatID,
		ChatType:         b.ChatType,
		ExternalChatName: textToPtr(b.ExternalChatName),
		IsPrimary:        b.IsPrimary,
		BoundByUserID:    uuidToString(b.BoundByUserID),
		CreatedAt:        timestampToString(b.CreatedAt),
	}
}

type CreateChannelBindingRequest struct {
	Token    string `json:"token"`
	Provider string `json:"provider"`
}

type SetPrimaryChannelBindingRequest struct {
	IsPrimary bool `json:"is_primary"`
}

// ---------------------------------------------------------------------------
// GET /api/workspaces/{id}/channel-bindings
// ---------------------------------------------------------------------------

func (h *Handler) ListChannelBindings(w http.ResponseWriter, r *http.Request) {
	workspaceID := workspaceIDFromURL(r, "id")
	wsUUID, ok := parseUUIDOrBadRequest(w, workspaceID, "workspace id")
	if !ok {
		return
	}

	bindings, err := h.Queries.ListChannelChatBindings(r.Context(), wsUUID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list bindings")
		return
	}

	resp := make([]ChannelBindingResponse, len(bindings))
	for i, b := range bindings {
		resp[i] = bindingToResponse(b)
	}

	writeJSON(w, http.StatusOK, map[string]any{"bindings": resp})
}

// ---------------------------------------------------------------------------
// POST /api/workspaces/{id}/channel-bindings
// ---------------------------------------------------------------------------

func (h *Handler) CreateChannelBinding(w http.ResponseWriter, r *http.Request) {
	workspaceID := workspaceIDFromURL(r, "id")
	member, ok := h.workspaceMember(w, r, workspaceID)
	if !ok {
		return
	}

	var req CreateChannelBindingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Token == "" || req.Provider == "" {
		writeError(w, http.StatusBadRequest, "token and provider are required")
		return
	}

	// Hash token and look it up
	tokenHash := sha256.Sum256([]byte(req.Token))
	token, err := h.Queries.ConsumeChannelBindToken(r.Context(), tokenHash[:])
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusBadRequest, "invalid or expired token")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to consume token")
		return
	}

	if token.Provider != req.Provider {
		writeError(w, http.StatusBadRequest, "provider mismatch")
		return
	}

	// Check if there are existing bindings for this workspace/provider
	// to determine is_primary
	existingBindings, err := h.Queries.ListChannelChatBindings(r.Context(), member.WorkspaceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to check existing bindings")
		return
	}
	isPrimary := len(existingBindings) == 0

	// The external_chat_id and chat_type come from the token consumer
	// For now, we use the external_user_id as a placeholder for external_chat_id
	// since the actual chat info would be resolved by the channel adapter.
	// In production, the channel adapter (e.g. Feishu) would provide the
	// actual chat info when consuming the token.
	binding, err := h.Queries.CreateChannelChatBinding(r.Context(), db.CreateChannelChatBindingParams{
		Provider:         req.Provider,
		ExternalChatID:   token.ExternalUserID, // placeholder: actual chat ID from channel
		ChatType:         "group",
		WorkspaceID:      member.WorkspaceID,
		IsPrimary:        isPrimary,
		BoundByUserID:    member.UserID,
		ExternalChatName: pgtype.Text{String: "", Valid: false},
	})
	if err != nil {
		if isUniqueViolation(err) {
			writeError(w, http.StatusConflict, "this chat is already bound to another workspace")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to create binding")
		return
	}

	writeJSON(w, http.StatusCreated, bindingToResponse(binding))
}

// ---------------------------------------------------------------------------
// DELETE /api/workspaces/{id}/channel-bindings/{bindingId}
// ---------------------------------------------------------------------------

func (h *Handler) DeleteChannelBinding(w http.ResponseWriter, r *http.Request) {
	workspaceID := workspaceIDFromURL(r, "id")
	member, ok := h.workspaceMember(w, r, workspaceID)
	if !ok {
		return
	}

	bindingID := chi.URLParam(r, "bindingId")
	bindingUUID, ok := parseUUIDOrBadRequest(w, bindingID, "binding id")
	if !ok {
		return
	}

	binding, err := h.Queries.GetChannelChatBinding(r.Context(), bindingUUID)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "binding not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load binding")
		return
	}

	if uuidToString(binding.WorkspaceID) != workspaceID {
		writeError(w, http.StatusNotFound, "binding not found")
		return
	}

	// Only binding creator or workspace admin/owner can delete
	if uuidToString(binding.BoundByUserID) != uuidToString(member.UserID) &&
		member.Role != "owner" && member.Role != "admin" {
		writeError(w, http.StatusForbidden, "insufficient permissions")
		return
	}

	if err := h.Queries.DeleteChannelChatBinding(r.Context(), bindingUUID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete binding")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// PATCH /api/workspaces/{id}/channel-bindings/{bindingId}
// ---------------------------------------------------------------------------

func (h *Handler) SetPrimaryChannelBinding(w http.ResponseWriter, r *http.Request) {
	workspaceID := workspaceIDFromURL(r, "id")
	member, ok := h.workspaceMember(w, r, workspaceID)
	if !ok {
		return
	}

	bindingID := chi.URLParam(r, "bindingId")
	bindingUUID, ok := parseUUIDOrBadRequest(w, bindingID, "binding id")
	if !ok {
		return
	}

	var req SetPrimaryChannelBindingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	binding, err := h.Queries.GetChannelChatBinding(r.Context(), bindingUUID)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "binding not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load binding")
		return
	}

	if uuidToString(binding.WorkspaceID) != workspaceID {
		writeError(w, http.StatusNotFound, "binding not found")
		return
	}

	// Only binding creator or workspace admin/owner can set primary
	if uuidToString(binding.BoundByUserID) != uuidToString(member.UserID) &&
		member.Role != "owner" && member.Role != "admin" {
		writeError(w, http.StatusForbidden, "insufficient permissions")
		return
	}

	// If setting primary, first clear existing primary for this workspace/provider
	if req.IsPrimary {
		if err := h.Queries.ClearPrimaryBindingsForWorkspaceProvider(r.Context(), db.ClearPrimaryBindingsForWorkspaceProviderParams{
			WorkspaceID: binding.WorkspaceID,
			Provider:    binding.Provider,
		}); err != nil {
			// Non-fatal: continue and try to set the new primary
		}
	}

	updated, err := h.Queries.SetChannelChatBindingPrimary(r.Context(), db.SetChannelChatBindingPrimaryParams{
		ID:        bindingUUID,
		IsPrimary: req.IsPrimary,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update binding")
		return
	}

	writeJSON(w, http.StatusOK, bindingToResponse(updated))
}
