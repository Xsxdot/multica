package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/channel/binding"
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

// canManageBinding returns true if the member is allowed to manage (delete or
// change primary status of) a binding. The rule is: binding creator OR
// workspace admin/owner.
func canManageBinding(binding db.ChannelChatBinding, member db.Member) bool {
	return uuidToString(binding.BoundByUserID) == uuidToString(member.UserID) ||
		member.Role == "owner" || member.Role == "admin"
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

type CreateChannelUserBindingRequest struct {
	Token    string `json:"token"`
	Provider string `json:"provider"`
}

type SetPrimaryChannelBindingRequest struct {
	IsPrimary bool `json:"is_primary"`
}

// ---------------------------------------------------------------------------
// POST /api/channel-user-bindings
// ---------------------------------------------------------------------------

func (h *Handler) CreateChannelUserBinding(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}

	var req CreateChannelUserBindingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Token == "" || req.Provider == "" {
		writeError(w, http.StatusBadRequest, "token and provider are required")
		return
	}

	consumer := binding.NewTokenConsumer(h.Queries)
	token, err := consumer.Consume(r.Context(), req.Token)
	if err != nil {
		switch {
		case errors.Is(err, binding.ErrTokenExpired):
			writeError(w, http.StatusBadRequest, "binding token expired")
		case errors.Is(err, binding.ErrTokenAlreadyConsumed):
			writeError(w, http.StatusConflict, "binding token already consumed")
		case errors.Is(err, binding.ErrTokenInvalid):
			writeError(w, http.StatusBadRequest, "invalid binding token")
		default:
			writeError(w, http.StatusInternalServerError, "failed to consume binding token")
		}
		return
	}
	if token.Provider != req.Provider {
		writeError(w, http.StatusBadRequest, "provider mismatch")
		return
	}

	tx, err := h.TxStarter.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to start binding transaction")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	if _, err := tx.Exec(r.Context(), `
		DELETE FROM channel_user_binding
		WHERE provider = $1 AND user_id = $2 AND external_user_id <> $3
	`, token.Provider, parseUUID(userID), token.ExternalUserID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to replace existing user binding")
		return
	}

	_, err = tx.Exec(r.Context(), `
		INSERT INTO channel_user_binding (provider, external_user_id, user_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (provider, external_user_id)
		DO UPDATE SET user_id = EXCLUDED.user_id, updated_at = now()
	`, token.Provider, token.ExternalUserID, parseUUID(userID))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create user binding")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to commit user binding")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"provider":         token.Provider,
		"external_user_id": token.ExternalUserID,
		"user_id":          userID,
	})
}

// ---------------------------------------------------------------------------
// GET /api/workspaces/{id}/channel-bindings
// ---------------------------------------------------------------------------

func (h *Handler) ListChannelBindings(w http.ResponseWriter, r *http.Request) {
	workspaceID := workspaceIDFromURL(r, "id")
	member, ok := h.workspaceMember(w, r, workspaceID)
	if !ok {
		return
	}

	bindings, err := h.Queries.ListChannelChatBindings(r.Context(), member.WorkspaceID)
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

	consumer := binding.NewTokenConsumer(h.Queries)
	token, err := consumer.Consume(r.Context(), req.Token)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid or expired token")
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
	if !canManageBinding(binding, member) {
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
	if !canManageBinding(binding, member) {
		writeError(w, http.StatusForbidden, "insufficient permissions")
		return
	}

	// If setting primary, first clear existing primary for this workspace/provider
	if req.IsPrimary {
		if err := h.Queries.ClearPrimaryBindingsForWorkspaceProvider(r.Context(), db.ClearPrimaryBindingsForWorkspaceProviderParams{
			WorkspaceID: binding.WorkspaceID,
			Provider:    binding.Provider,
		}); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to clear primary bindings")
			return
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
