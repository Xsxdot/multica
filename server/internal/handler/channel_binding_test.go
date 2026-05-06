package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func createTestBinding(t *testing.T, workspaceID, provider, externalChatID, chatType string, isPrimary bool, boundByUserID string) string {
	t.Helper()

	wsUUID := parseUUID(workspaceID)
	userUUID := parseUUID(boundByUserID)

	binding, err := testHandler.Queries.CreateChannelChatBinding(t.Context(), db.CreateChannelChatBindingParams{
		Provider:         provider,
		ExternalChatID:   externalChatID,
		ChatType:         chatType,
		WorkspaceID:      wsUUID,
		IsPrimary:        isPrimary,
		BoundByUserID:    userUUID,
		ExternalChatName: pgtype.Text{String: "Test Chat " + externalChatID, Valid: true},
	})
	if err != nil {
		t.Fatalf("failed to create test binding: %v", err)
	}

	t.Cleanup(func() {
		testHandler.Queries.DeleteChannelChatBinding(t.Context(), binding.ID)
	})

	return uuidToString(binding.ID)
}

// ---------------------------------------------------------------------------
// ListChannelBindings
// ---------------------------------------------------------------------------

func TestListChannelBindings_Success(t *testing.T) {
	bindingID := createTestBinding(t, testWorkspaceID, "feishu", "oc_test_list", "group", true, testUserID)

	w := httptest.NewRecorder()
	req := newRequest("GET", "/api/workspaces/"+testWorkspaceID+"/channel-bindings", nil)
	req = withURLParam(req, "id", testWorkspaceID)
	testHandler.ListChannelBindings(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Bindings []struct {
			ID               string `json:"id"`
			Provider         string `json:"provider"`
			ExternalChatID   string `json:"external_chat_id"`
			ChatType         string `json:"chat_type"`
			ExternalChatName string `json:"external_chat_name"`
			IsPrimary        bool   `json:"is_primary"`
			BoundByUserID    string `json:"bound_by_user_id"`
			CreatedAt        string `json:"created_at"`
		} `json:"bindings"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	found := false
	for _, b := range resp.Bindings {
		if b.ID == bindingID {
			found = true
			if b.Provider != "feishu" {
				t.Errorf("provider = %q, want feishu", b.Provider)
			}
			if !b.IsPrimary {
				t.Error("expected binding to be primary")
			}
			break
		}
	}
	if !found {
		t.Errorf("binding %s not found in response", bindingID)
	}
}

func TestListChannelBindings_EmptyWorkspace(t *testing.T) {
	w := httptest.NewRecorder()
	req := newRequest("GET", "/api/workspaces/"+testWorkspaceID+"/channel-bindings", nil)
	req = withURLParam(req, "id", testWorkspaceID)
	testHandler.ListChannelBindings(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Bindings []any `json:"bindings"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Bindings == nil {
		t.Error("expected empty array, got nil")
	}
}

// ---------------------------------------------------------------------------
// CreateChannelBinding
// ---------------------------------------------------------------------------

func TestCreateChannelBinding_Success(t *testing.T) {
	// Create a bind token first
	if _, err := testPool.Exec(t.Context(), `
		INSERT INTO channel_bind_token (token_hash, provider, external_user_id, expires_at)
		VALUES (decode('deadbeef', 'hex'), 'feishu', 'ext_user_1', now() + interval '1 hour')
	`); err != nil {
		t.Fatalf("failed to create bind token: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(t.Context(), `DELETE FROM channel_bind_token WHERE token_hash = decode('deadbeef', 'hex')`)
	})

	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/workspaces/"+testWorkspaceID+"/channel-bindings", map[string]any{
		"token":    "deadbeef",
		"provider": "feishu",
	})
	req = withURLParam(req, "id", testWorkspaceID)
	testHandler.CreateChannelBinding(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		ID             string `json:"id"`
		Provider       string `json:"provider"`
		ExternalChatID string `json:"external_chat_id"`
		IsPrimary      bool   `json:"is_primary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Provider != "feishu" {
		t.Errorf("provider = %q, want feishu", resp.Provider)
	}
	if !resp.IsPrimary {
		t.Error("expected new binding to be primary when it's the first one")
	}

	// Cleanup the created binding
	t.Cleanup(func() {
		testPool.Exec(t.Context(), `DELETE FROM channel_chat_binding WHERE id = $1`, parseUUID(resp.ID))
	})
}

func TestCreateChannelBinding_InvalidToken(t *testing.T) {
	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/workspaces/"+testWorkspaceID+"/channel-bindings", map[string]any{
		"token":    "invalid_token",
		"provider": "feishu",
	})
	req = withURLParam(req, "id", testWorkspaceID)
	testHandler.CreateChannelBinding(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateChannelBinding_ProviderMismatch(t *testing.T) {
	// Create a feishu token
	if _, err := testPool.Exec(t.Context(), `
		INSERT INTO channel_bind_token (token_hash, provider, external_user_id, expires_at)
		VALUES (decode('cafebabe', 'hex'), 'feishu', 'ext_user_1', now() + interval '1 hour')
	`); err != nil {
		t.Fatalf("failed to create bind token: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(t.Context(), `DELETE FROM channel_bind_token WHERE token_hash = decode('cafebabe', 'hex')`)
	})

	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/workspaces/"+testWorkspaceID+"/channel-bindings", map[string]any{
		"token":    "cafebabe",
		"provider": "discord", // mismatch
	})
	req = withURLParam(req, "id", testWorkspaceID)
	testHandler.CreateChannelBinding(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// DeleteChannelBinding
// ---------------------------------------------------------------------------

func TestDeleteChannelBinding_Success(t *testing.T) {
	bindingID := createTestBinding(t, testWorkspaceID, "feishu", "oc_test_del", "group", true, testUserID)

	w := httptest.NewRecorder()
	req := newRequest("DELETE", "/api/workspaces/"+testWorkspaceID+"/channel-bindings/"+bindingID, nil)
	req = withURLParam(req, "id", testWorkspaceID)
	req = withURLParam(req, "bindingId", bindingID)
	testHandler.DeleteChannelBinding(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteChannelBinding_NotFound(t *testing.T) {
	w := httptest.NewRecorder()
	req := newRequest("DELETE", "/api/workspaces/"+testWorkspaceID+"/channel-bindings/00000000-0000-0000-0000-000000000000", nil)
	req = withURLParam(req, "id", testWorkspaceID)
	req = withURLParam(req, "bindingId", "00000000-0000-0000-0000-000000000000")
	testHandler.DeleteChannelBinding(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteChannelBinding_OtherUserForbidden(t *testing.T) {
	// Create another user
	var otherUserID string
	if err := testPool.QueryRow(t.Context(), `
		INSERT INTO "user" (name, email) VALUES ($1, $2) RETURNING id
	`, "Other User", "other@example.com").Scan(&otherUserID); err != nil {
		t.Fatalf("failed to create other user: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(t.Context(), `DELETE FROM "user" WHERE id = $1`, otherUserID)
	})

	// Create binding as other user
	bindingID := createTestBinding(t, testWorkspaceID, "feishu", "oc_test_del_other", "group", true, otherUserID)

	w := httptest.NewRecorder()
	req := newRequest("DELETE", "/api/workspaces/"+testWorkspaceID+"/channel-bindings/"+bindingID, nil)
	req = withURLParam(req, "id", testWorkspaceID)
	req = withURLParam(req, "bindingId", bindingID)
	testHandler.DeleteChannelBinding(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// SetPrimaryChannelBinding
// ---------------------------------------------------------------------------

func TestSetPrimaryChannelBinding_Success(t *testing.T) {
	primaryID := createTestBinding(t, testWorkspaceID, "feishu", "oc_test_pri1", "group", true, testUserID)
	secondaryID := createTestBinding(t, testWorkspaceID, "feishu", "oc_test_pri2", "group", false, testUserID)

	w := httptest.NewRecorder()
	req := newRequest("PATCH", "/api/workspaces/"+testWorkspaceID+"/channel-bindings/"+secondaryID, map[string]any{
		"is_primary": true,
	})
	req = withURLParam(req, "id", testWorkspaceID)
	req = withURLParam(req, "bindingId", secondaryID)
	testHandler.SetPrimaryChannelBinding(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		ID        string `json:"id"`
		IsPrimary bool   `json:"is_primary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.ID != secondaryID {
		t.Errorf("id = %q, want %q", resp.ID, secondaryID)
	}
	if !resp.IsPrimary {
		t.Error("expected binding to be primary after patch")
	}

	// Verify old primary is no longer primary
	binding, err := testHandler.Queries.GetChannelChatBinding(t.Context(), parseUUID(primaryID))
	if err != nil {
		t.Fatalf("failed to get old primary: %v", err)
	}
	if binding.IsPrimary {
		t.Error("expected old primary to be demoted")
	}
}

func TestSetPrimaryChannelBinding_NotFound(t *testing.T) {
	w := httptest.NewRecorder()
	req := newRequest("PATCH", "/api/workspaces/"+testWorkspaceID+"/channel-bindings/00000000-0000-0000-0000-000000000000", map[string]any{
		"is_primary": true,
	})
	req = withURLParam(req, "id", testWorkspaceID)
	req = withURLParam(req, "bindingId", "00000000-0000-0000-0000-000000000000")
	testHandler.SetPrimaryChannelBinding(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSetPrimaryChannelBinding_OtherUserForbidden(t *testing.T) {
	// Create another user
	var otherUserID string
	if err := testPool.QueryRow(t.Context(), `
		INSERT INTO "user" (name, email) VALUES ($1, $2) RETURNING id
	`, "Other User 2", "other2@example.com").Scan(&otherUserID); err != nil {
		t.Fatalf("failed to create other user: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(t.Context(), `DELETE FROM "user" WHERE id = $1`, otherUserID)
	})

	bindingID := createTestBinding(t, testWorkspaceID, "feishu", "oc_test_pri_other", "group", false, otherUserID)

	w := httptest.NewRecorder()
	req := newRequest("PATCH", "/api/workspaces/"+testWorkspaceID+"/channel-bindings/"+bindingID, map[string]any{
		"is_primary": true,
	})
	req = withURLParam(req, "id", testWorkspaceID)
	req = withURLParam(req, "bindingId", bindingID)
	testHandler.SetPrimaryChannelBinding(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}
}
