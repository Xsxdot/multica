package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func createTestBinding(t *testing.T, workspaceID, provider, externalChatID, chatType string, isPrimary bool, boundByUserID string) string {
	t.Helper()

	connID := fmt.Sprintf("conn-test-binding-%d", time.Now().UnixNano())
	if _, err := testPool.Exec(t.Context(), `
		INSERT INTO channel_connection (id, provider, display_name, enabled, is_default, config, secret_config, status)
		VALUES ($1, $2, $3, true, false, '{}', '{}', 'connected')
	`, connID, provider, "Test "+connID); err != nil {
		t.Fatalf("failed to create channel connection: %v", err)
	}

	wsUUID := parseUUID(workspaceID)
	userUUID := parseUUID(boundByUserID)

	binding, err := testHandler.Queries.CreateChannelChatBinding(t.Context(), db.CreateChannelChatBindingParams{
		Provider:         provider,
		ConnectionID:     connID,
		ExternalChatID:   externalChatID,
		ChatType:         chatType,
		WorkspaceID:      wsUUID,
		IsPrimary:        isPrimary,
		BoundByUserID:    userUUID,
		ExternalChatName: pgtype.Text{String: "Test Chat " + externalChatID, Valid: true},
		ListenMode:       "mentions",
		DefaultProjectID: pgtype.UUID{Valid: false},
		AgentID:          pgtype.UUID{Valid: false},
	})
	if err != nil {
		t.Fatalf("failed to create test binding: %v", err)
	}

	t.Cleanup(func() {
		testHandler.Queries.DeleteChannelChatBinding(t.Context(), binding.ID)
		_, _ = testPool.Exec(t.Context(), `DELETE FROM channel_connection WHERE id = $1`, connID)
	})

	return uuidToString(binding.ID)
}

func createNonOwnerTestUser(t *testing.T, role string) string {
	t.Helper()
	userID := fmt.Sprintf("00000000-0000-0000-0000-%012d", time.Now().UnixNano()%1000000000000)
	email := "channel-non-owner-" + userID + "@example.test"
	_, err := testPool.Exec(t.Context(), `
		INSERT INTO "user" (id, name, email)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING
	`, userID, "Channel Non Owner", email)
	if err != nil {
		t.Fatalf("failed to create non-owner user: %v", err)
	}
	_, err = testPool.Exec(t.Context(), `
		INSERT INTO member (workspace_id, user_id, role)
		VALUES ($1, $2, $3)
		ON CONFLICT (workspace_id, user_id) DO UPDATE SET role = EXCLUDED.role
	`, testWorkspaceID, userID, role)
	if err != nil {
		t.Fatalf("failed to create non-owner member: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(t.Context(), `DELETE FROM member WHERE user_id = $1`, userID)
		_, _ = testPool.Exec(t.Context(), `DELETE FROM "user" WHERE id = $1`, userID)
	})
	return userID
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
// Channel connection management permissions
// ---------------------------------------------------------------------------

func TestListChannelConnections_RedactsForNonOwner(t *testing.T) {
	suffix := time.Now().UnixNano()
	enabledID := fmt.Sprintf("conn-enabled-%d", suffix)
	disabledID := fmt.Sprintf("conn-disabled-%d", suffix)
	_, err := testPool.Exec(t.Context(), `
		INSERT INTO channel_connection (
			id, provider, display_name, enabled, is_default, config, secret_config, status, last_error
		) VALUES
			($1, 'feishu', 'Enabled Conn', true, false, '{"app_id":"app"}', '{"app_secret":"secret"}', 'connected', 'secret failure'),
			($2, 'feishu', 'Disabled Conn', false, false, '{"app_id":"disabled"}', '{}', 'configured', NULL)
	`, enabledID, disabledID)
	if err != nil {
		t.Fatalf("failed to create channel connections: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(t.Context(), `DELETE FROM channel_connection WHERE id = $1 OR id = $2`, enabledID, disabledID)
	})

	nonOwnerID := createNonOwnerTestUser(t, "admin")
	req := newRequest("GET", "/api/channel-connections", nil)
	req.Header.Set("X-User-ID", nonOwnerID)
	w := httptest.NewRecorder()
	testHandler.ListChannelConnections(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp ListChannelConnectionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.CanManage {
		t.Fatal("expected non-owner can_manage=false")
	}
	if len(resp.Connections) != 1 || resp.Connections[0].ID != enabledID {
		t.Fatalf("expected only enabled connection %q, got %#v", enabledID, resp.Connections)
	}
	if len(resp.Connections[0].Config) != 0 {
		t.Fatalf("expected config to be redacted, got %#v", resp.Connections[0].Config)
	}
	if resp.Connections[0].LastError != nil {
		t.Fatalf("expected last_error to be redacted, got %q", *resp.Connections[0].LastError)
	}
}

func TestCreateChannelConnection_RequiresWorkspaceOwner(t *testing.T) {
	nonOwnerID := createNonOwnerTestUser(t, "admin")
	req := newRequest("POST", "/api/channel-connections", map[string]any{
		"provider":     "feishu",
		"display_name": "Should Not Create",
	})
	req.Header.Set("X-User-ID", nonOwnerID)
	w := httptest.NewRecorder()
	testHandler.CreateChannelConnection(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-owner create, got %d: %s", w.Code, w.Body.String())
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

func TestDeleteChannelBinding_LastBindingAllowed(t *testing.T) {
	// Create a fresh workspace with exactly one binding.
	var wsID string
	if err := testPool.QueryRow(t.Context(), `
		INSERT INTO workspace (name, slug, description, issue_prefix)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, "Delete Last Test", "del-last-test", "Temporary workspace", "DLT").Scan(&wsID); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(t.Context(), `DELETE FROM workspace WHERE id = $1`, wsID)
	})

	if _, err := testPool.Exec(t.Context(), `
		INSERT INTO member (workspace_id, user_id, role)
		VALUES ($1, $2, 'owner')
	`, wsID, testUserID); err != nil {
		t.Fatalf("add member: %v", err)
	}

	bindingID := createTestBinding(t, wsID, "feishu", "oc_test_last", "group", true, testUserID)

	w := httptest.NewRecorder()
	req := newRequest("DELETE", "/api/workspaces/"+wsID+"/channel-bindings/"+bindingID, nil)
	req = withURLParam(req, "id", wsID)
	req = withURLParam(req, "bindingId", bindingID)
	testHandler.DeleteChannelBinding(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for last binding deletion, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteChannelBinding_PrimaryWithOthersBlocked(t *testing.T) {
	// Create a fresh workspace with two bindings for the same provider.
	var wsID string
	if err := testPool.QueryRow(t.Context(), `
		INSERT INTO workspace (name, slug, description, issue_prefix)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, "Delete Primary Test", "del-primary-test", "Temporary workspace", "DPT").Scan(&wsID); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(t.Context(), `DELETE FROM workspace WHERE id = $1`, wsID)
	})

	if _, err := testPool.Exec(t.Context(), `
		INSERT INTO member (workspace_id, user_id, role)
		VALUES ($1, $2, 'owner')
	`, wsID, testUserID); err != nil {
		t.Fatalf("add member: %v", err)
	}

	primaryID := createTestBinding(t, wsID, "feishu", "oc_test_pri", "group", true, testUserID)
	createTestBinding(t, wsID, "feishu", "oc_test_sec", "group", false, testUserID)

	w := httptest.NewRecorder()
	req := newRequest("DELETE", "/api/workspaces/"+wsID+"/channel-bindings/"+primaryID, nil)
	req = withURLParam(req, "id", wsID)
	req = withURLParam(req, "bindingId", primaryID)
	testHandler.DeleteChannelBinding(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when deleting primary with other bindings, got %d: %s", w.Code, w.Body.String())
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

// ---------------------------------------------------------------------------
// Concurrent safety
// ---------------------------------------------------------------------------

func TestDeleteChannelBinding_LocksWorkspaceProvider(t *testing.T) {
	var wsID string
	if err := testPool.QueryRow(t.Context(), `
		INSERT INTO workspace (name, slug, description, issue_prefix)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, "Lock Test", "lock-test", "Temporary workspace", "LT").Scan(&wsID); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(t.Context(), `DELETE FROM workspace WHERE id = $1`, wsID)
	})

	if _, err := testPool.Exec(t.Context(), `
		INSERT INTO member (workspace_id, user_id, role)
		VALUES ($1, $2, 'owner')
	`, wsID, testUserID); err != nil {
		t.Fatalf("add member: %v", err)
	}

	createTestBinding(t, wsID, "feishu", "oc_test_lock", "group", true, testUserID)

	// Simulate the delete handler's lock acquisition.
	tx1, err := testPool.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(t.Context())

	if _, err := tx1.Exec(t.Context(), `
		SELECT id FROM channel_chat_binding
		WHERE workspace_id = $1 AND provider = $2
		FOR UPDATE
	`, parseUUID(wsID), "feishu"); err != nil {
		t.Fatalf("lock bindings: %v", err)
	}

	// Another goroutine trying to acquire the same lock (simulating create handler).
	done := make(chan error, 1)
	go func() {
		tx2, err := testPool.Begin(t.Context())
		if err != nil {
			done <- err
			return
		}
		defer tx2.Rollback(t.Context())

		if _, err := tx2.Exec(t.Context(), `
			SELECT id FROM channel_chat_binding
			WHERE workspace_id = $1 AND provider = $2
			FOR UPDATE
		`, parseUUID(wsID), "feishu"); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	// tx2 should block because tx1 holds the lock.
	select {
	case err := <-done:
		t.Fatalf("expected tx2 to block, but returned: %v", err)
	case <-time.After(100 * time.Millisecond):
		// expected — lock is held
	}

	// Release the lock.
	tx1.Rollback(t.Context())

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("tx2 failed after unblock: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tx2 did not unblock after tx1 rollback")
	}
}

func TestCreateChannelBinding_LocksEmptyWorkspaceProvider(t *testing.T) {
	var wsID string
	if err := testPool.QueryRow(t.Context(), `
		INSERT INTO workspace (name, slug, description, issue_prefix)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, "Empty Lock Test", "empty-lock-test", "Temporary workspace", "EL").Scan(&wsID); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(t.Context(), `DELETE FROM workspace WHERE id = $1`, wsID)
	})

	tx1, err := testPool.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(t.Context())

	if err := lockChannelBindingProvider(t.Context(), tx1, parseUUID(wsID), "feishu"); err != nil {
		t.Fatalf("lock provider: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		tx2, err := testPool.Begin(t.Context())
		if err != nil {
			done <- err
			return
		}
		defer tx2.Rollback(t.Context())

		done <- lockChannelBindingProvider(t.Context(), tx2, parseUUID(wsID), "feishu")
	}()

	select {
	case err := <-done:
		t.Fatalf("expected tx2 to block on empty provider lock, but returned: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	tx1.Rollback(t.Context())

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("tx2 failed after unblock: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tx2 did not unblock after tx1 rollback")
	}
}
