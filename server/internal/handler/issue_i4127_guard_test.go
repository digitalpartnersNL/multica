package handler

// I4127.DP regression: an issue created or moved into the in_progress
// category without an assignee is a zombie — no run is ever enqueued, the
// issue counts against the queue ceiling forever. The application-layer
// guard (validateInProgressRequiresAssignee) must refuse:
//
//  1. create in_progress without assignee        -> 400
//  2. create in_progress WITH agent assignee     -> 201 (legitimate)
//  3. create todo without assignee               -> 201 (legitimate)
//  4. update todo -> in_progress while unassigned -> 400
//  5. batch update todo -> in_progress unassigned -> silently skipped
//
// This mirrors the 2026-08-18 incident where 195 zombie issues (title =
// id, no assignee) occupied the queue ceiling and blocked dispatch.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestI4127_InProgressRequiresAssignee(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()

	var projectID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO project (workspace_id, title) VALUES ($1, $2) RETURNING id
	`, testWorkspaceID, fmt.Sprintf("I4127 %d", suffix)).Scan(&projectID); err != nil {
		t.Fatalf("create project: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM project WHERE id = $1`, projectID) })

	var agentID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent (
			workspace_id, name, description, runtime_mode, runtime_config,
			runtime_id, visibility, max_concurrent_tasks, owner_id
		)
		VALUES ($1, $2, '', 'cloud', '{}'::jsonb, $3, 'workspace', 1, $4)
		RETURNING id
	`, testWorkspaceID, fmt.Sprintf("I4127 Agent %d", suffix), testRuntimeID, testUserID).Scan(&agentID); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM agent WHERE id = $1`, agentID) })

	create := func(t *testing.T, status string, atype, aid *string) int {
		t.Helper()
		body := map[string]any{
			"title":      fmt.Sprintf("I4127 test %d", time.Now().UnixNano()),
			"status":     status,
			"project_id": projectID,
		}
		if atype != nil {
			body["assignee_type"] = *atype
		}
		if aid != nil {
			body["assignee_id"] = *aid
		}
		w := httptest.NewRecorder()
		testHandler.CreateIssue(w, newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, body))
		return w.Code
	}

	t.Run("create in_progress zonder assignee -> 400", func(t *testing.T) {
		if code := create(t, "in_progress", nil, nil); code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", code)
		}
	})

	t.Run("create in_progress met agent assignee -> 201", func(t *testing.T) {
		atype, aid := "agent", agentID
		if code := create(t, "in_progress", &atype, &aid); code != http.StatusCreated {
			t.Fatalf("expected 201, got %d", code)
		}
	})

	t.Run("create todo zonder assignee -> 201", func(t *testing.T) {
		if code := create(t, "todo", nil, nil); code != http.StatusCreated {
			t.Fatalf("expected 201, got %d", code)
		}
	})

	t.Run("update todo->in_progress zonder assignee -> 400", func(t *testing.T) {
		var issueID string
		if err := testPool.QueryRow(ctx, `
			INSERT INTO issue (workspace_id, project_id, title, status, number, creator_type, creator_id)
			VALUES ($1, $2, $3, 'todo',
				(SELECT COALESCE(MAX(number),0)+1 FROM issue WHERE workspace_id=$1), 'member', $4)
			RETURNING id
		`, testWorkspaceID, projectID, fmt.Sprintf("I4127 update %d", suffix), testUserID).Scan(&issueID); err != nil {
			t.Fatalf("create todo issue: %v", err)
		}
		t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID) })

		w := httptest.NewRecorder()
		req := newRequest("PUT", "/api/issues/"+issueID, map[string]any{"status": "in_progress"})
		req = withURLParam(req, "id", issueID)
		testHandler.UpdateIssue(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})

	t.Run("update todo->in_progress met agent -> 200", func(t *testing.T) {
		var issueID string
		if err := testPool.QueryRow(ctx, `
			INSERT INTO issue (workspace_id, project_id, title, status, number, creator_type, creator_id)
			VALUES ($1, $2, $3, 'todo',
				(SELECT COALESCE(MAX(number),0)+1 FROM issue WHERE workspace_id=$1), 'member', $4)
			RETURNING id
		`, testWorkspaceID, projectID, fmt.Sprintf("I4127 update-ok %d", suffix), testUserID).Scan(&issueID); err != nil {
			t.Fatalf("create todo issue: %v", err)
		}
		t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID) })

		w := httptest.NewRecorder()
		req := newRequest("PUT", "/api/issues/"+issueID, map[string]any{
			"status":        "in_progress",
			"assignee_type": "agent",
			"assignee_id":   agentID,
		})
		req = withURLParam(req, "id", issueID)
		testHandler.UpdateIssue(w, req)
		if w.Code < 200 || w.Code >= 300 {
			t.Fatalf("expected 2xx, got %d", w.Code)
		}
	})

	t.Run("batch todo->in_progress zonder assignee -> skip", func(t *testing.T) {
		var issueID string
		if err := testPool.QueryRow(ctx, `
			INSERT INTO issue (workspace_id, project_id, title, status, number, creator_type, creator_id)
			VALUES ($1, $2, $3, 'todo',
				(SELECT COALESCE(MAX(number),0)+1 FROM issue WHERE workspace_id=$1), 'member', $4)
			RETURNING id
		`, testWorkspaceID, projectID, fmt.Sprintf("I4127 batch %d", suffix), testUserID).Scan(&issueID); err != nil {
			t.Fatalf("create todo issue: %v", err)
		}
		t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID) })

		w := httptest.NewRecorder()
		testHandler.BatchUpdateIssues(w, newRequest("PATCH", "/api/issues/batch", map[string]any{
			"issue_ids": []string{issueID},
			"updates":   map[string]any{"status": "in_progress"},
		}))

		// I4191.DP: the request shape must be issue_ids + updates-object;
		// the old array shape made the handler bail on "issue_ids is
		// required" and the subtest passed without exercising the guard.
		// Assert the response (200, updated:0) as well as the DB state.
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var resp struct {
			Updated int `json:"updated"`
		}
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp.Updated != 0 {
			t.Fatalf("expected updated=0, got %d", resp.Updated)
		}

		var status string
		if err := testPool.QueryRow(ctx, `SELECT status FROM issue WHERE id=$1`, issueID).Scan(&status); err != nil {
			t.Fatalf("read issue: %v", err)
		}
		if status != "todo" {
			t.Fatalf("expected issue to stay todo, got %s", status)
		}
	})
}
