package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

// commentPlichtFixture is a running issue task plus its issue, created for one
// gate test and torn down after. The task is inserted directly in 'running'
// state exactly like a daemon-reported completion test: the gate under test
// fires at the /complete boundary before the terminal transaction.
type commentPlichtFixture struct {
	issueID string
	taskID  string
	agentID string
}

func newCommentPlichtFixture(t *testing.T, title string) commentPlichtFixture {
	t.Helper()
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	if err := testPool.QueryRow(ctx, `
		SELECT a.id, a.runtime_id FROM agent a WHERE a.workspace_id = $1 LIMIT 1
	`, testWorkspaceID).Scan(&agentID, &runtimeID); err != nil {
		t.Fatalf("setup: get agent: %v", err)
	}

	var issueID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO issue (workspace_id, title, status, priority, assignee_type, assignee_id, creator_id, creator_type, number, position)
		VALUES ($1, $2, 'in_progress', 'none', 'agent', $3, $4, 'member',
		        (SELECT COALESCE(max(number), 0) + 1 FROM issue WHERE workspace_id = $1), 0)
		RETURNING id
	`, testWorkspaceID, title, agentID, testUserID).Scan(&issueID); err != nil {
		t.Fatalf("setup: create issue: %v", err)
	}

	var taskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, started_at)
		VALUES ($1, $2, $3, 'running', 0, now())
		RETURNING id
	`, agentID, runtimeID, issueID).Scan(&taskID); err != nil {
		t.Fatalf("setup: create task: %v", err)
	}

	return commentPlichtFixture{issueID: issueID, taskID: taskID, agentID: agentID}
}

func (f commentPlichtFixture) cleanup(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, f.taskID)
	testPool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, f.issueID)
	testPool.Exec(ctx, `DELETE FROM issue WHERE id = $1`, f.issueID)
}

// postAgentComment inserts an agent comment of the given type directly, the
// way the CLI's /issues/{id}/comments endpoint would have persisted it.
func (f commentPlichtFixture) postAgentComment(t *testing.T, body, commentType string) {
	t.Helper()
	ctx := context.Background()
	if _, err := testPool.Exec(ctx, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type)
		VALUES ($1, $2, 'agent', $3, $4, $5)
	`, f.issueID, testWorkspaceID, f.agentID, body, commentType); err != nil {
		t.Fatalf("setup: insert comment: %v", err)
	}
}

// complete posts the daemon completion callback for the fixture task.
func (f commentPlichtFixture) complete(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	t.Setenv("COMMENT_PFLICHT_GATE", "on")
	w := httptest.NewRecorder()
	req := newDaemonTokenRequest("POST", "/api/daemon/tasks/"+f.taskID+"/complete",
		map[string]any{
			"output":   "task finished; details in the execution log",
			"work_dir": "/tmp/dp1909",
		},
		testWorkspaceID, "legit-daemon")
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("taskId", f.taskID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	testHandler.CompleteTask(w, req)
	return w
}

// TestCommentPlichtGate_G3a_NoFinalCommentFailsTheRun is the core HG-5 gate
// check (plan.0071 testplan G3a): a run that completes without any agent
// comment on its issue must be recorded as failed, not silently completed.
func TestCommentPlichtGate_G3a_NoFinalCommentFailsTheRun(t *testing.T) {
	f := newCommentPlichtFixture(t, "dp1909 G3a no-comment run")
	defer f.cleanup(t)

	w := f.complete(t)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var status, failureReason string
	if err := testPool.QueryRow(context.Background(), `
		SELECT status, COALESCE(failure_reason, '') FROM agent_task_queue WHERE id = $1
	`, f.taskID).Scan(&status, &failureReason); err != nil {
		t.Fatalf("read task: %v", err)
	}
	if status != "failed" {
		t.Fatalf("task status = %q, want failed — a run without a final comment must not silently succeed (HG-5)", status)
	}
	if failureReason != string(taskfailure.ReasonMissingFinalComment) {
		t.Fatalf("failure_reason = %q, want %q", failureReason, taskfailure.ReasonMissingFinalComment)
	}
}

// TestCommentPlichtGate_G3b_TrivialCommentDoesNotCount pins the G3b rule: a
// short trivial body ("OK") posted during the run does NOT satisfy the gate.
func TestCommentPlichtGate_G3b_TrivialCommentDoesNotCount(t *testing.T) {
	f := newCommentPlichtFixture(t, "dp1909 G3b trivial comment")
	defer f.cleanup(t)

	f.postAgentComment(t, "OK", "comment")
	f.postAgentComment(t, "status flipped to in_review", "system")

	w := f.complete(t)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var status, failureReason string
	if err := testPool.QueryRow(context.Background(), `
		SELECT status, COALESCE(failure_reason, '') FROM agent_task_queue WHERE id = $1
	`, f.taskID).Scan(&status, &failureReason); err != nil {
		t.Fatalf("read task: %v", err)
	}
	if status != "failed" || failureReason != string(taskfailure.ReasonMissingFinalComment) {
		t.Fatalf("status = %q reason = %q, want failed/missing_final_comment — trivial and non-comment rows must not count (G3b)", status, failureReason)
	}
}

// TestCommentPlichtGate_SubstantiveCommentPasses is the positive control: a
// run whose agent posted a ≥30-char type='comment' row completes normally.
func TestCommentPlichtGate_SubstantiveCommentPasses(t *testing.T) {
	f := newCommentPlichtFixture(t, "dp1909 substantive comment passes")
	defer f.cleanup(t)

	f.postAgentComment(t, "Resultaat: gate geïmplementeerd, tests groen, bewijs in de runlog.", "comment")

	w := f.complete(t)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var status string
	if err := testPool.QueryRow(context.Background(), `
		SELECT status FROM agent_task_queue WHERE id = $1
	`, f.taskID).Scan(&status); err != nil && err != pgx.ErrNoRows {
		t.Fatalf("read task: %v", err)
	}
	if status != "completed" {
		t.Fatalf("task status = %q, want completed — a substantive final comment satisfies the gate", status)
	}
}

// TestCommentPlichtGate_X5_MetadataExemptPasses pins the X5 escape hatch: an
// issue carrying metadata {"comment_plicht_exempt": true} completes silently
// by design (read-only / monitoring runs).
func TestCommentPlichtGate_X5_MetadataExemptPasses(t *testing.T) {
	f := newCommentPlichtFixture(t, "dp1909 X5 metadata-exempt run")
	defer f.cleanup(t)

	ctx := context.Background()
	if _, err := testPool.Exec(ctx, `
		UPDATE issue SET metadata = '{"comment_plicht_exempt": true}'::jsonb WHERE id = $1
	`, f.issueID); err != nil {
		t.Fatalf("set exempt metadata: %v", err)
	}

	w := f.complete(t)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var status string
	if err := testPool.QueryRow(ctx, `
		SELECT status FROM agent_task_queue WHERE id = $1
	`, f.taskID).Scan(&status); err != nil {
		t.Fatalf("read task: %v", err)
	}
	if status != "completed" {
		t.Fatalf("task status = %q, want completed — an exempt read-only run must not be failed (X5)", status)
	}
}

// TestCommentPlichtGate_ChatTaskNotGated guards the scope boundary: tasks
// without an IssueID (chat / quick-create) are not issue runs and must never
// be failed by the comment-plicht gate.
func TestCommentPlichtGate_ChatTaskNotGated(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	if err := testPool.QueryRow(ctx, `
		SELECT a.id, a.runtime_id FROM agent a WHERE a.workspace_id = $1 LIMIT 1
	`, testWorkspaceID).Scan(&agentID, &runtimeID); err != nil {
		t.Fatalf("setup: get agent: %v", err)
	}

	var chatSessionID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO chat_session (workspace_id, agent_id, creator_id, title)
		VALUES ($1, $2, $3, 'dp1909 chat gate scope')
		RETURNING id
	`, testWorkspaceID, agentID, testUserID).Scan(&chatSessionID); err != nil {
		t.Fatalf("setup: create chat session: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM chat_session WHERE id = $1`, chatSessionID) })

	var taskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, chat_session_id, status, priority, started_at)
		VALUES ($1, $2, $3, 'running', 0, now())
		RETURNING id
	`, agentID, runtimeID, chatSessionID).Scan(&taskID); err != nil {
		t.Fatalf("setup: create chat task: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, taskID) })

	t.Setenv("COMMENT_PFLICHT_GATE", "on")
	w := httptest.NewRecorder()
	req := newDaemonTokenRequest("POST", "/api/daemon/tasks/"+taskID+"/complete",
		map[string]any{"output": "chat reply delivered"},
		testWorkspaceID, "legit-daemon")
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("taskId", taskID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	testHandler.CompleteTask(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var status string
	if err := testPool.QueryRow(ctx, `
		SELECT status FROM agent_task_queue WHERE id = $1
	`, taskID).Scan(&status); err != nil {
		t.Fatalf("read task: %v", err)
	}
	if status != "completed" {
		t.Fatalf("task status = %q, want completed — chat tasks are outside the comment-plicht gate", status)
	}
}

// TestCommentPlichtGateEnabled pins the killswitch semantics: fail-closed
// default (unset / garbage = ON), explicit off-values only.
func TestCommentPlichtGateEnabled(t *testing.T) {
	h := &Handler{}
	cases := []struct {
		env  string
		want bool
	}{
		{"", true},
		{"on", true},
		{"anything-else", true},
		{" off ", false}, // trimmed
		{"OFF", false},   // case-insensitive
		{"false", false},
		{"0", false},
	}
	for _, tc := range cases {
		t.Setenv("COMMENT_PFLICHT_GATE", tc.env)
		if got := h.commentPlichtGateEnabled(); got != tc.want {
			t.Errorf("env %q: commentPlichtGateEnabled() = %v, want %v", tc.env, got, tc.want)
		}
	}
}

// TestCommentPlichtGate_FailTaskResponseShapeIsJSON guards the daemon
// contract: a gate re-route must return the same JSON task shape as a normal
// failTask so the daemon's terminal-callback handling stays uniform.
func TestCommentPlichtGate_FailTaskResponseShapeIsJSON(t *testing.T) {
	f := newCommentPlichtFixture(t, "dp1909 response shape")
	defer f.cleanup(t)

	w := f.complete(t)
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if s, _ := resp["status"].(string); s != "failed" {
		t.Fatalf("response status = %q, want failed", s)
	}
}
