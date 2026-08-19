package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func queuedTriggerSecurityTasks(t *testing.T, issueID, agentID string) int {
	t.Helper()
	var count int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM agent_task_queue
		WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'
	`, issueID, agentID).Scan(&count); err != nil {
		t.Fatalf("count queued trigger tasks: %v", err)
	}
	return count
}

func insertTriggerSecurityIssue(t *testing.T, assigneeType, assigneeID, status, title string) string {
	t.Helper()
	var issueID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO issue (workspace_id, creator_type, creator_id, title, status, assignee_type, assignee_id, number)
		VALUES ($1, 'member', $2, $3, $4, $5, $6,
		        (SELECT COALESCE(MAX(number), 0) + 1 FROM issue WHERE workspace_id = $1))
		RETURNING id
	`, testWorkspaceID, testUserID, title, status, assigneeType, assigneeID).Scan(&issueID); err != nil {
		t.Fatalf("insert issue: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
		testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID)
	})
	return issueID
}

func loadTriggerSecurityIssue(t *testing.T, issueID string) db.Issue {
	t.Helper()
	issue, err := testHandler.Queries.GetIssueInWorkspace(context.Background(), db.GetIssueInWorkspaceParams{
		ID:          util.MustParseUUID(issueID),
		WorkspaceID: util.MustParseUUID(testWorkspaceID),
	})
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	return issue
}

func createTriggerSecuritySquad(t *testing.T, leaderID string) string {
	t.Helper()
	var squadID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO squad (workspace_id, name, description, leader_id, creator_id)
		VALUES ($1, 'trigger-security-private-leader', '', $2, $3)
		RETURNING id
	`, testWorkspaceID, leaderID, testUserID).Scan(&squadID); err != nil {
		t.Fatalf("create squad: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM squad WHERE id = $1`, squadID)
	})
	return squadID
}

func TestIssueTriggerWriteProbe_DeniesPrivateTargetLikePreview(t *testing.T) {
	privateAgentID, _, memberID := privateAgentTestFixture(t)
	squadID := createTriggerSecuritySquad(t, privateAgentID)

	for _, tc := range []struct {
		name         string
		assigneeType string
		assigneeID   string
	}{
		{name: "direct agent", assigneeType: "agent", assigneeID: privateAgentID},
		{name: "squad leader", assigneeType: "squad", assigneeID: squadID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issueID := insertTriggerSecurityIssue(t, tc.assigneeType, tc.assigneeID, "backlog", "private status trigger "+tc.name)
			issue := loadTriggerSecurityIssue(t, issueID)
			issue.Status = "todo"
			req := newRequestAs(memberID, http.MethodPut, "/api/issues/"+issueID, nil)
			input := service.IssueTriggerInput{Issue: issue, PrevStatus: "backlog", StatusChanged: true}

			previewProbe := testHandler.issueTriggerPreviewProbe(req, "member", memberID, testWorkspaceID, issue)
			if _, ok := testHandler.IssueService.WillEnqueueRun(context.Background(), input, previewProbe); ok {
				t.Fatal("preview must deny a run for an inaccessible private target")
			}
			writeProbe := testHandler.issueTriggerWriteProbe(req, "member", memberID, testWorkspaceID, issue)
			if _, ok := testHandler.IssueService.WillEnqueueRun(context.Background(), input, writeProbe); ok {
				t.Fatal("write probe must deny the same inaccessible private target")
			}
		})
	}
}

func TestIssueStatusWrites_DoNotQueueInaccessiblePrivateTargets(t *testing.T) {
	privateAgentID, _, memberID := privateAgentTestFixture(t)
	squadID := createTriggerSecuritySquad(t, privateAgentID)

	for _, tc := range []struct {
		name         string
		batch        bool
		assigneeType string
		assigneeID   string
	}{
		{name: "single direct agent", assigneeType: "agent", assigneeID: privateAgentID},
		{name: "single squad leader", assigneeType: "squad", assigneeID: squadID},
		{name: "batch direct agent", batch: true, assigneeType: "agent", assigneeID: privateAgentID},
		{name: "batch squad leader", batch: true, assigneeType: "squad", assigneeID: squadID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issueID := insertTriggerSecurityIssue(t, tc.assigneeType, tc.assigneeID, "backlog", "private endpoint "+tc.name)
			w := httptest.NewRecorder()
			if tc.batch {
				req := newRequestAs(memberID, http.MethodPost, "/api/issues/batch-update", map[string]any{
					"issue_ids": []string{issueID},
					"updates":   map[string]any{"status": "todo"},
				})
				testHandler.BatchUpdateIssues(w, req)
			} else {
				req := withURLParam(newRequestAs(memberID, http.MethodPut, "/api/issues/"+issueID, map[string]any{"status": "todo"}), "id", issueID)
				testHandler.UpdateIssue(w, req)
			}
			if w.Code != http.StatusOK {
				t.Fatalf("status write: expected 200, got %d: %s", w.Code, w.Body.String())
			}
			if got := queuedTriggerSecurityTasks(t, issueID, privateAgentID); got != 0 {
				t.Fatalf("inaccessible private target received %d queued tasks", got)
			}
		})
	}
}

func insertTriggerSecurityTask(t *testing.T, agentID, issueID, status string) string {
	t.Helper()
	var taskID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority)
		VALUES ($1, $2, $3, $4, 0)
		RETURNING id
	`, agentID, handlerTestRuntimeID(t), issueID, status).Scan(&taskID); err != nil {
		t.Fatalf("insert %s trigger task: %v", status, err)
	}
	return taskID
}

func insertTriggerSecurityAgent(t *testing.T, name string) string {
	t.Helper()
	var agentID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO agent (
			workspace_id, name, description, runtime_mode, runtime_config,
			runtime_id, visibility, permission_mode, max_concurrent_tasks, owner_id,
			instructions, custom_env, custom_args
		)
		VALUES ($1, $2, '', 'cloud', '{}'::jsonb, $3, 'workspace', 'public_to', 1, $4, '', '{}'::jsonb, '[]'::jsonb)
		RETURNING id
	`, testWorkspaceID, name, handlerTestRuntimeID(t), testUserID).Scan(&agentID); err != nil {
		t.Fatalf("insert ready trigger agent: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent WHERE id = $1`, agentID)
	})
	return agentID
}

func TestIsAgentRunningOnIssue_TargetAndActiveAware(t *testing.T) {
	targetAgentID := seededReadyAgentID(t)
	otherAgentID := insertTriggerSecurityAgent(t, "self-loop-other-agent")
	targetSquadID := createTriggerSecuritySquad(t, targetAgentID)

	for _, tc := range []struct {
		name         string
		assigneeType string
		assigneeID   string
		taskAgentID  string
		taskStatus   string
		want         bool
	}{
		{name: "active direct target task", assigneeType: "agent", assigneeID: targetAgentID, taskAgentID: targetAgentID, taskStatus: "running", want: true},
		{name: "active different agent task", assigneeType: "agent", assigneeID: targetAgentID, taskAgentID: otherAgentID, taskStatus: "running", want: false},
		{name: "completed direct target task", assigneeType: "agent", assigneeID: targetAgentID, taskAgentID: targetAgentID, taskStatus: "completed", want: false},
		{name: "cancelled direct target task", assigneeType: "agent", assigneeID: targetAgentID, taskAgentID: targetAgentID, taskStatus: "cancelled", want: false},
		{name: "active squad leader task", assigneeType: "squad", assigneeID: targetSquadID, taskAgentID: targetAgentID, taskStatus: "running", want: true},
		{name: "active non-leader squad task", assigneeType: "squad", assigneeID: targetSquadID, taskAgentID: otherAgentID, taskStatus: "running", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issueID := insertTriggerSecurityIssue(t, tc.assigneeType, tc.assigneeID, "backlog", "self-loop "+tc.name)
			taskID := insertTriggerSecurityTask(t, tc.taskAgentID, issueID, tc.taskStatus)
			req := newRequest(http.MethodPut, "/api/issues/"+issueID, nil)
			req.Header.Set("X-Task-ID", taskID)
			issue := loadTriggerSecurityIssue(t, issueID)
			if got := testHandler.isAgentRunningOnIssue(req, "agent", issue); got != tc.want {
				t.Fatalf("isAgentRunningOnIssue = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIssueStatusWrites_SelfLoopUsesTargetAndActiveTask(t *testing.T) {
	targetAgentID := seededReadyAgentID(t)
	otherAgentID := insertTriggerSecurityAgent(t, "endpoint-self-loop-other")

	for _, tc := range []struct {
		name        string
		batch       bool
		taskAgentID string
		taskStatus  string
	}{
		{name: "single different agent", taskAgentID: otherAgentID, taskStatus: "running"},
		{name: "single terminal target", taskAgentID: targetAgentID, taskStatus: "completed"},
		{name: "batch different agent", batch: true, taskAgentID: otherAgentID, taskStatus: "running"},
		{name: "batch terminal target", batch: true, taskAgentID: targetAgentID, taskStatus: "completed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issueID := insertTriggerSecurityIssue(t, "agent", targetAgentID, "backlog", "endpoint self-loop "+tc.name)
			taskID := insertTriggerSecurityTask(t, tc.taskAgentID, issueID, tc.taskStatus)
			w := httptest.NewRecorder()
			var req *http.Request
			if tc.batch {
				req = newRequest(http.MethodPost, "/api/issues/batch-update", map[string]any{
					"issue_ids": []string{issueID},
					"updates":   map[string]any{"status": "todo"},
				})
			} else {
				req = withURLParam(newRequest(http.MethodPut, "/api/issues/"+issueID, map[string]any{"status": "todo"}), "id", issueID)
			}
			req.Header.Set("X-Agent-ID", tc.taskAgentID)
			req.Header.Set("X-Task-ID", taskID)
			if tc.batch {
				testHandler.BatchUpdateIssues(w, req)
			} else {
				testHandler.UpdateIssue(w, req)
			}
			if w.Code != http.StatusOK {
				t.Fatalf("status write: expected 200, got %d: %s", w.Code, w.Body.String())
			}
			if got := queuedTriggerSecurityTasks(t, issueID, targetAgentID); got != 1 {
				t.Fatalf("target should receive one fresh queued task, got %d", got)
			}
		})
	}
}

func TestIssueTriggerPreview_UsesFutureTaskOriginatorForSquadGate(t *testing.T) {
	privateLeaderID, requestOriginatorID, issueOriginatorID := privateAgentTestFixture(t)
	squadID := createTriggerSecuritySquad(t, privateLeaderID)
	actorAgentID := insertTriggerSecurityAgent(t, "originator-contract-actor")

	sourceIssueID := insertTriggerSecurityIssue(t, "agent", actorAgentID, "todo", "originator contract source")
	var actorTaskID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, originator_user_id, accountable_user_id)
		VALUES ($1, $2, $3, 'running', 0, $4, $4)
		RETURNING id
	`, actorAgentID, handlerTestRuntimeID(t), sourceIssueID, requestOriginatorID).Scan(&actorTaskID); err != nil {
		t.Fatalf("insert actor task: %v", err)
	}

	var targetIssueID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO issue (workspace_id, creator_type, creator_id, title, status, assignee_type, assignee_id, number)
		VALUES ($1, 'member', $2, 'originator contract target', 'backlog', 'squad', $3,
		        (SELECT COALESCE(MAX(number), 0) + 1 FROM issue WHERE workspace_id = $1))
		RETURNING id
	`, testWorkspaceID, issueOriginatorID, squadID).Scan(&targetIssueID); err != nil {
		t.Fatalf("insert target issue: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, targetIssueID)
		testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, targetIssueID)
	})

	w := httptest.NewRecorder()
	req := newRequestAs(requestOriginatorID, http.MethodPost, "/api/issues/preview-trigger?workspace_id="+testWorkspaceID, map[string]any{
		"issue_ids": []string{targetIssueID},
		"status":    "todo",
	})
	req.Header.Set("X-Agent-ID", actorAgentID)
	req.Header.Set("X-Task-ID", actorTaskID)
	testHandler.PreviewIssueTrigger(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("preview: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var preview IssueTriggerPreviewResponse
	if err := json.NewDecoder(w.Body).Decode(&preview); err != nil {
		t.Fatalf("decode preview: %v", err)
	}
	if preview.TotalCount != 0 {
		t.Fatalf("preview promised %d runs although dispatch uses an unauthorized issue originator", preview.TotalCount)
	}

	write := httptest.NewRecorder()
	writeReq := withURLParam(newRequestAs(requestOriginatorID, http.MethodPut, "/api/issues/"+targetIssueID, map[string]any{"status": "todo"}), "id", targetIssueID)
	writeReq.Header.Set("X-Agent-ID", actorAgentID)
	writeReq.Header.Set("X-Task-ID", actorTaskID)
	testHandler.UpdateIssue(write, writeReq)
	if write.Code != http.StatusOK {
		t.Fatalf("write: expected 200, got %d: %s", write.Code, write.Body.String())
	}
	if got := queuedTriggerSecurityTasks(t, targetIssueID, privateLeaderID); got != 0 {
		t.Fatalf("dispatch queued %d tasks for unauthorized future task originator", got)
	}
}
