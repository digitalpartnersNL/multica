package handler

import (
	"context"
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func insertTriggerSecurityIssue(t *testing.T, assigneeType, assigneeID, status, title string) string {
	t.Helper()
	var issueID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO issue (workspace_id, creator_type, creator_id, title, status, assignee_type, assignee_id)
		VALUES ($1, 'member', $2, $3, $4, $5, $6)
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
