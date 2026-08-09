package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

type blockingFailTxStarter struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingFailTxStarter) Begin(ctx context.Context) (pgx.Tx, error) {
	close(b.entered)
	select {
	case <-b.release:
		return nil, errors.New("injected transaction start failure")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// seededReadyAgentID returns a workspace agent that has a runtime bound (the
// fixture's first agent), so WillEnqueueRun treats it as ready.
func seededReadyAgentID(t *testing.T) string {
	t.Helper()
	var id string
	if err := testPool.QueryRow(context.Background(), `
		SELECT id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL
		ORDER BY created_at ASC LIMIT 1
	`, testWorkspaceID).Scan(&id); err != nil {
		t.Fatalf("load ready agent: %v", err)
	}
	return id
}

func previewIssueTrigger(t *testing.T, body map[string]any) IssueTriggerPreviewResponse {
	t.Helper()
	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/issues/preview-trigger?workspace_id="+testWorkspaceID, body)
	testHandler.PreviewIssueTrigger(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PreviewIssueTrigger: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp IssueTriggerPreviewResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode preview: %v", err)
	}
	return resp
}

func createIssueForTest(t *testing.T, body map[string]any) IssueResponse {
	t.Helper()
	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, body)
	testHandler.CreateIssue(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("CreateIssue: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var created IssueResponse
	if err := json.NewDecoder(w.Body).Decode(&created); err != nil {
		t.Fatalf("decode issue: %v", err)
	}
	t.Cleanup(func() {
		r := withURLParam(newRequest("DELETE", "/api/issues/"+created.ID, nil), "id", created.ID)
		testHandler.DeleteIssue(httptest.NewRecorder(), r)
	})
	return created
}

func taskCountFor(t *testing.T, issueID, agentID string) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2
	`, issueID, agentID).Scan(&n); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	return n
}

// TestPreviewIssueTrigger_CreateAgentVsBacklog covers the create entry point:
// an active status with an agent assignee previews one run; the same assignee
// parked in backlog previews none.
func TestPreviewIssueTrigger_CreateAgentVsBacklog(t *testing.T) {
	agentID := seededReadyAgentID(t)

	active := previewIssueTrigger(t, map[string]any{
		"is_create":     true,
		"assignee_type": "agent",
		"assignee_id":   agentID,
		"status":        "todo",
	})
	if active.TotalCount != 1 || len(active.Triggers) != 1 {
		t.Fatalf("active create: expected 1 trigger, got %+v", active)
	}
	if active.Triggers[0].AgentID != agentID || active.Triggers[0].Source != "assign" {
		t.Fatalf("active create: wrong trigger %+v", active.Triggers[0])
	}

	backlog := previewIssueTrigger(t, map[string]any{
		"is_create":     true,
		"assignee_type": "agent",
		"assignee_id":   agentID,
		"status":        "backlog",
	})
	if backlog.TotalCount != 0 {
		t.Fatalf("backlog create: expected 0 triggers, got %+v", backlog)
	}
}

// TestPreviewIssueTrigger_MemberNoTrigger verifies a member assignee never
// previews a run.
func TestPreviewIssueTrigger_MemberNoTrigger(t *testing.T) {
	resp := previewIssueTrigger(t, map[string]any{
		"is_create":     true,
		"assignee_type": "member",
		"assignee_id":   testUserID,
		"status":        "todo",
	})
	if resp.TotalCount != 0 {
		t.Fatalf("member assignee: expected 0 triggers, got %+v", resp)
	}
}

// TestPreviewIssueTrigger_BatchAggregates verifies the batch shape: two
// agent-assigned issues moving out of backlog preview two distinct runs.
func TestPreviewIssueTrigger_BatchAggregates(t *testing.T) {
	agentID := seededReadyAgentID(t)
	i1 := createIssueForTest(t, map[string]any{"title": "batch preview 1", "status": "backlog", "assignee_type": "agent", "assignee_id": agentID})
	i2 := createIssueForTest(t, map[string]any{"title": "batch preview 2", "status": "backlog", "assignee_type": "agent", "assignee_id": agentID})

	resp := previewIssueTrigger(t, map[string]any{
		"issue_ids": []string{i1.ID, i2.ID},
		"status":    "todo",
	})
	if resp.TotalCount != 2 {
		t.Fatalf("batch promote: expected total_count 2, got %+v", resp)
	}
	seen := map[string]bool{}
	for _, tr := range resp.Triggers {
		if tr.Source != "status" {
			t.Fatalf("batch promote: expected source=status, got %q", tr.Source)
		}
		seen[tr.IssueID] = true
	}
	if !seen[i1.ID] || !seen[i2.ID] {
		t.Fatalf("batch promote: missing an issue in %+v", resp.Triggers)
	}
}

// TestPreviewIssueTrigger_MatchesWritePath is the core invariant: when preview
// says a run will start, the real write path enqueues it; when preview says it
// won't, the write path enqueues nothing.
func TestPreviewIssueTrigger_MatchesWritePath(t *testing.T) {
	agentID := seededReadyAgentID(t)

	// Case 1: preview says assign will start → write path enqueues.
	issue := createIssueForTest(t, map[string]any{"title": "match write 1", "status": "todo"})
	pv := previewIssueTrigger(t, map[string]any{
		"issue_ids":     []string{issue.ID},
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	if pv.TotalCount != 1 {
		t.Fatalf("preview assign: expected 1, got %+v", pv)
	}
	w := httptest.NewRecorder()
	req := withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{"assignee_type": "agent", "assignee_id": agentID}), "id", issue.ID)
	testHandler.UpdateIssue(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("UpdateIssue assign: %d %s", w.Code, w.Body.String())
	}
	if got := taskCountFor(t, issue.ID, agentID); got == 0 {
		t.Fatalf("preview promised a run but write path enqueued none")
	}

	// Case 2: preview says backlog assign will NOT start → write enqueues none.
	issue2 := createIssueForTest(t, map[string]any{"title": "match write 2", "status": "backlog"})
	pv2 := previewIssueTrigger(t, map[string]any{
		"issue_ids":     []string{issue2.ID},
		"assignee_type": "agent",
		"assignee_id":   agentID,
		"status":        "backlog",
	})
	if pv2.TotalCount != 0 {
		t.Fatalf("preview backlog assign: expected 0, got %+v", pv2)
	}
	w2 := httptest.NewRecorder()
	req2 := withURLParam(newRequest("PUT", "/api/issues/"+issue2.ID, map[string]any{"assignee_type": "agent", "assignee_id": agentID}), "id", issue2.ID)
	testHandler.UpdateIssue(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("UpdateIssue backlog assign: %d %s", w2.Code, w2.Body.String())
	}
	if got := taskCountFor(t, issue2.ID, agentID); got != 0 {
		t.Fatalf("preview said no run for backlog assign but write path enqueued %d", got)
	}
}

// TestPreviewIssueTrigger_InReviewToTodoIsReactivation keeps preview and write
// behavior aligned for rejected verification rounds. A reviewer returning an
// assigned issue to todo must visibly promise a fresh execution run.
func TestPreviewIssueTrigger_InReviewToTodoIsReactivation(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{
		"title":  "preview rejected verification rework",
		"status": "in_review",
	})

	// Assigning an active issue starts an initial run. Mark it terminal so a
	// pending-run dedup cannot hide the reactivation decision under test.
	w := httptest.NewRecorder()
	req := withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{
		"assignee_type": "agent",
		"assignee_id":   agentID,
	}), "id", issue.ID)
	testHandler.UpdateIssue(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("assign issue: %d %s", w.Code, w.Body.String())
	}
	if _, err := testPool.Exec(context.Background(), `
		UPDATE agent_task_queue
		SET status = 'completed', completed_at = now()
		WHERE issue_id = $1 AND agent_id = $2
	`, issue.ID, agentID); err != nil {
		t.Fatalf("complete initial task: %v", err)
	}

	resp := previewIssueTrigger(t, map[string]any{
		"issue_ids": []string{issue.ID},
		"status":    "todo",
	})
	if resp.TotalCount != 1 {
		t.Fatalf("expected reactivation preview to promise 1 run, got %+v", resp)
	}
	if resp.Triggers[0].Source != "status" {
		t.Fatalf("expected source=status, got %q", resp.Triggers[0].Source)
	}
}

func TestPreviewIssueTrigger_InReviewToTodoDeduplicatesPendingRun(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{
		"title":  "preview rework with pending run",
		"status": "in_review",
	})

	w := httptest.NewRecorder()
	req := withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{
		"assignee_type": "agent",
		"assignee_id":   agentID,
	}), "id", issue.ID)
	testHandler.UpdateIssue(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("assign issue: %d %s", w.Code, w.Body.String())
	}

	resp := previewIssueTrigger(t, map[string]any{
		"issue_ids": []string{issue.ID},
		"status":    "todo",
	})
	if resp.TotalCount != 0 {
		t.Fatalf("pending run must deduplicate reactivation, got %+v", resp)
	}
}

// TestUpdateIssueSuppressRunSkipsEnqueue verifies suppress_run applies the
// assignee change but starts no run, while the same write without it does.
func TestUpdateIssueSuppressRunSkipsEnqueue(t *testing.T) {
	agentID := seededReadyAgentID(t)

	// Suppressed assign: assignee set, no task.
	suppressed := createIssueForTest(t, map[string]any{"title": "suppress on", "status": "todo"})
	w := httptest.NewRecorder()
	req := withURLParam(newRequest("PUT", "/api/issues/"+suppressed.ID, map[string]any{
		"assignee_type": "agent", "assignee_id": agentID, "suppress_run": true,
	}), "id", suppressed.ID)
	testHandler.UpdateIssue(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("UpdateIssue suppressed: %d %s", w.Code, w.Body.String())
	}
	if got := taskCountFor(t, suppressed.ID, agentID); got != 0 {
		t.Fatalf("suppress_run=true should not enqueue, got %d tasks", got)
	}

	// Control: same write without suppress_run enqueues.
	control := createIssueForTest(t, map[string]any{"title": "suppress off", "status": "todo"})
	w2 := httptest.NewRecorder()
	req2 := withURLParam(newRequest("PUT", "/api/issues/"+control.ID, map[string]any{
		"assignee_type": "agent", "assignee_id": agentID,
	}), "id", control.ID)
	testHandler.UpdateIssue(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("UpdateIssue control: %d %s", w2.Code, w2.Body.String())
	}
	if got := taskCountFor(t, control.ID, agentID); got == 0 {
		t.Fatalf("control (no suppress_run) should enqueue, got 0 tasks")
	}
}

// A failed activation compensates the status mutation so an identical retry
// remains a real transition and can start exactly one run.
func TestUpdateIssueRunEnqueueFailureIsVisible(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{
		"title":         "visible enqueue failure",
		"status":        "backlog",
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	seedDupRacePR(t, issue.ID, 999421)

	// Force the activation enqueue to fail after WillEnqueueRun has promised it.
	// Linked-PR activations require a transaction, so removing the transaction
	// starter is a deterministic service failpoint without corrupting Postgres.
	originalTxStarter := testHandler.TaskService.TxStarter
	testHandler.TaskService.TxStarter = nil
	t.Cleanup(func() { testHandler.TaskService.TxStarter = originalTxStarter })

	w := httptest.NewRecorder()
	req := withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{"status": "todo"}), "id", issue.ID)
	testHandler.UpdateIssue(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("UpdateIssue enqueue failure: got %d %s, want 503", w.Code, w.Body.String())
	}

	var storedStatus string
	if err := testPool.QueryRow(context.Background(), `SELECT status FROM issue WHERE id = $1`, issue.ID).Scan(&storedStatus); err != nil {
		t.Fatalf("read persisted issue status: %v", err)
	}
	if storedStatus != "backlog" {
		t.Fatalf("persisted issue status = %q, want compensated backlog", storedStatus)
	}
	if got := taskCountFor(t, issue.ID, agentID); got != 0 {
		t.Fatalf("failed enqueue created %d tasks, want 0", got)
	}

	testHandler.TaskService.TxStarter = originalTxStarter
	retryW := httptest.NewRecorder()
	retryReq := withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{"status": "todo"}), "id", issue.ID)
	testHandler.UpdateIssue(retryW, retryReq)
	if retryW.Code != http.StatusOK {
		t.Fatalf("UpdateIssue retry: got %d %s, want 200", retryW.Code, retryW.Body.String())
	}
	if got := taskCountFor(t, issue.ID, agentID); got != 1 {
		t.Fatalf("retry task count = %d, want exactly 1", got)
	}
}

func TestBatchUpdateIssueRunEnqueueFailureCompensatesAndRetries(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{
		"title":         "batch visible enqueue failure",
		"status":        "backlog",
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	seedDupRacePR(t, issue.ID, 999423)

	originalTxStarter := testHandler.TaskService.TxStarter
	testHandler.TaskService.TxStarter = nil
	t.Cleanup(func() { testHandler.TaskService.TxStarter = originalTxStarter })
	body := map[string]any{"issue_ids": []string{issue.ID}, "updates": map[string]any{"status": "todo"}}
	w := httptest.NewRecorder()
	testHandler.BatchUpdateIssues(w, newRequest("POST", "/api/issues/batch?workspace_id="+testWorkspaceID, body))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("BatchUpdateIssues enqueue failure: got %d %s, want 503", w.Code, w.Body.String())
	}
	var storedStatus string
	if err := testPool.QueryRow(context.Background(), `SELECT status FROM issue WHERE id = $1`, issue.ID).Scan(&storedStatus); err != nil {
		t.Fatalf("read compensated batch status: %v", err)
	}
	if storedStatus != "backlog" {
		t.Fatalf("batch persisted status = %q, want compensated backlog", storedStatus)
	}
	if got := taskCountFor(t, issue.ID, agentID); got != 0 {
		t.Fatalf("failed batch enqueue created %d tasks, want 0", got)
	}

	testHandler.TaskService.TxStarter = originalTxStarter
	retryW := httptest.NewRecorder()
	testHandler.BatchUpdateIssues(retryW, newRequest("POST", "/api/issues/batch?workspace_id="+testWorkspaceID, body))
	if retryW.Code != http.StatusOK {
		t.Fatalf("BatchUpdateIssues retry: got %d %s, want 200", retryW.Code, retryW.Body.String())
	}
	if got := taskCountFor(t, issue.ID, agentID); got != 1 {
		t.Fatalf("batch retry task count = %d, want exactly 1", got)
	}
}

func TestUpdateIssueAssigneeEnqueueFailureCompensatesAndRetries(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{
		"title": "single assignment compensation", "status": "todo",
		"assignee_type": "member", "assignee_id": testUserID,
	})
	seedDupRacePR(t, issue.ID, 999425)
	original := testHandler.TaskService.TxStarter
	testHandler.TaskService.TxStarter = nil
	t.Cleanup(func() { testHandler.TaskService.TxStarter = original })
	body := map[string]any{"assignee_type": "agent", "assignee_id": agentID}

	w := httptest.NewRecorder()
	testHandler.UpdateIssue(w, withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, body), "id", issue.ID))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("assignment enqueue failure: got %d %s, want 503", w.Code, w.Body.String())
	}
	var assigneeType, assigneeID string
	if err := testPool.QueryRow(context.Background(), `SELECT assignee_type, assignee_id FROM issue WHERE id=$1`, issue.ID).Scan(&assigneeType, &assigneeID); err != nil {
		t.Fatalf("read compensated assignment: %v", err)
	}
	if assigneeType != "member" || assigneeID != testUserID {
		t.Fatalf("compensated assignee = %s/%s, want member/%s", assigneeType, assigneeID, testUserID)
	}

	testHandler.TaskService.TxStarter = original
	retry := httptest.NewRecorder()
	testHandler.UpdateIssue(retry, withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, body), "id", issue.ID))
	if retry.Code != http.StatusOK || taskCountFor(t, issue.ID, agentID) != 1 {
		t.Fatalf("assignment retry: status=%d tasks=%d, want 200/1", retry.Code, taskCountFor(t, issue.ID, agentID))
	}
}

func TestBatchUpdateIssueAssigneeEnqueueFailureCompensatesAndRetries(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{
		"title": "batch assignment compensation", "status": "todo",
		"assignee_type": "member", "assignee_id": testUserID,
	})
	seedDupRacePR(t, issue.ID, 999426)
	original := testHandler.TaskService.TxStarter
	testHandler.TaskService.TxStarter = nil
	t.Cleanup(func() { testHandler.TaskService.TxStarter = original })
	body := map[string]any{"issue_ids": []string{issue.ID}, "updates": map[string]any{"assignee_type": "agent", "assignee_id": agentID}}

	w := httptest.NewRecorder()
	testHandler.BatchUpdateIssues(w, newRequest("POST", "/api/issues/batch?workspace_id="+testWorkspaceID, body))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("batch assignment enqueue failure: got %d %s, want 503", w.Code, w.Body.String())
	}
	var assigneeType, assigneeID string
	if err := testPool.QueryRow(context.Background(), `SELECT assignee_type, assignee_id FROM issue WHERE id=$1`, issue.ID).Scan(&assigneeType, &assigneeID); err != nil {
		t.Fatalf("read compensated batch assignment: %v", err)
	}
	if assigneeType != "member" || assigneeID != testUserID {
		t.Fatalf("compensated batch assignee = %s/%s, want member/%s", assigneeType, assigneeID, testUserID)
	}

	testHandler.TaskService.TxStarter = original
	retry := httptest.NewRecorder()
	testHandler.BatchUpdateIssues(retry, newRequest("POST", "/api/issues/batch?workspace_id="+testWorkspaceID, body))
	if retry.Code != http.StatusOK || taskCountFor(t, issue.ID, agentID) != 1 {
		t.Fatalf("batch assignment retry: status=%d tasks=%d, want 200/1", retry.Code, taskCountFor(t, issue.ID, agentID))
	}
}

func TestUpdateIssueRunEnqueueFailureCompensationPreservesConcurrentStatusAndAssignee(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{
		"title":         "concurrent status survives compensation",
		"status":        "backlog",
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	seedDupRacePR(t, issue.ID, 999424)

	originalTxStarter := testHandler.TaskService.TxStarter
	failpoint := &blockingFailTxStarter{entered: make(chan struct{}), release: make(chan struct{})}
	testHandler.TaskService.TxStarter = failpoint
	t.Cleanup(func() { testHandler.TaskService.TxStarter = originalTxStarter })

	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{"status": "todo"}), "id", issue.ID)
		testHandler.UpdateIssue(w, req)
	}()
	<-failpoint.entered
	if _, err := testPool.Exec(context.Background(), `UPDATE issue SET status = 'in_progress', assignee_type = 'member', assignee_id = $2, updated_at = now() + interval '1 second' WHERE id = $1`, issue.ID, testUserID); err != nil {
		t.Fatalf("write concurrent status and assignee: %v", err)
	}
	close(failpoint.release)
	<-done

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("UpdateIssue enqueue failure: got %d %s, want 503", w.Code, w.Body.String())
	}
	var storedStatus, storedAssigneeType, storedAssigneeID string
	if err := testPool.QueryRow(context.Background(), `SELECT status, assignee_type, assignee_id FROM issue WHERE id = $1`, issue.ID).Scan(&storedStatus, &storedAssigneeType, &storedAssigneeID); err != nil {
		t.Fatalf("read concurrent issue mutation: %v", err)
	}
	if storedStatus != "in_progress" || storedAssigneeType != "member" || storedAssigneeID != testUserID {
		t.Fatalf("concurrent mutation = %q/%q/%q, want in_progress/member/%s", storedStatus, storedAssigneeType, storedAssigneeID, testUserID)
	}
}

func TestUpdateIssueRunEnqueueFailureCompensatesAcrossConcurrentTitleEdit(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{"title": "before title edit", "status": "backlog", "assignee_type": "agent", "assignee_id": agentID})
	seedDupRacePR(t, issue.ID, 999427)
	original := testHandler.TaskService.TxStarter
	failpoint := &blockingFailTxStarter{entered: make(chan struct{}), release: make(chan struct{})}
	testHandler.TaskService.TxStarter = failpoint
	t.Cleanup(func() { testHandler.TaskService.TxStarter = original })
	w, done := httptest.NewRecorder(), make(chan struct{})
	go func() {
		defer close(done)
		testHandler.UpdateIssue(w, withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{"status": "todo"}), "id", issue.ID))
	}()
	<-failpoint.entered
	if _, err := testPool.Exec(context.Background(), `UPDATE issue SET title='concurrent title', updated_at=now() WHERE id=$1`, issue.ID); err != nil {
		t.Fatalf("concurrent title edit: %v", err)
	}
	close(failpoint.release)
	<-done
	var status, title string
	if err := testPool.QueryRow(context.Background(), `SELECT status,title FROM issue WHERE id=$1`, issue.ID).Scan(&status, &title); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusServiceUnavailable || status != "backlog" || title != "concurrent title" {
		t.Fatalf("single result http=%d status=%q title=%q", w.Code, status, title)
	}
	testHandler.TaskService.TxStarter = original
	retry := httptest.NewRecorder()
	testHandler.UpdateIssue(retry, withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{"status": "todo"}), "id", issue.ID))
	if retry.Code != http.StatusOK || taskCountFor(t, issue.ID, agentID) != 1 {
		t.Fatalf("single retry http=%d tasks=%d", retry.Code, taskCountFor(t, issue.ID, agentID))
	}
}

func TestBatchUpdateIssueRunEnqueueFailureCompensatesAcrossConcurrentTitleEdit(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{"title": "before batch title edit", "status": "backlog", "assignee_type": "agent", "assignee_id": agentID})
	seedDupRacePR(t, issue.ID, 999428)
	original := testHandler.TaskService.TxStarter
	failpoint := &blockingFailTxStarter{entered: make(chan struct{}), release: make(chan struct{})}
	testHandler.TaskService.TxStarter = failpoint
	t.Cleanup(func() { testHandler.TaskService.TxStarter = original })
	body := map[string]any{"issue_ids": []string{issue.ID}, "updates": map[string]any{"status": "todo"}}
	w, done := httptest.NewRecorder(), make(chan struct{})
	go func() {
		defer close(done)
		testHandler.BatchUpdateIssues(w, newRequest("POST", "/api/issues/batch?workspace_id="+testWorkspaceID, body))
	}()
	<-failpoint.entered
	if _, err := testPool.Exec(context.Background(), `UPDATE issue SET title='concurrent batch title', updated_at=now() WHERE id=$1`, issue.ID); err != nil {
		t.Fatalf("concurrent batch title edit: %v", err)
	}
	close(failpoint.release)
	<-done
	var status, title string
	if err := testPool.QueryRow(context.Background(), `SELECT status,title FROM issue WHERE id=$1`, issue.ID).Scan(&status, &title); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusServiceUnavailable || status != "backlog" || title != "concurrent batch title" {
		t.Fatalf("batch result http=%d status=%q title=%q", w.Code, status, title)
	}
	testHandler.TaskService.TxStarter = original
	retry := httptest.NewRecorder()
	testHandler.BatchUpdateIssues(retry, newRequest("POST", "/api/issues/batch?workspace_id="+testWorkspaceID, body))
	if retry.Code != http.StatusOK || taskCountFor(t, issue.ID, agentID) != 1 {
		t.Fatalf("batch retry http=%d tasks=%d", retry.Code, taskCountFor(t, issue.ID, agentID))
	}
}

func TestUpdateIssueStatusActivationConflictPreservesCommentTask(t *testing.T) {
	agentID := seededReadyAgentID(t)
	issue := createIssueForTest(t, map[string]any{
		"title":         "comment blocker remains durable",
		"status":        "backlog",
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	seedDupRacePR(t, issue.ID, 999422)

	var runtimeID, commentID, taskID string
	if err := testPool.QueryRow(context.Background(), `SELECT runtime_id FROM agent WHERE id = $1`, agentID).Scan(&runtimeID); err != nil {
		t.Fatalf("load agent runtime: %v", err)
	}
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content)
		VALUES ($1, $2, 'agent', $3, 'durable old-head comment') RETURNING id
	`, issue.ID, testWorkspaceID, agentID).Scan(&commentID); err != nil {
		t.Fatalf("seed comment: %v", err)
	}
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, status, context)
		VALUES ($1, $2, $3, $4, 'queued', jsonb_build_object('head_sha', $5::text)) RETURNING id
	`, agentID, runtimeID, issue.ID, commentID, dupRaceHeadA).Scan(&taskID); err != nil {
		t.Fatalf("seed comment task: %v", err)
	}

	w := httptest.NewRecorder()
	req := withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{"status": "todo"}), "id", issue.ID)
	testHandler.UpdateIssue(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("UpdateIssue blocked activation: got %d %s, want 409", w.Code, w.Body.String())
	}

	var storedStatus, taskStatus, storedHead, storedTrigger string
	if err := testPool.QueryRow(context.Background(), `SELECT status FROM issue WHERE id = $1`, issue.ID).Scan(&storedStatus); err != nil {
		t.Fatalf("read persisted issue: %v", err)
	}
	if err := testPool.QueryRow(context.Background(), `
		SELECT status, COALESCE(context->>'head_sha', ''), trigger_comment_id::text
		FROM agent_task_queue WHERE id = $1
	`, taskID).Scan(&taskStatus, &storedHead, &storedTrigger); err != nil {
		t.Fatalf("read preserved task: %v", err)
	}
	if storedStatus != "backlog" || taskStatus != "queued" || storedHead != dupRaceHeadA || storedTrigger != commentID {
		t.Fatalf("persisted conflict state = issue:%q task:%q head:%q trigger:%q", storedStatus, taskStatus, storedHead, storedTrigger)
	}
	if got := taskCountFor(t, issue.ID, agentID); got != 1 {
		t.Fatalf("blocked activation task count = %d, want only the preserved comment task", got)
	}
}

// TestUpdateIssueHandoffNotePersistsOnTask verifies an assign carrying a
// handoff_note writes that note onto the enqueued task (the daemon then renders
// it), while a suppressed assign with a note enqueues nothing at all.
func TestUpdateIssueHandoffNotePersistsOnTask(t *testing.T) {
	agentID := seededReadyAgentID(t)
	note := "Only touch the login flow."

	issue := createIssueForTest(t, map[string]any{"title": "handoff persist", "status": "todo"})
	w := httptest.NewRecorder()
	req := withURLParam(newRequest("PUT", "/api/issues/"+issue.ID, map[string]any{
		"assignee_type": "agent", "assignee_id": agentID, "handoff_note": note,
	}), "id", issue.ID)
	testHandler.UpdateIssue(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("UpdateIssue with handoff: %d %s", w.Code, w.Body.String())
	}

	var stored string
	if err := testPool.QueryRow(context.Background(), `
		SELECT COALESCE(handoff_note, '') FROM agent_task_queue
		WHERE issue_id = $1 AND agent_id = $2 ORDER BY created_at DESC LIMIT 1
	`, issue.ID, agentID).Scan(&stored); err != nil {
		t.Fatalf("read task handoff_note: %v", err)
	}
	if stored != note {
		t.Fatalf("expected task handoff_note %q, got %q", note, stored)
	}

	// Suppressed assign with a note: no task at all (no run to inject into).
	suppressed := createIssueForTest(t, map[string]any{"title": "handoff suppressed", "status": "todo"})
	w2 := httptest.NewRecorder()
	req2 := withURLParam(newRequest("PUT", "/api/issues/"+suppressed.ID, map[string]any{
		"assignee_type": "agent", "assignee_id": agentID, "handoff_note": note, "suppress_run": true,
	}), "id", suppressed.ID)
	testHandler.UpdateIssue(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("UpdateIssue suppressed handoff: %d %s", w2.Code, w2.Body.String())
	}
	if got := taskCountFor(t, suppressed.ID, agentID); got != 0 {
		t.Fatalf("suppressed handoff should enqueue no task, got %d", got)
	}
}

// TestPreviewIssueTrigger_MalformedBody verifies the endpoint rejects a
// malformed body with 400 rather than a 500 or a silent empty result.
func TestPreviewIssueTrigger_MalformedBody(t *testing.T) {
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/issues/preview-trigger?workspace_id="+testWorkspaceID, strings.NewReader("{not json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", testUserID)
	req.Header.Set("X-Workspace-ID", testWorkspaceID)
	testHandler.PreviewIssueTrigger(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed body: expected 400, got %d: %s", w.Code, w.Body.String())
	}
}
