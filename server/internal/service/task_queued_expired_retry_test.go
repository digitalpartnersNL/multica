package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// seedQueuedExpiredRetryFixture creates an isolated workspace/runtime/agent/
// issue and returns their ids. Everything is removed again in t.Cleanup.
func seedQueuedExpiredRetryFixture(t *testing.T, pool *pgxpool.Pool) (workspaceID, userID, agentID, runtimeID, issueID string) {
	t.Helper()
	ctx := context.Background()
	suffix := time.Now().UnixNano()

	if err := pool.QueryRow(ctx, `INSERT INTO "user" (name, email) VALUES ('Retry User', $1) RETURNING id`,
		fmt.Sprintf("qexp-%d@multica.test", suffix)).Scan(&userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM "user" WHERE id = $1`, userID) })

	if err := pool.QueryRow(ctx, `INSERT INTO workspace (name, slug) VALUES ('qexp ws', $1) RETURNING id`,
		fmt.Sprintf("qexp-%d", suffix)).Scan(&workspaceID); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM workspace WHERE id = $1`, workspaceID) })

	if _, err := pool.Exec(ctx, `INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'owner')`, workspaceID, userID); err != nil {
		t.Fatalf("seed member: %v", err)
	}

	if err := pool.QueryRow(ctx, `
		INSERT INTO agent_runtime (workspace_id, name, runtime_mode, provider, status, device_info, metadata, owner_id)
		VALUES ($1, 'qexp-runtime', 'cloud', 'codex', 'online', '', '{}'::jsonb, $2)
		RETURNING id`, workspaceID, userID).Scan(&runtimeID); err != nil {
		t.Fatalf("seed runtime: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO agent (workspace_id, name, runtime_mode, runtime_config, runtime_id, visibility,
			max_concurrent_tasks, owner_id, instructions, custom_env, custom_args)
		VALUES ($1, 'qexp-agent', 'cloud', '{}'::jsonb, $2, 'workspace', 1, $3, '', '{}'::jsonb, '[]'::jsonb)
		RETURNING id`, workspaceID, runtimeID, userID).Scan(&agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO issue (workspace_id, title, creator_type, creator_id, assignee_type, assignee_id, priority)
		VALUES ($1, 'qexp retry issue', 'member', $2, 'agent', $3, 'medium')
		RETURNING id`, workspaceID, userID, agentID).Scan(&issueID); err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	return workspaceID, userID, agentID, runtimeID, issueID
}

// TestMaybeRetryFailedTask_QueuedExpiredIsRetryable is the regression test for
// I6570.DP / DP-5738: a queued task that expired in the queue
// (failure_reason='queued_expired') MUST get an automatic retry child. Before
// the fix, queued_expired was missing from retryableReasons, so
// MaybeRetryFailedTask declined, the failed task never got a follow-up run,
// and the issue was silently reset to todo with nothing running it — a
// terminal dead end for dispatch work whenever the queue was saturated longer
// than the 2h TTL (e.g. behind a 70+ minute run with queue capacity 1).
func TestMaybeRetryFailedTask_QueuedExpiredIsRetryable(t *testing.T) {
	pool := newTaskClaimRacePool(t)
	ctx := context.Background()
	_, _, agentID, runtimeID, issueID := seedQueuedExpiredRetryFixture(t, pool)
	svc := NewTaskService(db.New(pool), pool, nil, events.New())

	// A task that sat in 'queued' past the TTL and was failed by the
	// ExpireStaleQueuedTasks sweeper: attempt=1, max_attempts=2 (default
	// budget), failure_reason='queued_expired'.
	var parentID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, attempt, max_attempts, failure_reason, error, completed_at)
		VALUES ($1, $2, $3, 'failed', 1, 2, 'queued_expired', 'task expired in queue', now())
		RETURNING id
	`, agentID, runtimeID, issueID).Scan(&parentID); err != nil {
		t.Fatalf("seed expired parent task: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
	})

	parent, err := svc.Queries.GetAgentTask(ctx, util.MustParseUUID(parentID))
	if err != nil {
		t.Fatalf("load parent task: %v", err)
	}

	child, err := svc.MaybeRetryFailedTask(ctx, parent)
	if err != nil {
		t.Fatalf("MaybeRetryFailedTask: %v", err)
	}
	if child == nil {
		t.Fatal("expected a retry child for queued_expired, got nil — issue would be left without a follow-up run (I6570.DP)")
	}
	if child.Status != "queued" {
		t.Fatalf("child status = %q, want queued", child.Status)
	}
	if child.Attempt != 2 {
		t.Fatalf("child attempt = %d, want 2", child.Attempt)
	}
	if !child.ParentTaskID.Valid || util.UUIDToString(child.ParentTaskID) != parentID {
		t.Fatalf("child parent_task_id = %v, want parent %s", child.ParentTaskID, parentID)
	}
	if !child.IssueID.Valid || util.UUIDToString(child.IssueID) != issueID {
		t.Fatalf("child issue link broken: %v", child.IssueID)
	}
	if !child.AgentID.Valid || util.UUIDToString(child.AgentID) != agentID {
		t.Fatalf("child agent link broken: %v", child.AgentID)
	}
}

// TestMaybeRetryFailedTask_QueuedExpiredBudgetExhausted verifies the loop
// guard: once the retry chain has consumed its max_attempts budget, a second
// queued_expired failure is terminal — no infinite requeue loop when the
// queue stays saturated longer than the TTL twice in a row.
func TestMaybeRetryFailedTask_QueuedExpiredBudgetExhausted(t *testing.T) {
	pool := newTaskClaimRacePool(t)
	ctx := context.Background()
	_, _, agentID, runtimeID, issueID := seedQueuedExpiredRetryFixture(t, pool)
	svc := NewTaskService(db.New(pool), pool, nil, events.New())

	// Same shape as above, but the budget is spent: attempt=2 of max_attempts=2.
	var parentID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, attempt, max_attempts, failure_reason, error, completed_at)
		VALUES ($1, $2, $3, 'failed', 2, 2, 'queued_expired', 'task expired in queue', now())
		RETURNING id
	`, agentID, runtimeID, issueID).Scan(&parentID); err != nil {
		t.Fatalf("seed exhausted parent task: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
	})

	parent, err := svc.Queries.GetAgentTask(ctx, util.MustParseUUID(parentID))
	if err != nil {
		t.Fatalf("load parent task: %v", err)
	}

	child, err := svc.MaybeRetryFailedTask(ctx, parent)
	if err != nil {
		t.Fatalf("MaybeRetryFailedTask: %v", err)
	}
	if child != nil {
		t.Fatalf("expected no retry once the budget is exhausted, got child %s", util.UUIDToString(child.ID))
	}
}
