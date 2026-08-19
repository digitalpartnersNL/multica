package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// JobNameReapZombieIssues is the canonical name used in audit rows.
// Stable across releases — do not rename without a migration.
const JobNameReapZombieIssues = "reap_zombie_issues"

// zombieScanLimit caps how many zombies a single run repairs. The scan is
// ordered oldest-first; a database that somehow accumulated more than the
// cap gets them in batches on successive ticks instead of one giant
// unbounded UPDATE. With the I4127.DP DB CHECK constraint in place the
// steady-state expectation is zero rows, so the cap is pure belt-and-braces.
const zombieScanLimit = 1000

// ZombieReaperJob returns the JobSpec that drives the periodic scan for
// zombie issues — rows stuck in `in_progress` with no assignee (I4127.DP).
//
// An in_progress issue without an assignee can never have a run enqueued
// (the enqueue path requires a valid assignee), so it sits in_progress
// forever, counts against the queue ceiling, and has no owner. The HTTP
// handler gate, the IssueService.Create gate, and the migration-342 DB
// CHECK all refuse to CREATE such a row — but a row created before the
// constraint (legacy), or written by a future route that bypasses all
// three (raw SQL backfill, admin tooling, a dropped constraint), would
// otherwise go undetected. This job detects those rows and repairs them
// to `todo`, which is the legitimate unassigned state.
//
// The spec follows the same operational envelope as the task_usage
// hourly job: a 15-minute cadence with a 5-minute schedule delay keeps
// the scan cheap while still catching a zombie within ~20 minutes of it
// appearing. The handler is idempotent (it only touches rows that still
// match the zombie predicate), so AllowStaleReentry + retry are safe.
func ZombieReaperJob(pool *pgxpool.Pool) JobSpec {
	return JobSpec{
		Name:              JobNameReapZombieIssues,
		Cadence:           15 * time.Minute,
		ScheduleDelay:     5 * time.Minute,
		CatchUpMode:       CatchUpLatestOnly,
		CatchUpWindow:     24 * time.Hour,
		RunTimeout:        10 * time.Minute,
		StaleTimeout:      15 * time.Minute,
		HeartbeatInterval: 30 * time.Second,
		AllowStaleReentry: true,
		MaxAttempts:       3,
		RetryBackoff: []time.Duration{
			1 * time.Minute,
			5 * time.Minute,
			15 * time.Minute,
		},
		Scopes:  StaticScopes(ScopeGlobal),
		Handler: makeZombieReaperHandler(pool),
	}
}

// makeZombieReaperHandler scans for zombie issues (status='in_progress'
// with a NULL assignee) and repairs each to 'todo'. RowsAffected is the
// number of issues repaired; the result map carries the ids for the audit
// row so operators can see what was touched without grepping logs.
func makeZombieReaperHandler(pool *pgxpool.Pool) Handler {
	return func(ctx context.Context, in HandlerInput) (HandlerResult, error) {
		rows, err := pool.Query(ctx, `
			SELECT id, workspace_id, number
			FROM issue
			WHERE status = 'in_progress'
			  AND (assignee_type IS NULL OR assignee_id IS NULL)
			ORDER BY created_at
			LIMIT $1
		`, zombieScanLimit)
		if err != nil {
			return HandlerResult{}, fmt.Errorf("scan zombie issues: %w", err)
		}

		type zombie struct {
			id          string
			workspaceID string
			number      int32
		}
		var zombies []zombie
		for rows.Next() {
			var z zombie
			if err := rows.Scan(&z.id, &z.workspaceID, &z.number); err != nil {
				rows.Close()
				return HandlerResult{}, fmt.Errorf("scan zombie issue row: %w", err)
			}
			zombies = append(zombies, z)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return HandlerResult{}, fmt.Errorf("iterate zombie issues: %w", err)
		}

		if len(zombies) == 0 {
			return HandlerResult{
				RowsAffected: 0,
				Result:       map[string]any{"repaired": 0},
			}, nil
		}

		repairedIDs := make([]string, 0, len(zombies))
		for _, z := range zombies {
			// Re-check inside the UPDATE so a concurrent legitimate
			// re-assignment between the scan and the repair is never
			// clobbered: only rows still matching the zombie predicate
			// are touched.
			tag, err := pool.Exec(ctx, `
				UPDATE issue
				SET status = 'todo', updated_at = now()
				WHERE id = $1
				  AND status = 'in_progress'
				  AND (assignee_type IS NULL OR assignee_id IS NULL)
			`, z.id)
			if err != nil {
				return HandlerResult{}, fmt.Errorf("repair zombie issue %s: %w", z.id, err)
			}
			if tag.RowsAffected() == 1 {
				repairedIDs = append(repairedIDs, z.id)
				slog.Warn("zombie issue repaired to todo",
					"issue_id", z.id,
					"workspace_id", z.workspaceID,
					"number", z.number,
					"job", JobNameReapZombieIssues)
			}
		}

		// Light heartbeat at the end keeps stale_after fresh for jobs
		// that ran much shorter than HeartbeatInterval.
		if in.Heartbeat != nil {
			_ = in.Heartbeat(ctx)
		}

		return HandlerResult{
			RowsAffected: int64(len(repairedIDs)),
			Result: map[string]any{
				"repaired":    len(repairedIDs),
				"repaired_ids": repairedIDs,
			},
		}, nil
	}
}
