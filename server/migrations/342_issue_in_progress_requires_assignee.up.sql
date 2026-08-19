-- I4127.DP / DP-2027 (C6): in_progress requires an assignee at the DB layer.
--
-- The application-layer gate (validateInProgressRequiresAssignee, I4127.DP)
-- refuses in_progress-without-assignee on the HTTP create/update/batch
-- routes, and IssueService.Create now enforces the same invariant. But a
-- future entry point (MCP, backfill, admin tool) that writes the issue table
-- directly would bypass both. This CHECK makes the invariant fail-closed at
-- the lowest layer: no route, present or future, can persist a zombie
-- (status='in_progress' AND assignee IS NULL).
--
-- The shape mirrors the handler gate exactly: in_progress requires BOTH
-- assignee_type and assignee_id to be non-NULL (a partial pair is still a
-- zombie — no run can be enqueued for it).
--
-- Added NOT VALID so the ADD takes no table scan under ACCESS EXCLUSIVE
-- lock; migration 343 validates existing rows separately (SHARE UPDATE
-- EXCLUSIVE, non-blocking for normal traffic). Live pre-check before this
-- migration: 0 rows in the zombie shape.
ALTER TABLE issue
    ADD CONSTRAINT issue_in_progress_requires_assignee
    CHECK (
        status <> 'in_progress'
        OR (assignee_type IS NOT NULL AND assignee_id IS NOT NULL)
    )
    NOT VALID;
