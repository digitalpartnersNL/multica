-- I4127.DP / DP-2027: rollback of the in_progress-requires-assignee CHECK.
--
-- Dropping the constraint re-opens the DB layer for zombies. That is the
-- deliberate choice of a down migration — the application gates (handler +
-- IssueService.Create) remain in place, so the HTTP routes and the service
-- still refuse the zombie shape; only direct-SQL writers escape again.
ALTER TABLE issue
    DROP CONSTRAINT IF EXISTS issue_in_progress_requires_assignee;
