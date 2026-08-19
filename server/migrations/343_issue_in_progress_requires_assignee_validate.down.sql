-- DP-2027: rollback of the VALIDATE step. PostgreSQL cannot mark a
-- validated constraint NOT VALID again, so recreate the shadow constraint
-- to restore the state immediately after migration 342.
ALTER TABLE issue
    DROP CONSTRAINT IF EXISTS issue_in_progress_requires_assignee;

ALTER TABLE issue
    ADD CONSTRAINT issue_in_progress_requires_assignee
    CHECK (
        status <> 'in_progress'
        OR (assignee_type IS NOT NULL AND assignee_id IS NOT NULL)
    )
    NOT VALID;
