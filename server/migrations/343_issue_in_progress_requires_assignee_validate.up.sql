-- I4127.DP / DP-2027: validate the in_progress-requires-assignee CHECK
-- against existing rows. Takes a SHARE UPDATE EXCLUSIVE lock while
-- scanning, which permits normal INSERT/UPDATE/DELETE traffic to continue.
--
-- Any pre-existing zombie row (status='in_progress' with a NULL assignee)
-- causes this migration to fail closed. The live pre-check before migration
-- 342 reported 0 such rows; a production deploy that somehow carries one
-- must repair it first (the zombie-reaper scheduler job does exactly that).
ALTER TABLE issue
    VALIDATE CONSTRAINT issue_in_progress_requires_assignee;
