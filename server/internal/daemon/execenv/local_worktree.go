package execenv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Local worktree mode gives every task on a local_directory resource its own
// git worktree of the user's repo, created inside the daemon-owned env root.
// Tasks on the same directory then run concurrently instead of queueing on the
// per-path mutex, and each one delivers its work as a branch in the user's own
// repo — discoverable with `git branch`, no new result channel needed.
//
// Three properties this file exists to guarantee:
//
//  1. The agent sees what the user sees. `git worktree add` alone would check
//     out HEAD, silently hiding the user's uncommitted work. We replay the
//     dirty state into the worktree instead (tracked edits via a stash commit,
//     untracked files by copy).
//  2. The user's directory is never written to. Everything — including the
//     sidecar context files Prepare writes — lands inside the worktree, which
//     is disposable. The only lasting effect on the user's repo is the branch.
//  3. Nothing is silently discarded. Whatever the agent leaves uncommitted is
//     committed to the branch before the worktree goes away.

const (
	// localWorktreeDirName is the env-root-relative directory holding the
	// worktree. Kept short: on Windows the worktree path plus the deepest
	// repo path must stay under MAX_PATH for tools that predate long paths.
	localWorktreeDirName = "worktree"

	// gitTimeout bounds every git invocation this file makes. These are all
	// local-only operations (no network), so a slow one means a wedged index
	// lock rather than a slow remote; failing the task beats hanging a daemon
	// slot forever.
	gitTimeout = 2 * time.Minute

	// maxUntrackedFiles / maxUntrackedBytes bound the untracked-file replay.
	// `--exclude-standard` already drops anything gitignored (node_modules,
	// build output, venvs), so a repo hitting these limits has an unusual
	// amount of untracked-but-not-ignored content. We copy up to the bound and
	// report the remainder rather than silently truncating or hanging on a
	// multi-gigabyte copy.
	maxUntrackedFiles = 2000
	maxUntrackedBytes = 200 << 20 // 200 MiB
)

// gitRootLocks serialises git admin operations per repository. Concurrent
// `git worktree add` / `remove` / `prune` on one repo race on the same
// lockfiles (worktrees/, packed-refs.lock, config.lock), and unlike a fetch
// these are fast, so a plain mutex costs nothing. Keyed by the repo root so
// tasks on different repos never wait on each other.
var gitRootLocks sync.Map // gitRoot -> *sync.Mutex

func lockGitRoot(gitRoot string) func() {
	v, _ := gitRootLocks.LoadOrStore(gitRoot, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// LocalWorktreeParams describes the worktree Prepare should build for a
// local_directory task running in worktree mode.
type LocalWorktreeParams struct {
	// LocalPath is the user's configured directory. It may be the repo root
	// or any subdirectory of it; the worktree always covers the whole repo,
	// and the agent's cwd is the matching subdirectory inside it.
	LocalPath string
	// EnvRoot is the daemon-owned task env root. The worktree is created
	// inside it so the ordinary env-root GC reclaims it.
	EnvRoot string
	// AgentName and TaskID name the branch: agent/<name>/<short-task-id>.
	AgentName string
	TaskID    string
}

// LocalWorktree is a prepared worktree plus everything the daemon needs to
// finalize it after the agent exits.
type LocalWorktree struct {
	// GitRoot is the user's repository root — the repo that owns the branch.
	GitRoot string
	// Path is the worktree root inside the env root.
	Path string
	// WorkDir is the agent's cwd: Path, plus the offset of LocalPath inside
	// the repo when the user pointed the resource at a subdirectory.
	WorkDir string
	// Branch is the branch created for this task, in the user's repo.
	Branch string
	// BaseCommit is the commit the worktree started from. Finalize compares
	// the branch tip against it to decide whether the task produced anything.
	BaseCommit string
	// DirtyBaseCaptured records that the user had uncommitted tracked edits
	// which were replayed into the worktree.
	DirtyBaseCaptured bool
	// UntrackedCopied / UntrackedSkipped report the untracked-file replay.
	// A non-zero skip count means the bounds below were hit and the agent is
	// looking at less than the user has on disk; it is logged at warn level so
	// the gap is findable rather than invisible.
	UntrackedCopied  int
	UntrackedSkipped int
}

// LocalWorktreeOutcome is what a finished worktree task delivered.
type LocalWorktreeOutcome struct {
	// Branch is the branch holding the task's work, or "" when the task made
	// no changes at all (a read-only run) — in that case the branch is deleted
	// so it never shows up in the user's `git branch` as an empty artifact.
	Branch string
	// AutoCommitted is true when the agent left uncommitted changes that
	// Finalize committed so they would survive the worktree's removal.
	AutoCommitted bool
}

// PrepareLocalWorktree creates the task's worktree and replays the user's
// uncommitted state into it. It never writes to the user's working tree: the
// dirty state is read through `git stash create`, which builds a commit object
// without touching the index or the files on disk.
func PrepareLocalWorktree(params LocalWorktreeParams, logger *slog.Logger) (*LocalWorktree, error) {
	if params.LocalPath == "" {
		return nil, errors.New("execenv: local worktree requires a local path")
	}
	if params.EnvRoot == "" {
		return nil, errors.New("execenv: local worktree requires an env root")
	}
	if params.TaskID == "" {
		return nil, errors.New("execenv: local worktree requires a task id")
	}

	gitRoot, err := resolveGitRoot(params.LocalPath)
	if err != nil {
		return nil, err
	}

	// The agent's cwd keeps the user's chosen depth: a resource pointed at
	// <repo>/services/api must land the agent in <worktree>/services/api, not
	// at the repo root, or the task's whole notion of "the project" shifts.
	//
	// Canonicalise before the comparison: gitRoot comes back canonical, while
	// the configured path routinely isn't (on macOS every /tmp and /var path is
	// a symlink into /private). Comparing the two forms directly reads a repo
	// root as "outside itself".
	localPath := params.LocalPath
	if resolved, evalErr := filepath.EvalSymlinks(localPath); evalErr == nil {
		localPath = resolved
	}
	rel, err := filepath.Rel(gitRoot, localPath)
	if err != nil {
		return nil, fmt.Errorf("execenv: locate %q inside repo %q: %w", localPath, gitRoot, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("execenv: %q is not inside its repository root %q", localPath, gitRoot)
	}

	worktreePath := filepath.Join(params.EnvRoot, localWorktreeDirName)

	// Everything below mutates the repo's worktree admin state, so take the
	// per-repo lock first — including the stale-path cleanup, which runs `git
	// worktree remove` and would otherwise race a sibling task's `worktree add`.
	unlock := lockGitRoot(gitRoot)
	defer unlock()

	if _, statErr := os.Stat(worktreePath); statErr == nil {
		// Prepare wipes and recreates envRoot, so an existing worktree path
		// means a stale registration in the user's repo pointing here. Remove
		// both rather than failing the task.
		removeLocalWorktreeDir(gitRoot, worktreePath, logger)
	}

	// Self-heal registrations orphaned by a crashed daemon: their env roots are
	// long gone, but the user's repo still lists them. Prune only drops entries
	// whose directory no longer exists, so it can never disturb a live task.
	if out, pruneErr := runGit(gitRoot, "worktree", "prune"); pruneErr != nil && logger != nil {
		logger.Warn("execenv: git worktree prune failed (non-fatal)",
			"git_root", gitRoot, "output", out, "error", pruneErr)
	}

	headSHA, err := runGitTrimmed(gitRoot, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("execenv: repository %q has no commit to branch from "+
			"(worktree mode needs at least one commit; make an initial commit or switch the resource back to in_place): %w", gitRoot, err)
	}

	// `git stash create` builds a commit capturing tracked modifications and
	// returns its sha WITHOUT stashing — the user's index and working tree are
	// untouched. Empty output means the tree is clean. The identity args cover
	// a repo with no user.email configured: writing a commit object needs a
	// committer, and without them the user's uncommitted work would be dropped
	// on a technicality.
	stashSHA, stashErr := runGitTrimmed(gitRoot, append(commitIdentityArgs(gitRoot), "stash", "create")...)
	if stashErr != nil {
		// Don't fail the task over this: the worktree is still usable at HEAD,
		// it just won't carry the user's uncommitted edits. Loud warning, and
		// the caller reports the degraded state to the user.
		if logger != nil {
			logger.Warn("execenv: capture uncommitted changes failed; worktree will start from HEAD",
				"git_root", gitRoot, "error", stashErr)
		}
		stashSHA = ""
	}

	branch := fmt.Sprintf("agent/%s/%s", sanitizeName(params.AgentName), shortID(params.TaskID))
	actualBranch, err := addLocalWorktree(gitRoot, worktreePath, branch, headSHA)
	if err != nil {
		return nil, err
	}

	wt := &LocalWorktree{
		GitRoot:    gitRoot,
		Path:       worktreePath,
		WorkDir:    filepath.Join(worktreePath, rel),
		Branch:     actualBranch,
		BaseCommit: headSHA,
	}

	// Replay tracked edits. Applied as unstaged modifications on top of HEAD so
	// the branch history stays linear and the agent sees the same
	// work-in-progress the user has open in their editor.
	if stashSHA != "" {
		if out, applyErr := runGit(worktreePath, "stash", "apply", stashSHA); applyErr != nil {
			// A conflict here would mean HEAD moved between the two commands.
			// Leave the clean HEAD checkout in place rather than a half-applied
			// tree, and let the user know what they're looking at.
			if logger != nil {
				logger.Warn("execenv: replay uncommitted changes into worktree failed; agent sees HEAD",
					"git_root", gitRoot, "output", out, "error", applyErr)
			}
		} else {
			wt.DirtyBaseCaptured = true
		}
	}

	copied, skipped, err := copyUntrackedFiles(gitRoot, worktreePath, logger)
	if err != nil && logger != nil {
		logger.Warn("execenv: copy untracked files into worktree failed (non-fatal)",
			"git_root", gitRoot, "error", err)
	}
	wt.UntrackedCopied = copied
	wt.UntrackedSkipped = skipped
	if skipped > 0 && logger != nil {
		// Never let this pass as a clean prepare: the agent is about to reason
		// about a tree that is missing files the user can see.
		logger.Warn("execenv: some untracked files were not replayed into the worktree; the agent sees fewer files than the user has",
			"git_root", gitRoot, "copied", copied, "skipped", skipped,
			"max_files", maxUntrackedFiles, "max_bytes", maxUntrackedBytes)
	}

	// Commit the replayed state as a baseline so "did this task change
	// anything?" has an exact answer later. Without it the user's own
	// uncommitted work counts as a change: a read-only task on a repo with an
	// untracked scratch file would auto-commit that file at the end and leave
	// behind a branch the agent never touched. The baseline also makes the
	// delivered branch readable — `git diff <baseline>..<branch>` is precisely
	// the agent's work, with the user's WIP as its own labelled commit.
	if dirty, dirtyErr := worktreeIsDirty(worktreePath); dirtyErr == nil && dirty {
		if baseline, ok := commitBaseline(worktreePath, logger); ok {
			wt.BaseCommit = baseline
		}
	}

	// Note on keeping sidecars out of the delivered branch: we deliberately do
	// NOT write .git/info/exclude here. A linked worktree reads info/exclude
	// from the repo's COMMON git dir, so the only file that would take effect
	// is the user's own .git/info/exclude — editing it would change what `git
	// status` shows in the user's checkout, which is theirs, not ours. Instead
	// the daemon runs the existing CleanupRuntimeConfig + CleanupSidecars pass
	// over the worktree before Finalize, so the sidecars are simply gone by the
	// time anything is committed. That also preserves a genuine agent edit to a
	// tracked CLAUDE.md, which a blanket exclude would have swallowed.

	if logger != nil {
		logger.Info("execenv: local worktree ready",
			"git_root", gitRoot,
			"path", worktreePath,
			"branch", actualBranch,
			"base", headSHA,
			"dirty_base_captured", wt.DirtyBaseCaptured,
			"untracked_copied", copied,
			"untracked_skipped", skipped,
		)
	}
	return wt, nil
}

// Finalize commits whatever the agent left behind, removes the worktree, and
// reports the branch. Called after the agent exits, before the env root is
// handed to the GC.
//
// The auto-commit is the reason a worktree task can't lose work: `git worktree
// remove --force` would happily delete uncommitted edits, and the user would
// have no way to get them back. Committing first turns "the agent edited files"
// into "the branch has a commit", which is the delivery contract for this mode.
func (w *LocalWorktree) Finalize(logger *slog.Logger) LocalWorktreeOutcome {
	if w == nil {
		return LocalWorktreeOutcome{}
	}
	unlock := lockGitRoot(w.GitRoot)
	defer unlock()

	outcome := LocalWorktreeOutcome{Branch: w.Branch}

	if dirty, err := worktreeIsDirty(w.Path); err != nil {
		if logger != nil {
			logger.Warn("execenv: inspect worktree status failed; committing defensively",
				"path", w.Path, "error", err)
		}
		// Unknown state: try to commit anyway. A pointless empty commit is a
		// far cheaper mistake than dropping the agent's edits.
		outcome.AutoCommitted = w.commitAll(logger)
	} else if dirty {
		outcome.AutoCommitted = w.commitAll(logger)
	}

	// A branch still sitting exactly on its base commit means the task changed
	// nothing — the read-only case. Delete it so the user's branch list only
	// ever grows for tasks that actually produced work.
	tip, err := runGitTrimmed(w.Path, "rev-parse", "--verify", "HEAD")
	producedWork := err != nil || tip != w.BaseCommit

	removeLocalWorktreeDir(w.GitRoot, w.Path, logger)

	if !producedWork {
		if out, delErr := runGit(w.GitRoot, "branch", "-D", w.Branch); delErr != nil && logger != nil {
			logger.Warn("execenv: delete empty task branch failed (non-fatal)",
				"branch", w.Branch, "output", out, "error", delErr)
		}
		outcome.Branch = ""
	}

	if logger != nil {
		logger.Info("execenv: local worktree finalized",
			"git_root", w.GitRoot,
			"branch", outcome.Branch,
			"auto_committed", outcome.AutoCommitted,
			"produced_work", producedWork,
		)
	}
	return outcome
}

// commitBaseline records the user's replayed uncommitted state as the first
// commit on the task branch, returning the new tip.
func commitBaseline(worktreePath string, logger *slog.Logger) (string, bool) {
	if !commitEverything(worktreePath, "chore(agent): baseline — uncommitted work from the local directory", logger) {
		return "", false
	}
	tip, err := runGitTrimmed(worktreePath, "rev-parse", "--verify", "HEAD")
	if err != nil {
		if logger != nil {
			logger.Warn("execenv: resolve baseline commit failed", "path", worktreePath, "error", err)
		}
		return "", false
	}
	return tip, true
}

// commitAll stages and commits everything the agent left behind. Returns
// whether a commit was actually created.
func (w *LocalWorktree) commitAll(logger *slog.Logger) bool {
	return commitEverything(w.Path, "chore(agent): uncommitted changes from task", logger)
}

func commitEverything(worktreePath, message string, logger *slog.Logger) bool {
	if out, err := runGit(worktreePath, "add", "-A"); err != nil {
		if logger != nil {
			logger.Warn("execenv: stage worktree changes failed", "path", worktreePath, "output", out, "error", err)
		}
		return false
	}
	// --no-verify: the user's commit hooks are written for the user's own
	// workflow (interactive linters, test suites, signing prompts) and a hook
	// failure here would mean losing the agent's work to save a lint run.
	args := append(commitIdentityArgs(worktreePath), "commit", "--no-verify", "-m", message)
	if out, err := runGit(worktreePath, args...); err != nil {
		if strings.Contains(out, "nothing to commit") {
			return false
		}
		if logger != nil {
			logger.Warn("execenv: commit worktree changes failed", "path", worktreePath, "output", out, "error", err)
		}
		return false
	}
	return true
}

// commitIdentityArgs supplies a committer identity only when the repo doesn't
// already have one. A repo with user.email configured keeps it, so commits
// still look like they came from the user's own setup.
func commitIdentityArgs(dir string) []string {
	if email, err := runGitTrimmed(dir, "config", "user.email"); err == nil && email != "" {
		return nil
	}
	return []string{
		"-c", "user.name=Multica Agent",
		"-c", "user.email=agent@multica.local",
	}
}

func worktreeIsDirty(worktreePath string) (bool, error) {
	out, err := runGit(worktreePath, "status", "--porcelain")
	if err != nil {
		return false, fmt.Errorf("git status: %s: %w", strings.TrimSpace(out), err)
	}
	return strings.TrimSpace(out) != "", nil
}

// removeLocalWorktreeDir unregisters the worktree from the user's repo and
// deletes its directory. The branch is deliberately left alone — it is the
// task's deliverable.
func removeLocalWorktreeDir(gitRoot, worktreePath string, logger *slog.Logger) {
	if out, err := runGit(gitRoot, "worktree", "remove", "--force", worktreePath); err != nil {
		if logger != nil {
			logger.Warn("execenv: git worktree remove failed; pruning registration",
				"path", worktreePath, "output", out, "error", err)
		}
		// Fall back to deleting the directory ourselves and dropping the now
		// dangling registration, so the user's repo isn't left listing a
		// worktree that no longer exists.
		if rmErr := os.RemoveAll(worktreePath); rmErr != nil && logger != nil {
			logger.Warn("execenv: remove worktree directory failed", "path", worktreePath, "error", rmErr)
		}
		if out, pruneErr := runGit(gitRoot, "worktree", "prune"); pruneErr != nil && logger != nil {
			logger.Warn("execenv: git worktree prune failed", "output", out, "error", pruneErr)
		}
	}
}

// resolveGitRoot returns the repository root containing dir. Worktree mode is
// opt-in per resource, so a non-git directory here is a misconfiguration the
// user needs to see and fix — we fail closed with an actionable message rather
// than silently degrading to the in-place lock, which would leave the user
// wondering why their tasks still queue.
func resolveGitRoot(dir string) (string, error) {
	root, err := runGitTrimmed(dir, "rev-parse", "--show-toplevel")
	if err != nil || root == "" {
		return "", fmt.Errorf("execenv: local_directory %q is not a git repository, "+
			"but its project resource is set to execution_mode=worktree; "+
			"initialise a repository there or switch the resource back to in_place", dir)
	}
	// EvalSymlinks so the root matches the path git reports from inside the
	// worktree later — on macOS /tmp vs /private/tmp otherwise produce two
	// different lock keys for one repo.
	if resolved, evalErr := filepath.EvalSymlinks(root); evalErr == nil {
		root = resolved
	}
	return filepath.Clean(root), nil
}

// addLocalWorktree creates the worktree, retrying once under a suffixed branch
// name when the branch already exists (a re-dispatched task keeps its id, so
// its branch can survive from the previous run).
func addLocalWorktree(gitRoot, worktreePath, branch, baseRef string) (string, error) {
	out, err := runGit(gitRoot, "worktree", "add", "-b", branch, worktreePath, baseRef)
	if err != nil && strings.Contains(strings.ToLower(out), "already exists") {
		branch = fmt.Sprintf("%s-%d", branch, time.Now().Unix())
		out, err = runGit(gitRoot, "worktree", "add", "-b", branch, worktreePath, baseRef)
	}
	if err != nil {
		return "", fmt.Errorf("execenv: git worktree add: %s: %w", strings.TrimSpace(out), err)
	}
	return branch, nil
}

// copyUntrackedFiles replays the user's untracked-but-not-ignored files into
// the worktree. `git worktree add` only materialises committed content, so
// without this a brand-new file the user just created would be invisible to the
// agent. Bounded by maxUntrackedFiles / maxUntrackedBytes; the number skipped
// is returned so the caller can tell the user instead of quietly under-copying.
func copyUntrackedFiles(gitRoot, worktreePath string, logger *slog.Logger) (copied, skipped int, err error) {
	// stdout only: a warning on stderr would otherwise be split apart and
	// treated as file paths to copy.
	out, err := runGitTrimmed(gitRoot, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return 0, 0, fmt.Errorf("git ls-files: %w", err)
	}

	var budget int64 = maxUntrackedBytes
	for _, rel := range strings.Split(out, "\x00") {
		if rel == "" {
			continue
		}
		if copied >= maxUntrackedFiles || budget <= 0 {
			skipped++
			continue
		}
		src := filepath.Join(gitRoot, rel)
		info, statErr := os.Lstat(src)
		if statErr != nil || !info.Mode().IsRegular() {
			// Vanished between listing and copying, or not a regular file
			// (socket, fifo, symlink). Nothing useful to replay.
			continue
		}
		if info.Size() > budget {
			skipped++
			continue
		}
		if copyErr := copyUntrackedFile(src, filepath.Join(worktreePath, rel), info.Mode()); copyErr != nil {
			skipped++
			if logger != nil {
				logger.Warn("execenv: copy untracked file into worktree failed", "file", rel, "error", copyErr)
			}
			continue
		}
		budget -= info.Size()
		copied++
	}
	return copied, skipped, nil
}

// copyUntrackedFile copies one untracked file into the worktree, creating
// parent directories and preserving the executable bit — a script the user just
// wrote and hasn't committed has to stay runnable for the agent.
func copyUntrackedFile(src, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Chmod(dst, mode.Perm())
}

// runGit runs git in dir and returns combined output. Callers inspect the
// output for git's own error text, so stdout and stderr stay merged.
func runGit(dir string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), gitTimeout)
	defer cancel()

	full := append([]string{"-C", dir}, args...)
	cmd := exec.CommandContext(ctx, "git", full...)
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// runGitTrimmed runs git for its stdout value, discarding stderr so a
// diagnostic line can't be mistaken for the value (`rev-parse` output, a
// config value, a stash sha).
func runGitTrimmed(dir string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), gitTimeout)
	defer cancel()

	full := append([]string{"-C", dir}, args...)
	cmd := exec.CommandContext(ctx, "git", full...)
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
