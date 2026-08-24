// Package testutil holds small helpers shared by the server's test suites.
package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Suite advisory-lock keys. Every DB-backed test suite that owns a fixed-slug
// fixture on the shared test database serializes same-suite runs on its own
// key. Without this, two concurrent runs of the SAME suite (e.g. two agents
// running `go test ./internal/handler/` at the same time) share one fixture
// workspace — slug "handler-tests" — and each run's TestMain setup/teardown
// deletes workspace rows with that slug, so the second run deletes the first
// run's fixture mid-suite. I4187.DP (19 aug 2026, Ron): the baseline run
// collapsed with 459 FK violations, the hertest with 751 ("workspace not
// found"), both caused by a parallel suite run of another agent on the same
// test database.
//
// Different suites use different keys so unrelated suites can still run in
// parallel: they own disjoint fixtures (handler-tests vs integration-tests).
const (
	// HandlerSuiteLockKey serializes `go test ./internal/handler/` runs.
	HandlerSuiteLockKey int64 = 4_187_000_001
	// IntegrationSuiteLockKey serializes `go test ./cmd/server/` runs.
	IntegrationSuiteLockKey int64 = 4_187_000_002
)

// acquireSuiteLockTimeout bounds how long a suite waits for a concurrent run
// of the same suite to finish before failing with a clear message instead of
// hanging forever.
const acquireSuiteLockTimeout = 15 * time.Minute

// SuiteLock is a held session-scoped advisory lock. It is tied to one checked
// out connection; releasing it returns the connection to the pool.
type SuiteLock struct {
	conn *pgxpool.Conn
	key  int64
}

// AcquireSuiteLock takes a session-scoped advisory lock for the whole test
// run. The lock is session-scoped, so it dies with the process: a crashed or
// SIGKILLed suite can never leave a stale lock behind. A concurrent run of
// the same suite makes this call wait (logged once) instead of letting both
// runs corrupt each other's fixture.
func AcquireSuiteLock(ctx context.Context, pool *pgxpool.Pool, key int64) (*SuiteLock, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	warned := false
	deadline := time.Now().Add(acquireSuiteLockTimeout)
	for {
		var acquired bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil {
			conn.Release()
			return nil, err
		}
		if acquired {
			return &SuiteLock{conn: conn, key: key}, nil
		}
		if !warned {
			fmt.Printf("test suite: another run of this suite holds advisory lock %d on the test database; waiting for it to finish (timeout %s)...\n", key, acquireSuiteLockTimeout)
			warned = true
		}
		if time.Now().After(deadline) {
			conn.Release()
			return nil, fmt.Errorf("another run of this suite holds advisory lock %d on the test database; timed out after %s. Serialize suite runs on the test database (I4187.DP)", key, acquireSuiteLockTimeout)
		}
		time.Sleep(5 * time.Second)
	}
}

// Release unlocks and returns the connection to the pool. Safe to call on a
// nil lock. If the process exits without releasing (os.Exit, crash), the
// session-scoped lock is released by Postgres when the connection dies.
func (l *SuiteLock) Release(ctx context.Context) {
	if l == nil || l.conn == nil {
		return
	}
	_, _ = l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, l.key)
	l.conn.Release()
}
