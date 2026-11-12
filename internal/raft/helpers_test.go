package raft_test

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/test"
)

var _ test.Node = (*raft.Raft)(nil)

func factory(c test.Config) test.Node {
	return raft.New(raft.Config{
		ID: c.ID, Peers: c.Peers, Transport: c.Transport, Clock: c.Clock,
		Storage: c.Storage, SM: c.SM, Seed: c.Seed,
	})
}

// testSeed returns the seed for a test: RAFT_SEED if set (to reproduce a
// failure), otherwise a fresh random one. The harness prints it on failure.
func testSeed(t testing.TB) int64 {
	if s := os.Getenv("RAFT_SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("bad RAFT_SEED %q: %v", s, err)
		}
		return v
	}
	return time.Now().UnixNano() % 1_000_000
}

// newRealCluster runs on the real clock. Smoke tests only.
func newRealCluster(t *testing.T, n int) *test.Cluster {
	t.Helper()
	return test.StartCluster(t, n, testSeed(t), factory)
}

func newCluster(t *testing.T, n int) *test.Cluster {
	t.Helper()
	return test.StartCluster(t, n, testSeed(t), factory, test.WithFakeClock())
}

// settleTime is the real time given to node goroutines after each fake-clock
// step. Timing-dependent tests run on FakeClock; only smoke tests use the real
// clock (BUILDMAP §3).
const settleTime = 150 * time.Microsecond

// advance moves cluster time forward by d in 10 ms steps, yielding real time
// after each so node goroutines can react.
func advance(c *test.Cluster, d time.Duration) {
	f := c.Clock.(*clock.FakeClock)
	for ; d > 0; d -= 10 * time.Millisecond {
		f.Advance(10 * time.Millisecond)
		time.Sleep(settleTime)
	}
}

// waitFor advances cluster time (up to 10 s) until cond holds.
func waitFor(t *testing.T, c *test.Cluster, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if cond() {
			return
		}
		advance(c, 10*time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func status(c *test.Cluster, id raft.NodeID) raft.Status { return c.Node(id).Status() }
