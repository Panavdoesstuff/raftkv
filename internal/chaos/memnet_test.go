package chaos

import (
	"context"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
)

// waitCluster polls (on the cluster clock) until cond holds or limit passes.
func waitCluster(t *testing.T, tg Target, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	clk := tg.Clock()
	end := clk.Now().Add(limit)
	for clk.Now().Before(end) {
		if cond() {
			return
		}
		tm := clk.NewTimer(20 * time.Millisecond)
		<-tm.C()
	}
	t.Fatalf("timed out waiting for %s", what)
}

func newMem(t *testing.T, seed int64) *MemTarget {
	t.Helper()
	m, err := NewMemTarget(MemOptions{Nodes: 3, Seed: seed, ReadMode: kv.ReadIndex})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		m.Close()
		if t.Failed() {
			t.Logf("seed=%d", seed)
		}
	})
	return m
}

func put(t *testing.T, tg Target, c *kv.Client, k, v string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	tm := tg.Clock().NewTimer(20 * time.Second)
	go func() {
		select {
		case <-tm.C():
			cancel()
		case <-ctx.Done():
		}
	}()
	defer cancel()
	if err := c.Put(ctx, k, v); err != nil {
		t.Fatalf("put %s: %v", k, err)
	}
}

func get(t *testing.T, tg Target, c *kv.Client, k string) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	tm := tg.Clock().NewTimer(20 * time.Second)
	go func() {
		select {
		case <-tm.C():
			cancel()
		case <-ctx.Done():
		}
	}()
	defer cancel()
	v, err := c.Get(ctx, k)
	if err != nil {
		t.Fatalf("get %s: %v", k, err)
	}
	return v
}

func TestMemTargetCrashRestartKeepsData(t *testing.T) {
	m := newMem(t, 11)
	waitCluster(t, m, 10*time.Second, "leader", func() bool { _, ok := m.Leader(); return ok })
	c := kv.NewClient("t", m.Nodes(), m.Caller(), m.Clock())
	put(t, m, c, "k", "v1")
	old, _ := m.Leader()
	if err := m.Crash(old); err != nil {
		t.Fatal(err)
	}
	waitCluster(t, m, 10*time.Second, "new leader", func() bool { l, ok := m.Leader(); return ok && l != old })
	put(t, m, c, "k2", "v2") // writes continue with one node down
	if err := m.Restart(old); err != nil {
		t.Fatal(err)
	}
	waitCluster(t, m, 10*time.Second, "restarted node to catch up", func() bool {
		return m.cl.Node(old).Status().LastApplied >= m.cl.Node(mustLeader(m)).Status().CommitIndex
	})
	if got := get(t, m, c, "k"); got != "v1" {
		t.Fatalf("k = %q after crash/restart", got)
	}
	if errs := m.Harness(); len(errs) > 0 {
		t.Fatal(errs)
	}
}

func mustLeader(m *MemTarget) raft.NodeID { l, _ := m.Leader(); return l }

// A paused node keeps its memory, makes no progress (term, clock), and on
// resume finds the cluster has moved on and rejoins as a follower.
func TestMemTargetPauseFreezesAndResumes(t *testing.T) {
	m := newMem(t, 12)
	waitCluster(t, m, 10*time.Second, "leader", func() bool { _, ok := m.Leader(); return ok })
	c := kv.NewClient("t", m.Nodes(), m.Caller(), m.Clock())
	put(t, m, c, "k", "before")
	old, _ := m.Leader()
	termBefore := m.cl.Node(old).Status().Term
	clkBefore := m.clocks[old].Now()
	if err := m.Pause(old); err != nil {
		t.Fatal(err)
	}
	waitCluster(t, m, 10*time.Second, "another leader", func() bool { l, ok := m.Leader(); return ok && l != old })
	put(t, m, c, "k", "during") // the paused node cannot take part
	// 2 s of cluster time pass; the paused node's clock and term must not move.
	tm := m.Clock().NewTimer(2 * time.Second)
	<-tm.C()
	if d := m.clocks[old].Now().Sub(clkBefore); d > 10*time.Millisecond {
		t.Fatalf("paused node's clock advanced %v", d)
	}
	if st := m.cl.Node(old).Status(); st.Term != termBefore || st.Role != raft.Leader {
		t.Fatalf("paused node changed state: %+v (term was %d)", st, termBefore)
	}
	if err := m.Resume(old); err != nil {
		t.Fatal(err)
	}
	waitCluster(t, m, 10*time.Second, "old leader to step down and catch up", func() bool {
		st := m.cl.Node(old).Status()
		return st.Role != raft.Leader && st.Term > termBefore && st.LastApplied >= m.cl.Node(mustLeader(m)).Status().CommitIndex
	})
	if got := get(t, m, c, "k"); got != "during" {
		t.Fatalf("k = %q, want during", got)
	}
	if errs := m.Harness(); len(errs) > 0 {
		t.Fatal(errs)
	}
}

// A client request to a paused node hangs until the request is cancelled.
func TestMemTargetPausedNodeHangsRequests(t *testing.T) {
	m := newMem(t, 13)
	waitCluster(t, m, 10*time.Second, "leader", func() bool { _, ok := m.Leader(); return ok })
	id := m.Nodes()[0]
	m.Pause(id)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := memCaller{m}.server(ctx, id); done <- err }()
	select {
	case <-done:
		t.Fatal("request to a paused node returned")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("want cancellation error")
	}
}
