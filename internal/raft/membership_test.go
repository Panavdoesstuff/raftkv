package raft_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/test"
)

func rf(c *test.Cluster, id raft.NodeID) *raft.Raft { return c.Node(id).(*raft.Raft) }

// leaderAndOthers returns the leader and the other original nodes, in ID order.
func leaderAndOthers(c *test.Cluster) (raft.NodeID, []raft.NodeID) {
	l := c.CheckOneLeader()
	var others []raft.NodeID
	for _, id := range c.IDs() {
		if id != l {
			others = append(others, id)
		}
	}
	return l, others
}

// drive runs f on its own goroutine and advances cluster time until it returns
// (at most limit of cluster time).
func drive(t *testing.T, c *test.Cluster, limit time.Duration, f func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f() }()
	for waited := time.Duration(0); waited < limit; waited += 10 * time.Millisecond {
		select {
		case err := <-done:
			return err
		default:
		}
		advance(c, 10*time.Millisecond)
	}
	select {
	case err := <-done:
		return err
	default:
	}
	t.Fatalf("call did not return within %v of cluster time", limit)
	return nil
}

// start runs f on its own goroutine and returns its result channel.
func start(f func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- f() }()
	return done
}

func voters(r *raft.Raft) string { return fmt.Sprint(r.Config().Voters) }

func addServer(t *testing.T, c *test.Cluster, l raft.NodeID, id raft.NodeID) error {
	t.Helper()
	return drive(t, c, 30*time.Second, func() error { return rf(c, l).AddServer(context.Background(), id, "mem:"+string(id)) })
}

func removeServer(t *testing.T, c *test.Cluster, l raft.NodeID, id raft.NodeID) error {
	t.Helper()
	return drive(t, c, 30*time.Second, func() error { return rf(c, l).RemoveServer(context.Background(), id) })
}

func terms(c *test.Cluster) map[raft.NodeID]uint64 {
	m := map[raft.NodeID]uint64{}
	for _, id := range c.IDs() {
		m[id] = status(c, id).Term
	}
	return m
}

func sameTerms(t *testing.T, what string, before, after map[raft.NodeID]uint64) {
	t.Helper()
	for id, b := range before {
		if after[id] != b {
			t.Fatalf("%s: node %s term moved %d -> %d (all before: %v, after: %v)", what, id, b, after[id], before, after)
		}
	}
}

// TestAddServerBasic: a new node with an empty log joins as a learner, catches
// up, becomes a voter, and the 4-node cluster commits.
func TestAddServerBasic(t *testing.T) {
	c := newCluster(t, 3)
	l, _ := leaderAndOthers(c)
	for i := 0; i < 5; i++ {
		c.One([]byte(fmt.Sprintf("pre%d", i)), 3, true)
	}
	c.AddNode("n3")
	if err := addServer(t, c, l, "n3"); err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	if got := voters(rf(c, l)); got != "[n0 n1 n2 n3]" {
		t.Fatalf("leader voters %s", got)
	}
	idx := c.One([]byte("post"), 4, true)
	waitFor(t, c, "n3 config", func() bool { return rf(c, "n3").Config().IsVoter("n3") })
	if n, _ := c.NCommitted(idx); n != 4 {
		t.Fatalf("%d nodes applied the post-add entry, want 4", n)
	}
	c.CheckLogsAgree()
}

// TestRemoveFollower: the removed follower leaves every quorum. With two
// voters left, the leader cannot commit without the other follower.
func TestRemoveFollower(t *testing.T) {
	c := newCluster(t, 3)
	l, others := leaderAndOthers(c)
	f, g := others[0], others[1]
	c.One([]byte("a"), 3, true)
	if err := removeServer(t, c, l, f); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	if got, want := voters(rf(c, l)), fmt.Sprint([]raft.NodeID{l, g}); got != want && got != fmt.Sprint([]raft.NodeID{g, l}) {
		t.Fatalf("voters %s, want {%s %s}", got, l, g)
	}
	c.One([]byte("b"), 2, true)

	c.Crash(g) // quorum of {l, g} is 2: l alone cannot commit
	idx := c.Propose([]byte("c"))
	if idx == 0 {
		t.Fatal("leader refused a proposal")
	}
	advance(c, 2*time.Second)
	if ci := status(c, l).CommitIndex; ci >= idx {
		t.Fatalf("entry %d committed (commit=%d) without the other voter: the removed node was counted", idx, ci)
	}
	c.Restart(g)
	waitFor(t, c, "commit after g returns", func() bool { return status(c, l).CommitIndex >= idx })
}

// TestRemoveLeader: a leader that removes itself keeps leading, without
// counting itself, until the entry commits; then it steps down and never
// campaigns again.
func TestRemoveLeader(t *testing.T) {
	c := newCluster(t, 3)
	l, others := leaderAndOthers(c)
	c.One([]byte("a"), 3, true)

	// Both other voters down: the removal cannot commit, because the new
	// config is {a, b} and the leader is not in it.
	c.Crash(others[0])
	c.Crash(others[1])
	res := start(func() error { return rf(c, l).RemoveServer(context.Background(), l) })
	advance(c, 2*time.Second)
	select {
	case err := <-res:
		t.Fatalf("RemoveServer returned %v without a quorum of the new config", err)
	default:
	}
	if s := status(c, l); s.Role != raft.Leader {
		t.Fatalf("leader stepped down before its removal committed: %+v", s)
	}
	c.Restart(others[0])
	c.Restart(others[1])
	var err error
	for i := 0; i < 3000 && err == nil; i++ {
		select {
		case err = <-res:
			if err != nil {
				t.Fatalf("RemoveServer: %v", err)
			}
			i = 3000
		default:
			advance(c, 10*time.Millisecond)
		}
	}
	waitFor(t, c, "old leader stepped down", func() bool { return status(c, l).Role != raft.Leader })

	termAtExit := status(c, l).Term
	nl := c.CheckOneLeader()
	if nl == l {
		t.Fatal("removed node is still the leader")
	}
	c.One([]byte("b"), 2, true)
	advance(c, 5*time.Second)
	if s := status(c, l); s.Role == raft.Leader || s.Role == raft.Candidate || s.Term != termAtExit {
		t.Fatalf("removed leader kept campaigning: %+v (term was %d)", s, termAtExit)
	}
	c.CheckTermLeaders()
}

// TestLearnerNeverCountsForCommit: a learner is replicated to but is not a
// voter. With the two real followers down, leader + up-to-date learner must
// not commit.
func TestLearnerNeverCountsForCommit(t *testing.T) {
	c := newCluster(t, 3)
	l, others := leaderAndOthers(c)
	c.One([]byte("a"), 3, true)
	c.AddNode("n3")
	c.Disconnect("n3") // so AddServer stops at the learner stage
	if err := addServer(t, c, l, "n3"); !errors.Is(err, raft.ErrCatchUpFailed) {
		t.Fatalf("AddServer to an unreachable node: %v, want ErrCatchUpFailed", err)
	}
	if cfg := rf(c, l).Config(); !cfg.IsLearner("n3") || cfg.IsVoter("n3") {
		t.Fatalf("n3 should be a learner, config %+v", cfg)
	}
	c.Connect("n3")
	c.One([]byte("b"), 3, true) // commits on the voters; n3 replicates too
	waitFor(t, c, "learner catches up", func() bool { return status(c, "n3").LastIndex >= status(c, l).LastIndex })

	c.Crash(others[0])
	c.Crash(others[1])
	idx := c.Propose([]byte("c"))
	if idx == 0 {
		t.Fatal("leader refused a proposal")
	}
	waitFor(t, c, "learner receives the entry", func() bool { return status(c, "n3").LastIndex >= idx })
	advance(c, 2*time.Second)
	if ci := status(c, l).CommitIndex; ci >= idx {
		t.Fatalf("entry %d committed (commit=%d) with only the leader and a learner up", idx, ci)
	}
}

// TestAddServerCatchUpFails: a learner that cannot catch up within 10 rounds
// of one election timeout each makes AddServer fail, and it is not promoted.
func TestAddServerCatchUpFails(t *testing.T) {
	c := newCluster(t, 3)
	l, _ := leaderAndOthers(c)
	c.One([]byte("a"), 3, true)
	c.AddNode("n3")
	c.Disconnect("n3")
	t0 := c.Clock.Now()
	err := addServer(t, c, l, "n3")
	took := c.Clock.Now().Sub(t0)
	if !errors.Is(err, raft.ErrCatchUpFailed) {
		t.Fatalf("AddServer: %v, want ErrCatchUpFailed", err)
	}
	if took < 9*300*time.Millisecond {
		t.Fatalf("gave up after %v: fewer than 10 rounds of an election timeout", took)
	}
	if rf(c, l).Config().IsVoter("n3") {
		t.Fatal("n3 was promoted without catching up")
	}
	c.One([]byte("b"), 3, true) // the cluster is unaffected
}

// TestConcurrentChangeRejected: while a config entry is uncommitted every
// other change is refused, and refused immediately.
func TestConcurrentChangeRejected(t *testing.T) {
	c := newCluster(t, 3)
	l, others := leaderAndOthers(c)
	c.One([]byte("a"), 3, true)
	c.AddNode("n3")
	c.AddNode("n4")
	c.Crash(others[0]) // no quorum: the learner entry stays uncommitted
	c.Crash(others[1])

	first := start(func() error { return rf(c, l).AddServer(context.Background(), "n3", "mem:n3") })
	waitFor(t, c, "learner entry appended", func() bool { return rf(c, l).Config().IsLearner("n3") })

	err := drive(t, c, 5*time.Second, func() error { return rf(c, l).RemoveServer(context.Background(), others[0]) })
	if !errors.Is(err, raft.ErrConfigPending) {
		t.Fatalf("RemoveServer during a pending change: %v, want ErrConfigPending", err)
	}
	err = drive(t, c, 5*time.Second, func() error { return rf(c, l).AddServer(context.Background(), "n4", "mem:n4") })
	if !errors.Is(err, raft.ErrConfigPending) {
		t.Fatalf("AddServer during a pending change: %v, want ErrConfigPending", err)
	}
	if cfg := rf(c, l).Config(); cfg.IsMember("n4") || len(cfg.Voters) != 3 {
		t.Fatalf("a rejected change altered the config: %+v", cfg)
	}

	c.Restart(others[0])
	c.Restart(others[1])
	for i := 0; ; i++ {
		select {
		case err := <-first:
			if err != nil {
				t.Fatalf("first AddServer: %v", err)
			}
			if err := addServer(t, c, l, "n4"); err != nil {
				t.Fatalf("AddServer after the first committed: %v", err)
			}
			if got := voters(rf(c, l)); got != "[n0 n1 n2 n3 n4]" {
				t.Fatalf("voters %s", got)
			}
			return
		default:
			if i > 3000 {
				t.Fatal("first AddServer never finished")
			}
			advance(c, 10*time.Millisecond)
		}
	}
}

// TestChangeNeedsCurrentTermCommit: a leader that has not committed an entry
// in its own term refuses config changes (thesis §4.1, Ongaro's 2015 bug).
func TestChangeNeedsCurrentTermCommit(t *testing.T) {
	c := newCluster(t, 3)
	l, others := leaderAndOthers(c)
	c.One([]byte("a"), 3, true)
	f := others[0]
	c.Disconnect(f)
	rf(c, f).SetTerm(status(c, l).Term + 5)
	rf(c, f).ForceLeader() // leader of a fresh term; its commit index is from an older one
	if err := rf(c, f).AddServer(context.Background(), "n9", "x"); !errors.Is(err, raft.ErrNotReady) {
		t.Fatalf("AddServer before a current-term commit: %v, want ErrNotReady", err)
	}
	if err := rf(c, f).RemoveServer(context.Background(), others[1]); !errors.Is(err, raft.ErrNotReady) {
		t.Fatalf("RemoveServer before a current-term commit: %v, want ErrNotReady", err)
	}
}

// TestRemovedNodeNoDisruption: a removed follower never hears of its removal
// and keeps timing out. With PreVote it never gets to raise its term, and the
// rest of the cluster is not disturbed either.
func TestRemovedNodeNoDisruption(t *testing.T) {
	c := newCluster(t, 3)
	l, others := leaderAndOthers(c)
	f := others[0]
	c.One([]byte("a"), 3, true)
	if err := removeServer(t, c, l, f); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	before := terms(c)
	advance(c, 10*time.Second)
	sameTerms(t, "after 10 s with a removed node timing out", before, terms(c))
	if s := status(c, f); s.Role != raft.Follower {
		t.Fatalf("removed node changed role: %+v", s)
	}
	if got := c.CheckOneLeader(); got != l {
		t.Fatalf("leadership moved from %s to %s", l, got)
	}
	c.One([]byte("b"), 2, true)
}

// TestRejoinAfterPartitionPreVote: a node cut off for a long time keeps its
// term (its PreVotes get no answers), so when it rejoins nothing is disturbed.
func TestRejoinAfterPartitionPreVote(t *testing.T) {
	c := newCluster(t, 3)
	l, others := leaderAndOthers(c)
	f := others[0]
	c.One([]byte("a"), 3, true)
	before := terms(c)
	c.Partition([]raft.NodeID{f}, []raft.NodeID{l, others[1]})
	advance(c, 5*time.Second)
	c.One([]byte("b"), 2, true)
	c.Heal()
	advance(c, 2*time.Second)
	sameTerms(t, "partition and rejoin", before, terms(c))
	if got := c.CheckOneLeader(); got != l {
		t.Fatalf("leadership moved from %s to %s", l, got)
	}
	c.One([]byte("c"), 3, true)
}

// TestConfigRevertsOnTruncate: a config entry that only an isolated leader
// holds is overwritten by the new leader's log; the isolated node must go back
// to the previous config.
func TestConfigRevertsOnTruncate(t *testing.T) {
	c := newCluster(t, 3)
	l, others := leaderAndOthers(c)
	c.One([]byte("a"), 3, true)
	c.AddNode("n3")
	c.Partition([]raft.NodeID{l}, []raft.NodeID{others[0], others[1], "n3"})
	res := start(func() error { return rf(c, l).AddServer(context.Background(), "n3", "mem:n3") })
	waitFor(t, c, "uncommitted config entry", func() bool { return rf(c, l).Config().IsLearner("n3") })
	if status(c, l).ConfigIndex == 0 {
		t.Fatal("no config index while a config entry is in the log")
	}
	// The majority elects a new leader and commits entries at the same indexes.
	waitFor(t, c, "new leader", func() bool {
		for _, id := range others {
			if status(c, id).Role == raft.Leader {
				return true
			}
		}
		return false
	})
	c.One([]byte("b"), 2, true)
	c.One([]byte("c"), 2, true)
	c.Heal()
	waitFor(t, c, "old leader reverts its config", func() bool {
		cfg := rf(c, l).Config()
		return status(c, l).Role == raft.Follower && !cfg.IsMember("n3") && status(c, l).ConfigIndex == 0
	})
	if got := voters(rf(c, l)); got != "[n0 n1 n2]" {
		t.Fatalf("voters after revert: %s", got)
	}
	// The blocked AddServer polls on the cluster clock, so keep it moving.
	var err error
	for i := 0; i < 1000; i++ {
		select {
		case err = <-res:
			i = 1000
		default:
			advance(c, 10*time.Millisecond)
		}
	}
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("AddServer on the deposed leader: %v, want ErrNotLeader", err)
	}
}

// TestConfigSurvivesSnapshotAndRestart: the config entries a snapshot covers
// are gone from the log, so the snapshot must carry the config. Checked across
// a restart and across InstallSnapshot to a brand-new node.
func TestConfigSurvivesSnapshotAndRestart(t *testing.T) {
	c := newCluster(t, 3)
	l, others := leaderAndOthers(c)
	c.AddNode("n3")
	if err := addServer(t, c, l, "n3"); err != nil {
		t.Fatalf("AddServer n3: %v", err)
	}
	for i := 0; i < 5; i++ {
		c.One([]byte(fmt.Sprintf("x%d", i)), 4, true)
	}
	snapAll := func() {
		waitFor(t, c, "applied everywhere", func() bool {
			for _, id := range c.IDs() {
				if s := status(c, id); s.LastApplied < s.CommitIndex || s.CommitIndex < status(c, l).CommitIndex {
					return false
				}
			}
			return true
		})
		for _, id := range c.IDs() {
			rf(c, id).Snapshot(status(c, id).LastApplied, nil)
			if status(c, id).SnapIndex == 0 {
				t.Fatalf("%s did not snapshot", id)
			}
			if len(rf(c, id).Log()) != 0 {
				t.Fatalf("%s kept %d log entries after snapshotting everything", id, len(rf(c, id).Log()))
			}
		}
	}
	snapAll()

	// Restart: the voter set must come back from the snapshot, not from the
	// bootstrap list (which has no n3).
	c.Restart(others[0])
	if got := voters(rf(c, others[0])); got != "[n0 n1 n2 n3]" {
		t.Fatalf("voters after restart from snapshot: %s", got)
	}
	// A new node must get the config through InstallSnapshot.
	c.AddNode("n4")
	if err := addServer(t, c, l, "n4"); err != nil {
		t.Fatalf("AddServer n4 after compaction: %v", err)
	}
	waitFor(t, c, "n4 adopts the config", func() bool { return rf(c, "n4").Config().IsVoter("n4") })
	if got := voters(rf(c, "n4")); got != "[n0 n1 n2 n3 n4]" {
		t.Fatalf("n4 voters: %s", got)
	}
	c.One([]byte("post"), 5, true)
}
