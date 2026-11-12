package raft_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/storage"
	"github.com/panavdoesstuff/raftkv/internal/transport"
)

func cmd(s string, args ...any) []byte { return []byte(fmt.Sprintf(s, args...)) }

func others(c interface{ IDs() []raft.NodeID }, not ...raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
next:
	for _, id := range c.IDs() {
		for _, n := range not {
			if id == n {
				continue next
			}
		}
		out = append(out, id)
	}
	return out
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3)
	c.CheckOneLeader()
	var prev uint64
	for i := 1; i <= 5; i++ {
		idx := c.One(cmd("cmd%d", i), 3, false)
		if prev != 0 && idx != prev+1 {
			t.Fatalf("command %d got index %d, want %d", i, idx, prev+1)
		}
		prev = idx
	}
	// The new leader's no-op occupies the first slot.
	if prev != 6 {
		t.Fatalf("last index = %d, want 6 (no-op + 5 commands)", prev)
	}
	c.CheckLogsAgree()
}

func TestFailAgree(t *testing.T) {
	c := newCluster(t, 3)
	c.One(cmd("a"), 3, false)

	leader := c.CheckOneLeader()
	follower := others(c, leader)[0]
	c.Disconnect(follower)
	c.One(cmd("b"), 2, false)
	c.One(cmd("c"), 2, false)
	last := c.One(cmd("d"), 2, false)
	if _, ok := c.Applied(follower, last); ok {
		t.Fatalf("disconnected follower applied index %d", last)
	}

	c.Connect(follower)
	c.One(cmd("e"), 3, true) // needs all three: the follower must catch up
	if _, ok := c.Applied(follower, last); !ok {
		t.Fatalf("follower never caught up to index %d", last)
	}
	c.CheckLogsAgree()
}

func TestFailNoAgree(t *testing.T) {
	c := newCluster(t, 5)
	c.One(cmd("a"), 5, false)

	leader := c.CheckOneLeader()
	followers := others(c, leader)
	for _, f := range followers[:3] {
		c.Disconnect(f)
	}
	idx, _, ok := c.Node(leader).Propose(cmd("lost"))
	if !ok {
		t.Fatal("leader refused a proposal")
	}
	c.Advance(2 * time.Second)
	if n, _ := c.NCommitted(idx); n != 0 {
		t.Fatalf("index %d committed on %d nodes without a majority", idx, n)
	}

	for _, f := range followers[:3] {
		c.Connect(f)
	}
	c.One(cmd("b"), 5, true)
	c.CheckLogsAgree()
}

func TestRejoin(t *testing.T) {
	c := newCluster(t, 3)
	c.One(cmd("101"), 3, false)

	leader1 := c.CheckOneLeader()
	c.Disconnect(leader1)
	// The stale leader accepts entries it can never commit.
	for _, s := range []string{"102", "103", "104"} {
		if _, _, ok := c.Node(leader1).Propose(cmd("%s", s)); !ok {
			t.Fatal("old leader refused a proposal")
		}
	}
	c.One(cmd("103x"), 2, true) // the new leader commits at the same indexes
	leader2 := c.CheckOneLeader()
	if leader2 == leader1 {
		t.Fatalf("leadership did not move away from %s", leader1)
	}

	// Old leader rejoins alone with the other follower; its entries lose.
	c.Disconnect(leader2)
	c.Connect(leader1)
	c.One(cmd("104x"), 2, true)

	c.Connect(leader2)
	last := c.One(cmd("105"), 3, true)
	c.CheckLogsAgree()
	for _, id := range c.IDs() {
		for _, bad := range []string{"102", "103", "104"} {
			for i := uint64(1); i <= last; i++ {
				if e, ok := c.Applied(id, i); ok && string(e.Data) == bad {
					t.Fatalf("%s applied the overwritten entry %q at index %d", id, bad, i)
				}
			}
		}
	}
}

// TestBackup: a follower 50+ entries behind with a conflicting log must catch
// up in a handful of RPCs, not one RPC per entry.
func TestBackup(t *testing.T) {
	c := newCluster(t, 5)
	c.One(cmd("start"), 5, false)

	leader1 := c.CheckOneLeader()
	rest := others(c, leader1)
	partner, majority := rest[0], rest[1:]

	// leader1 + partner are cut off; leader1 piles up 60 uncommitted entries.
	for _, id := range majority {
		c.Disconnect(id)
	}
	for i := 0; i < 60; i++ {
		if _, _, ok := c.Node(leader1).Propose(cmd("stale%d", i)); !ok {
			t.Fatal("old leader refused a proposal")
		}
	}
	c.Advance(100 * time.Millisecond) // let some reach partner

	// The majority side takes over and commits 60 different entries.
	c.Disconnect(leader1)
	c.Disconnect(partner)
	for _, id := range majority {
		c.Connect(id)
	}
	for i := 0; i < 60; i++ {
		c.One(cmd("fresh%d", i), 3, true)
	}

	// leader1 rejoins. Count AppendEntries until it matches the new leader.
	newLeader := c.CheckOneLeader()
	before := c.Net.AppendEntriesSent()
	c.Connect(leader1)
	target := status(c, newLeader).CommitIndex
	waitFor(t, c, "old leader to catch up", func() bool {
		return status(c, leader1).CommitIndex >= target
	})
	if sent := c.Net.AppendEntriesSent() - before; sent > 40 {
		t.Fatalf("catching up 60 conflicting entries took %d AppendEntries; want fast backtracking", sent)
	}

	c.Connect(partner)
	c.One(cmd("end"), 5, true)
	c.CheckLogsAgree()
}

// TestFigure8 scripts Figure 8 of the paper. S1 is the leader of term 4 with
// log [1:t1, 2:t2]; entry 2 sits on S1, S2 and S3, a majority of five. It must
// NOT count as committed, because it is from an old term. S5 (log [1:t1,
// 2:t3]) then wins term 5 and overwrites it on a majority.
func TestFigure8(t *testing.T) {
	ids := []raft.NodeID{"s1", "s2", "s3", "s4", "s5"}
	e1, e2, e2b := raft.Entry{Index: 1, Term: 1}, raft.Entry{Index: 2, Term: 2}, raft.Entry{Index: 2, Term: 3}

	mk := func(id raft.NodeID, term uint64, log ...raft.Entry) *raft.Raft {
		st := storage.NewMem()
		st.SaveHardState(term, "")
		st.Append(log)
		clk := clock.NewFake() // never advanced: no timers fire, we script everything
		n := raft.New(raft.Config{
			ID: id, Peers: ids, Clock: clk, Storage: st, Seed: testSeed(t),
			Transport: transport.NewMemnet(1, clk).Transport(id), // nobody listening
		})
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(n.Stop)
		return n
	}
	s1 := mk("s1", 4, e1, e2)
	s2, s3, s4 := mk("s2", 3, e1, e2), mk("s3", 3, e1, e2), mk("s4", 3, e1)
	s5 := mk("s5", 3, e1, e2b)

	// (c) S1 leads term 4 and has replicated index 2 to S2 and S3.
	s1.ForceLeader()
	s1.SetMatch("s2", 2)
	s1.SetMatch("s3", 2)
	s1.AdvanceCommit()
	if got := s1.Status().CommitIndex; got != 0 {
		t.Fatalf("S1 committed index %d by counting replicas of an old-term entry (Figure 8c)", got)
	}

	// (d) S1 dies. S5's log is more up to date (last term 3 > 2), so S2, S3
	// and S4 all vote for it in term 5...
	for _, n := range []*raft.Raft{s2, s3, s4} {
		r, err := n.RequestVote(context.Background(), &raft.RequestVoteArgs{
			Term: 5, CandidateID: "s5", LastLogIndex: 2, LastLogTerm: 3})
		if err != nil || !r.VoteGranted {
			t.Fatalf("S5 should win this vote: %+v %v", r, err)
		}
	}
	// ...and overwrites index 2 on S2 and S3: the "majority" copy is gone.
	for _, n := range []*raft.Raft{s2, s3} {
		r, err := n.AppendEntries(context.Background(), &raft.AppendEntriesArgs{
			Term: 5, LeaderID: "s5", PrevLogIndex: 1, PrevLogTerm: 1, Entries: []raft.Entry{e2b}})
		if err != nil || !r.Success {
			t.Fatalf("S2/S3 should accept S5's entry: %+v %v", r, err)
		}
		if l := n.Log(); len(l) != 2 || l[1].Term != 3 {
			t.Fatalf("index 2 not overwritten: %+v", l)
		}
	}
	_ = s5

	// Had S1 committed index 2 it would now be lost. Instead, once S1 gets a
	// current-term entry onto a majority, everything before it commits too.
	s1b := mk("s1", 4, e1, e2)
	s1b.ForceLeader()
	idx, term, ok := s1b.Propose([]byte("x"))
	if !ok || idx != 3 || term != 4 {
		t.Fatalf("Propose = %d,%d,%v", idx, term, ok)
	}
	s1b.SetMatch("s2", 3)
	s1b.SetMatch("s3", 3)
	s1b.AdvanceCommit()
	if got := s1b.Status().CommitIndex; got != 3 {
		t.Fatalf("commit = %d after a term-4 entry reached a majority, want 3", got)
	}
}

// TestReorderedAppends: heavy delay, duplication and drops reorder
// AppendEntries. Entries the cluster reported committed must never vanish, and
// the harness checks State Machine Safety on every apply.
func TestReorderedAppends(t *testing.T) {
	seed := testSeed(t)
	c := newCluster(t, 3)
	c.Net.SetDelay(0, 120*time.Millisecond)
	c.Net.SetDupProb(0.25)
	c.Net.SetDropProb(0.1)
	rng := rand.New(rand.NewSource(seed))

	type rec struct {
		idx uint64
		cmd []byte
	}
	var committed []rec
	for i := 0; i < 25; i++ {
		if i%8 == 4 { // some leader churn too
			id := c.IDs()[rng.Intn(3)]
			c.Disconnect(id)
			c.Advance(400 * time.Millisecond)
			c.Connect(id)
		}
		b := cmd("v%d", i)
		committed = append(committed, rec{c.One(b, 2, true), b})
	}

	c.Net.SetDelay(0, 0)
	c.Net.SetDupProb(0)
	c.Net.SetDropProb(0)
	c.One(cmd("final"), 3, true)
	waitFor(t, c, "all nodes to converge", func() bool {
		for _, id := range c.IDs() {
			if status(c, id).CommitIndex < committed[len(committed)-1].idx {
				return false
			}
		}
		return true
	})
	for _, r := range committed {
		for _, id := range c.IDs() {
			e, ok := c.Applied(id, r.idx)
			if !ok || string(e.Data) != string(r.cmd) {
				t.Fatalf("seed %d: committed entry %q at index %d lost on %s (has %q, applied=%v)",
					seed, r.cmd, r.idx, id, e.Data, ok)
			}
		}
	}
	c.CheckLogsAgree()
}
