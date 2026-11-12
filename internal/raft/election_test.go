package raft_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/test"
)

// TestInitialElection is the one real-clock smoke test: it covers real timers
// and goroutine scheduling. Everything else runs on FakeClock.
func TestInitialElection(t *testing.T) {
	c := newRealCluster(t, 3)
	leader := c.CheckOneLeader()
	time.Sleep(300 * time.Millisecond) // heartbeats must keep everyone quiet
	checkSettled(t, c, leader)
}

func TestInitialElectionFakeClock(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.CheckOneLeader()
	advance(c, time.Second) // heartbeats must keep everyone quiet
	checkSettled(t, c, leader)
}

func checkSettled(t *testing.T, c *test.Cluster, leader raft.NodeID) {
	t.Helper()

	term := status(c, leader).Term
	if term == 0 {
		t.Fatalf("leader %s is still in term 0", leader)
	}
	leaders := 0
	for _, id := range c.IDs() {
		s := status(c, id)
		if s.Term != term {
			t.Errorf("%s is in term %d, want %d", id, s.Term, term)
		}
		if s.Role == raft.Leader {
			leaders++
		} else if s.Role != raft.Follower {
			t.Errorf("%s is %s, want follower", id, s.Role)
		}
	}
	if leaders != 1 {
		t.Fatalf("want exactly one leader, got %d", leaders)
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	c := newCluster(t, 1)
	if l := c.CheckOneLeader(); l != "n0" {
		t.Fatalf("leader = %s", l)
	}
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3)
	old := c.CheckOneLeader()
	oldTerm := status(c, old).Term

	c.Disconnect(old)
	var next raft.NodeID
	waitFor(t, c, "a new leader", func() bool {
		for _, id := range c.IDs() {
			if s := status(c, id); id != old && s.Role == raft.Leader && s.Term > oldTerm {
				next = id
				return true
			}
		}
		return false
	})
	newTerm := status(c, next).Term

	// The old leader still believes it leads until it hears the higher term.
	if s := status(c, old); s.Role != raft.Leader || s.Term != oldTerm {
		t.Fatalf("disconnected leader changed state on its own: %+v", s)
	}

	c.Connect(old)
	waitFor(t, c, "old leader to step down", func() bool {
		s := status(c, old)
		return s.Role == raft.Follower && s.Term == newTerm
	})
	advance(c, time.Second)
	if got := c.CheckOneLeader(); got != next {
		t.Fatalf("leadership moved from %s to %s after the old leader rejoined", next, got)
	}
	if s := status(c, next); s.Term != newTerm {
		t.Fatalf("rejoin disturbed the cluster: term %d -> %d", newTerm, s.Term)
	}
}

func TestNoLeaderWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.CheckOneLeader()
	var survivor raft.NodeID
	for _, id := range c.IDs() {
		if id != leader && survivor == "" {
			survivor = id
		} else if id != survivor {
			c.Crash(id)
		}
	}
	// Leader and one follower are down; the survivor can never win.
	termBefore := status(c, survivor).Term
	advance(c, 3*time.Second)
	c.CheckNoLeader()
	if s := status(c, survivor); s.Role != raft.Candidate || s.Term <= termBefore {
		t.Fatalf("survivor should keep campaigning in higher terms, got %+v (was term %d)", s, termBefore)
	}

	// Restore a majority: a leader appears.
	for _, id := range c.IDs() {
		if id != survivor && !c.Alive(id) {
			c.Restart(id)
			break
		}
	}
	c.CheckOneLeader()
}

// TestManyElections: random disconnects for 10 s of cluster time. At no point
// may two different nodes lead the same term.
func TestManyElections(t *testing.T) {
	seed := testSeed(t)
	c := newCluster(t, 5)
	rng := rand.New(rand.NewSource(seed))
	ids := c.IDs()
	leaderOf := map[uint64]raft.NodeID{}

	check := func() {
		for _, id := range ids {
			s := status(c, id)
			if s.Role != raft.Leader {
				continue
			}
			if prev, ok := leaderOf[s.Term]; ok && prev != id {
				t.Fatalf("election safety violated: term %d led by both %s and %s (seed %d)", s.Term, prev, id, seed)
			}
			leaderOf[s.Term] = id
		}
		c.CheckTermLeaders()
	}

	for step := 0; step < 1000; step++ { // 1000 x 10 ms
		if step%25 == 0 {
			id := ids[rng.Intn(len(ids))]
			if rng.Intn(2) == 0 {
				c.Disconnect(id)
			} else {
				c.Connect(id)
			}
		}
		advance(c, 10*time.Millisecond)
		check()
	}

	for _, id := range ids {
		c.Connect(id)
	}
	waitFor(t, c, "a stable leader after healing", func() bool {
		check()
		leaders, terms := 0, map[uint64]bool{}
		for _, id := range ids {
			s := status(c, id)
			terms[s.Term] = true
			if s.Role == raft.Leader {
				leaders++
			}
		}
		return leaders == 1 && len(terms) == 1
	})
	if len(leaderOf) < 2 {
		t.Logf("only %d terms had a leader; the chaos was gentle for seed %d", len(leaderOf), seed)
	}
}
