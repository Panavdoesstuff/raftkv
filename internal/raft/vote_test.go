package raft_test

import (
	"context"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/storage"
	"github.com/panavdoesstuff/raftkv/internal/transport"
)

// solo builds one started node (n0 of n0,n1,n2) whose peers are unreachable,
// with the given durable state, on a FakeClock that only moves when told to.
type soloNode struct {
	t   *testing.T
	n   *raft.Raft
	clk *clock.FakeClock
}

func newSolo(t *testing.T, term uint64, log []raft.Entry) *soloNode {
	t.Helper()
	st := storage.NewMem()
	if term > 0 {
		st.SaveHardState(term, "")
	}
	if len(log) > 0 {
		st.Append(log)
	}
	clk := clock.NewFake()
	net := transport.NewMemnet(1, clk)
	n := raft.New(raft.Config{
		ID: "n0", Peers: []raft.NodeID{"n0", "n1", "n2"},
		Transport: net.Transport("n0"), Clock: clk, Storage: st, Seed: testSeed(t),
	})
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return &soloNode{t: t, n: n, clk: clk}
}

func (s *soloNode) advance(d time.Duration) {
	for ; d > 0; d -= 10 * time.Millisecond {
		s.clk.Advance(10 * time.Millisecond)
		time.Sleep(settleTime)
	}
}

func (s *soloNode) vote(term uint64, cand raft.NodeID, lastIdx, lastTerm uint64) *raft.RequestVoteReply {
	s.t.Helper()
	r, err := s.n.RequestVote(context.Background(), &raft.RequestVoteArgs{
		Term: term, CandidateID: cand, LastLogIndex: lastIdx, LastLogTerm: lastTerm,
	})
	if err != nil {
		s.t.Fatal(err)
	}
	return r
}

func (s *soloNode) heartbeat(term uint64) *raft.AppendEntriesReply {
	s.t.Helper()
	r, err := s.n.AppendEntries(context.Background(), &raft.AppendEntriesArgs{Term: term, LeaderID: "n1"})
	if err != nil {
		s.t.Fatal(err)
	}
	return r
}

// log: (1,t1) (2,t1) (3,t2); last entry is index 3, term 2.
var testLog = []raft.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 2}}

func TestRequestVoteLogUpToDate(t *testing.T) {
	cases := []struct {
		name          string
		lastIdx, term uint64
		grant         bool
	}{
		{"older last term but longer log", 10, 1, false},
		{"same last term, shorter log", 2, 2, false},
		{"same last term, same length", 3, 2, true},
		{"same last term, longer log", 4, 2, true},
		{"newer last term but shorter log", 1, 3, true},
		{"empty log", 0, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSolo(t, 2, testLog)
			r := s.vote(3, "n1", tc.lastIdx, tc.term)
			if r.VoteGranted != tc.grant {
				t.Fatalf("VoteGranted = %v, want %v", r.VoteGranted, tc.grant)
			}
			// A higher term is adopted even when the vote is refused.
			if got := s.n.Status().Term; got != 3 || r.Term != 3 {
				t.Fatalf("node term %d, reply term %d; want both 3", got, r.Term)
			}
		})
	}
}

func TestRequestVoteStaleTerm(t *testing.T) {
	s := newSolo(t, 5, testLog)
	r := s.vote(4, "n1", 3, 2)
	if r.VoteGranted || r.Term != 5 {
		t.Fatalf("stale-term vote: %+v", r)
	}
	if s.n.Status().Term != 5 {
		t.Fatal("stale request changed our term")
	}
}

func TestVoteOncePerTerm(t *testing.T) {
	s := newSolo(t, 2, testLog)
	if !s.vote(3, "n1", 3, 2).VoteGranted {
		t.Fatal("first vote should be granted")
	}
	if s.vote(3, "n2", 3, 2).VoteGranted {
		t.Fatal("second candidate in the same term must be refused")
	}
	if !s.vote(3, "n1", 3, 2).VoteGranted {
		t.Fatal("a repeated request from the same candidate should be granted again")
	}
	if !s.vote(4, "n2", 3, 2).VoteGranted {
		t.Fatal("a new term allows a new vote")
	}
}

func TestStepDownOnHigherTerm(t *testing.T) {
	s := newSolo(t, 5, testLog)
	s.advance(400 * time.Millisecond) // becomes a candidate; peers are unreachable
	if st := s.n.Status(); st.Role != raft.Candidate || st.Term <= 5 {
		t.Fatalf("expected a candidate in term > 5, got %+v", st)
	}
	r := s.heartbeat(s.n.Status().Term + 3)
	if !r.Success {
		t.Fatal("heartbeat from a higher-term leader should succeed")
	}
	if st := s.n.Status(); st.Role != raft.Follower {
		t.Fatalf("candidate did not step down: %+v", st)
	}
}

// The election timer is reset only by: granting a vote, AppendEntries from the
// current leader, or starting an election (Figure 2, Rules for Followers).
func TestElectionTimerResetRules(t *testing.T) {
	t.Run("current-leader heartbeats keep a follower quiet", func(t *testing.T) {
		s := newSolo(t, 5, testLog)
		for i := 0; i < 40; i++ { // 2 s of heartbeats every 50 ms
			s.advance(50 * time.Millisecond)
			s.heartbeat(5)
		}
		if st := s.n.Status(); st.Role != raft.Follower || st.Term != 5 {
			t.Fatalf("follower disturbed: %+v", st)
		}
	})
	t.Run("stale-term heartbeats do not reset", func(t *testing.T) {
		s := newSolo(t, 5, testLog)
		for i := 0; i < 20; i++ {
			s.advance(50 * time.Millisecond)
			if r := s.heartbeat(3); r.Success {
				t.Fatal("stale heartbeat accepted")
			}
		}
		if st := s.n.Status(); st.Term <= 5 {
			t.Fatalf("stale heartbeats kept the election timer alive: %+v", st)
		}
	})
	t.Run("granting a vote resets", func(t *testing.T) {
		s := newSolo(t, 5, testLog)
		for term := uint64(6); term < 16; term++ { // 1.4 s, always 140 ms apart
			s.advance(140 * time.Millisecond)
			if !s.vote(term, "n1", 3, 2).VoteGranted {
				t.Fatalf("vote in term %d refused", term)
			}
		}
		if st := s.n.Status(); st.Role != raft.Follower || st.Term != 15 {
			t.Fatalf("granted votes should have kept the node a follower: %+v", st)
		}
	})
	t.Run("refusing a vote does not reset", func(t *testing.T) {
		s := newSolo(t, 5, testLog)
		sawCandidate := false
		for term := uint64(6); term < 16; term++ {
			s.advance(140 * time.Millisecond)
			if s.n.Status().Role == raft.Candidate {
				sawCandidate = true
			}
			s.vote(term, "n1", 1, 1) // stale log: refused
		}
		if !sawCandidate {
			t.Fatal("refused votes postponed the election")
		}
	})
}
