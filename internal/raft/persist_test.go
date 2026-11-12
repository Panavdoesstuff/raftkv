package raft_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/storage"
	"github.com/panavdoesstuff/raftkv/internal/transport"
	"github.com/panavdoesstuff/raftkv/test"
)

// TestPersistBasic: restart every node; committed entries survive and nobody's
// term goes backwards.
func TestPersistBasic(t *testing.T) {
	c := newCluster(t, 3)
	c.CheckOneLeader()
	var idxs []uint64
	for i := 0; i < 10; i++ {
		idxs = append(idxs, c.One(cmd("p%d", i), 3, false))
	}
	terms := map[raft.NodeID]uint64{}
	for _, id := range c.IDs() {
		terms[id] = status(c, id).Term
	}

	for _, id := range c.IDs() {
		c.Crash(id)
	}
	for _, id := range c.IDs() {
		c.Restart(id)
		if got := status(c, id).Term; got < terms[id] {
			t.Fatalf("%s restarted in term %d, was in term %d: term not persisted", id, got, terms[id])
		}
	}

	c.CheckOneLeader()
	last := c.One(cmd("after"), 3, true)
	for _, id := range c.IDs() {
		for i, idx := range idxs {
			if e, ok := c.Applied(id, idx); !ok || string(e.Data) != string(cmd("p%d", i)) {
				t.Fatalf("%s lost committed entry p%d at index %d (has %q, applied=%v)", id, i, idx, e.Data, ok)
			}
		}
	}
	if last <= idxs[len(idxs)-1] {
		t.Fatalf("new entry got index %d, not after %d", last, idxs[len(idxs)-1])
	}
	c.CheckLogsAgree()
}

// TestPersistChurn: random crash/restart during replication for 30 s of
// cluster time. Logs agree at the end and nothing observed committed is lost.
func TestPersistChurn(t *testing.T) {
	seed := testSeed(t)
	c := newCluster(t, 5)
	rng := rand.New(rand.NewSource(seed))
	committed := runChurn(t, c, rng, 3000, churnOpts{maxDown: 2})
	settleAndVerify(t, c, committed, seed)
}

// TestFigure8Unreliable: the leader is crashed again and again, sometimes right
// after accepting a proposal, on a network that drops, delays, reorders and
// duplicates. This is the Figure 8 setting. Afterwards all logs agree and no
// committed entry is lost.
func TestFigure8Unreliable(t *testing.T) {
	seed := testSeed(t)
	c := newCluster(t, 5)
	c.Net.SetDelay(0, 80*time.Millisecond)
	c.Net.SetDropProb(0.15)
	c.Net.SetDupProb(0.10)
	rng := rand.New(rand.NewSource(seed))
	committed := runChurn(t, c, rng, 3000, churnOpts{maxDown: 2, leaderOnly: true, crashAfterPropose: true})

	c.Net.SetDelay(0, 0)
	c.Net.SetDropProb(0)
	c.Net.SetDupProb(0)
	settleAndVerify(t, c, committed, seed)
}

type proposal struct {
	idx uint64
	cmd []byte
}

type churnOpts struct {
	maxDown           int  // at most this many nodes down at once
	leaderOnly        bool // crash the leader rather than a random node
	crashAfterPropose bool // sometimes crash the leader right after it accepts a proposal
}

func liveLeader(c *test.Cluster) (raft.NodeID, bool) {
	var best raft.NodeID
	var bestTerm uint64
	for _, id := range c.IDs() {
		if !c.Alive(id) {
			continue
		}
		if s := status(c, id); s.Role == raft.Leader && s.Term >= bestTerm {
			best, bestTerm = id, s.Term
		}
	}
	return best, best != ""
}

// runChurn drives steps x 10 ms of cluster time with random crashes, restarts
// and proposals, and returns every proposal seen committed (applied on any
// node, which implies committed).
func runChurn(t *testing.T, c *test.Cluster, rng *rand.Rand, steps int, o churnOpts) []proposal {
	t.Helper()
	ids := c.IDs()
	downUntil := map[raft.NodeID]int{}
	var pending, committed []proposal

	down := func() int {
		n := 0
		for _, id := range ids {
			if !c.Alive(id) {
				n++
			}
		}
		return n
	}
	crash := func(id raft.NodeID, step int) {
		c.Crash(id)
		downUntil[id] = step + 5 + rng.Intn(60) // 50-650 ms
	}

	for step := 0; step < steps; step++ {
		for _, id := range ids {
			if !c.Alive(id) && step >= downUntil[id] {
				c.Restart(id)
			}
		}
		if step%4 == 0 {
			b := cmd("c%d", step)
			if idx := c.Propose(b); idx != 0 {
				pending = append(pending, proposal{idx, b})
				if len(pending) > 40 {
					pending = pending[1:]
				}
				if o.crashAfterPropose && down() < o.maxDown && rng.Intn(8) == 0 {
					if l, ok := liveLeader(c); ok {
						crash(l, step)
					}
				}
			}
		}
		if step%20 == 0 && down() < o.maxDown && rng.Intn(3) == 0 {
			if o.leaderOnly {
				if l, ok := liveLeader(c); ok {
					crash(l, step)
				}
			} else if id := ids[rng.Intn(len(ids))]; c.Alive(id) {
				crash(id, step)
			}
		}
		advance(c, 10*time.Millisecond)

		live := pending[:0]
		for _, p := range pending {
			n, got := c.NCommitted(p.idx)
			switch {
			case n == 0:
				live = append(live, p)
			case bytes.Equal(got, p.cmd):
				committed = append(committed, p)
			} // else: overwritten by another leader; it was never committed
		}
		pending = live
		c.CheckTermLeaders()
	}
	if len(committed) == 0 {
		t.Fatalf("no proposal committed in %d steps; the test proved nothing", steps)
	}
	return committed
}

func settleAndVerify(t *testing.T, c *test.Cluster, committed []proposal, seed int64) {
	t.Helper()
	for _, id := range c.IDs() {
		if !c.Alive(id) {
			c.Restart(id)
		}
	}
	c.Heal()
	c.CheckOneLeader()
	c.One(cmd("final"), len(c.IDs()), true) // every node applies everything before it, too
	c.CheckLogsAgree()
	for _, p := range committed {
		for _, id := range c.IDs() {
			e, ok := c.Applied(id, p.idx)
			if !ok || !bytes.Equal(e.Data, p.cmd) {
				t.Fatalf("seed %d: committed %q at index %d lost on %s (has %q, applied=%v)", seed, p.cmd, p.idx, id, e.Data, ok)
			}
		}
	}
	t.Logf("seed %d: %d entries observed committed, none lost", seed, len(committed))
}

// TestNoDoubleVote: crash right after voting. The restarted node must not give
// the same term's vote to someone else.
func TestNoDoubleVote(t *testing.T) {
	st := storage.NewMem()
	s := startSolo(t, st)
	if r := s.vote(5, "n1", 0, 0); !r.VoteGranted {
		t.Fatalf("first vote refused: %+v", r)
	}
	s.n.Stop() // crash; the "disk" survives

	s = startSolo(t, st)
	if r := s.vote(5, "n2", 0, 0); r.VoteGranted {
		t.Fatal("restarted node voted twice in term 5: two leaders could win the term")
	}
	if r := s.vote(5, "n1", 0, 0); !r.VoteGranted {
		t.Fatal("restarted node must still honour its vote for n1 (retry of the same request)")
	}
	if r := s.vote(6, "n2", 0, 0); !r.VoteGranted {
		t.Fatal("a new term must allow a new vote")
	}
	s.n.Stop()
	s = startSolo(t, st)
	if got := s.n.Status().Term; got != 6 {
		t.Fatalf("restarted in term %d, want 6", got)
	}
}

// TestPersistBeforeReply: by the time any RPC returns, the state it changed is
// already on "disk". Each case is observed from outside, through the storage.
func TestPersistBeforeReply(t *testing.T) {
	ctx := context.Background()
	type disk struct {
		hs  raft.HardState
		log []raft.Entry
	}
	load := func(st *storage.MemStorage) disk {
		hs, log, _, _ := st.Load()
		return disk{hs, log}
	}
	e := func(i, term uint64, d string) raft.Entry { return raft.Entry{Index: i, Term: term, Data: []byte(d)} }

	cases := []struct {
		name    string
		term    uint64
		log     []raft.Entry
		do      func(n *raft.Raft) error
		wantHS  raft.HardState
		wantLog []raft.Entry
	}{
		{"vote granted", 0, nil, func(n *raft.Raft) error {
			_, err := n.RequestVote(ctx, &raft.RequestVoteArgs{Term: 5, CandidateID: "n1"})
			return err
		}, raft.HardState{Term: 5, VotedFor: "n1"}, nil},
		{"vote refused but term raised", 2, testLog, func(n *raft.Raft) error {
			_, err := n.RequestVote(ctx, &raft.RequestVoteArgs{Term: 7, CandidateID: "n1", LastLogIndex: 1, LastLogTerm: 1})
			return err
		}, raft.HardState{Term: 7}, testLog},
		{"heartbeat from newer term", 2, nil, func(n *raft.Raft) error {
			_, err := n.AppendEntries(ctx, &raft.AppendEntriesArgs{Term: 9, LeaderID: "n1"})
			return err
		}, raft.HardState{Term: 9}, nil},
		{"entries appended", 0, nil, func(n *raft.Raft) error {
			_, err := n.AppendEntries(ctx, &raft.AppendEntriesArgs{Term: 2, LeaderID: "n1",
				Entries: []raft.Entry{e(1, 2, "a"), e(2, 2, "b")}})
			return err
		}, raft.HardState{Term: 2}, []raft.Entry{e(1, 2, "a"), e(2, 2, "b")}},
		{"conflicting suffix truncated and replaced", 2, testLog, func(n *raft.Raft) error {
			_, err := n.AppendEntries(ctx, &raft.AppendEntriesArgs{Term: 3, LeaderID: "n1",
				PrevLogIndex: 1, PrevLogTerm: 1, Entries: []raft.Entry{e(2, 3, "x"), e(3, 3, "y")}})
			return err
		}, raft.HardState{Term: 3}, []raft.Entry{{Index: 1, Term: 1}, e(2, 3, "x"), e(3, 3, "y")}},
		{"matching prefix keeps the longer log", 2, testLog, func(n *raft.Raft) error {
			_, err := n.AppendEntries(ctx, &raft.AppendEntriesArgs{Term: 2, LeaderID: "n1",
				PrevLogIndex: 0, Entries: []raft.Entry{{Index: 1, Term: 1}}})
			return err
		}, raft.HardState{Term: 2}, testLog},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := storage.NewMem()
			if tc.term > 0 {
				st.SaveHardState(tc.term, "")
			}
			if len(tc.log) > 0 {
				st.Append(tc.log)
			}
			s := startSolo(t, st)
			if err := tc.do(s.n); err != nil {
				t.Fatal(err)
			}
			got := load(st)
			if got.hs != tc.wantHS {
				t.Fatalf("disk hardstate %+v, want %+v", got.hs, tc.wantHS)
			}
			if len(got.log) != len(tc.wantLog) {
				t.Fatalf("disk log %+v, want %+v", got.log, tc.wantLog)
			}
			for i := range got.log {
				w := tc.wantLog[i]
				if got.log[i].Index != w.Index || got.log[i].Term != w.Term || !bytes.Equal(got.log[i].Data, w.Data) {
					t.Fatalf("disk log %+v, want %+v", got.log, tc.wantLog)
				}
			}
		})
	}
}

// TestPersistElectionAndProposal: a candidate's own vote and a leader's own
// entries are durable before anything is sent.
func TestPersistElectionAndProposal(t *testing.T) {
	st := storage.NewMem()
	// Single-node cluster: it elects itself without peers.
	clk := clock.NewFake()
	n := raft.New(raft.Config{
		ID: "n0", Peers: []raft.NodeID{"n0"}, Clock: clk, Storage: st, Seed: testSeed(t),
		Transport: transport.NewMemnet(1, clk).Transport("n0"),
	})
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	for i := 0; i < 100 && n.Status().Role != raft.Leader; i++ {
		clk.Advance(10 * time.Millisecond)
		time.Sleep(settleTime)
	}
	if n.Status().Role != raft.Leader {
		t.Fatal("single node never became leader")
	}
	hs, log, _, _ := st.Load()
	if hs.Term == 0 || hs.VotedFor != "n0" {
		t.Fatalf("candidate's term and self-vote not durable: %+v", hs)
	}
	if len(log) != 1 || log[0].Term != hs.Term {
		t.Fatalf("leader's no-op not durable: %+v", log)
	}
	if _, _, ok := n.Propose([]byte("x")); !ok {
		t.Fatal("not leader")
	}
	if _, log, _, _ = st.Load(); len(log) != 2 || string(log[1].Data) != "x" {
		t.Fatalf("proposal not durable on return: %+v", log)
	}
}

// failStorage fails writes on demand.
type failStorage struct {
	raft.Storage
	fail atomic.Bool
}

var errDisk = errors.New("disk on fire")

func (f *failStorage) SaveHardState(term uint64, v raft.NodeID) error {
	if f.fail.Load() {
		return errDisk
	}
	return f.Storage.SaveHardState(term, v)
}

func (f *failStorage) Append(es []raft.Entry) error {
	if f.fail.Load() {
		return errDisk
	}
	return f.Storage.Append(es)
}

// If the disk write fails, the node must not reply as if it had succeeded, and
// must stop rather than carry on with state it could not make durable.
func TestStorageFailureHaltsNode(t *testing.T) {
	fs := &failStorage{Storage: storage.NewMem()}
	s := startSolo(t, fs)
	fs.fail.Store(true)
	ctx := context.Background()
	if r, err := s.n.RequestVote(ctx, &raft.RequestVoteArgs{Term: 5, CandidateID: "n1"}); err == nil {
		t.Fatalf("granted a vote that is not durable: %+v", r)
	}
	fs.fail.Store(false) // even once the disk recovers, the node stays down
	if _, err := s.n.RequestVote(ctx, &raft.RequestVoteArgs{Term: 6, CandidateID: "n1"}); err == nil {
		t.Fatal("node kept serving after a storage failure")
	}
}

// TestTornWrite: a node on a real WAL is killed mid-write. The file is cut in
// the middle of the last record; the node must load cleanly, lose only that
// (never acknowledged) record, and accept it again from the leader.
func TestTornWrite(t *testing.T) {
	dir := t.TempDir()
	wal := filepath.Join(dir, storage.WALFile)
	ctx := context.Background()
	app := func(s *soloNode, prev uint64, e raft.Entry) {
		t.Helper()
		r, err := s.n.AppendEntries(ctx, &raft.AppendEntriesArgs{
			Term: 1, LeaderID: "n1", PrevLogIndex: prev, PrevLogTerm: map[bool]uint64{true: 1}[prev > 0],
			Entries: []raft.Entry{e}})
		if err != nil || !r.Success {
			t.Fatalf("append %d: %+v %v", e.Index, r, err)
		}
	}
	size := func() int64 {
		fi, err := os.Stat(wal)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}

	st, err := storage.OpenFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := startSolo(t, st)
	app(s, 0, raft.Entry{Index: 1, Term: 1, Data: []byte("a")})
	app(s, 1, raft.Entry{Index: 2, Term: 1, Data: []byte("b")})
	two := size()
	app(s, 2, raft.Entry{Index: 3, Term: 1, Data: []byte("c")})
	three := size()
	s.n.Stop()
	st.Close()

	if err := os.Truncate(wal, two+(three-two)/2); err != nil { // power cut mid-record
		t.Fatal(err)
	}

	st, err = storage.OpenFile(dir)
	if err != nil {
		t.Fatalf("torn WAL did not load: %v", err)
	}
	defer st.Close()
	s = startSolo(t, st)
	if got := s.n.Status(); got.LastIndex != 2 || got.Term != 1 {
		t.Fatalf("after torn write: %+v, want last index 2 in term 1", got)
	}
	if l := s.n.Log(); len(l) != 2 || string(l[1].Data) != "b" {
		t.Fatalf("log %+v", l)
	}
	app(s, 2, raft.Entry{Index: 3, Term: 1, Data: []byte("c")}) // leader resends it
	s.n.Stop()
	st.Close()

	st, err = storage.OpenFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if l := startSolo(t, st).n.Log(); len(l) != 3 || string(l[2].Data) != "c" {
		t.Fatalf("re-appended entry did not stick: %+v", l)
	}
}
