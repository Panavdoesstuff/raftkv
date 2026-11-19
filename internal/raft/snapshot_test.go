package raft_test

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/storage"
	"github.com/panavdoesstuff/raftkv/internal/transport"
	"github.com/panavdoesstuff/raftkv/test"
)

// snapSM is a state machine that folds every applied command into (count,
// hash) and snapshots its Raft node once the log passes max bytes, the way
// the KV server does. Its state is order-sensitive, so two nodes agree only if
// they applied the same commands in the same order.
type snapSM struct {
	mu       sync.Mutex
	count    uint64
	hash     uint64
	restores int
	rf       *raft.Raft
	max      int
}

func (s *snapSM) state() []byte {
	b := binary.LittleEndian.AppendUint64(nil, s.count)
	return binary.LittleEndian.AppendUint64(b, s.hash)
}

func (s *snapSM) Apply(e raft.Entry) []byte {
	h := fnv.New64a()
	h.Write(e.Data)
	s.mu.Lock()
	s.count++
	s.hash = s.hash*1099511628211 ^ h.Sum64()
	st := s.state()
	s.mu.Unlock()
	if s.rf != nil && s.max > 0 && s.rf.LogSize() > s.max {
		s.rf.Snapshot(e.Index, st)
	}
	return nil
}

func (s *snapSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state(), nil
}

func (s *snapSM) Restore(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count = binary.LittleEndian.Uint64(b)
	s.hash = binary.LittleEndian.Uint64(b[8:])
	s.restores++
	return nil
}

func (s *snapSM) get() (count, hash uint64, restores int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count, s.hash, s.restores
}

type snapCluster struct {
	*test.Cluster
	mu  sync.Mutex
	sms map[raft.NodeID]*snapSM // latest incarnation of each node
}

func newSnapCluster(t *testing.T, n, max int) *snapCluster {
	t.Helper()
	sc := &snapCluster{sms: map[raft.NodeID]*snapSM{}}
	f := func(c test.Config) test.Node {
		sm := c.SM.(*snapSM)
		sm.max = max
		r := raft.New(raft.Config{ID: c.ID, Peers: c.Peers, Transport: c.Transport,
			Clock: c.Clock, Storage: c.Storage, SM: sm, Seed: c.Seed})
		sm.rf = r
		sc.mu.Lock()
		sc.sms[c.ID] = sm
		sc.mu.Unlock()
		return r
	}
	sc.Cluster = test.StartCluster(t, n, testSeed(t), f, test.WithFakeClock(),
		test.WithStateMachine(func() raft.StateMachine { return &snapSM{} }))
	return sc
}

func (sc *snapCluster) sm(id raft.NodeID) *snapSM {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.sms[id]
}

func (sc *snapCluster) raft(id raft.NodeID) *raft.Raft { return sc.Node(id).(*raft.Raft) }

// converged waits until every live node's state machine equals want.
func (sc *snapCluster) waitCount(t *testing.T, want uint64, ids ...raft.NodeID) {
	t.Helper()
	if len(ids) == 0 {
		ids = sc.IDs()
	}
	waitFor(t, sc.Cluster, fmt.Sprintf("count %d on %v", want, ids), func() bool {
		for _, id := range ids {
			if c, _, _ := sc.sm(id).get(); c != want {
				return false
			}
		}
		return true
	})
	_, h0, _ := sc.sm(ids[0]).get()
	for _, id := range ids[1:] {
		if _, h, _ := sc.sm(id).get(); h != h0 {
			t.Fatalf("state machines diverged: %s hash %x vs %s hash %x", ids[0], h0, id, h)
		}
	}
}

func snapCmd(i int) []byte { return []byte(fmt.Sprintf("cmd-%06d-%s", i, strings.Repeat("x", 40))) }

// TestSnapshotBoundedLog: under continuous load every node's log stays near
// the limit, and every node ends in the same state.
func TestSnapshotBoundedLog(t *testing.T) {
	const max = 2048
	sc := newSnapCluster(t, 3, max)
	sc.CheckOneLeader()
	worst := 0
	for i := 0; i < 500; i++ {
		sc.One(snapCmd(i), 3, true)
		for _, id := range sc.IDs() {
			if n := sc.raft(id).LogSize(); n > worst {
				worst = n
			}
		}
	}
	if worst > max+512 {
		t.Fatalf("log reached %d bytes, limit %d (seed=%d)", worst, max, sc.Seed)
	}
	for _, id := range sc.IDs() {
		if status(sc.Cluster, id).SnapIndex == 0 {
			t.Fatalf("%s never snapshotted", id)
		}
	}
	sc.waitCount(t, 500)
	sc.CheckLogsAgree()
}

// TestSnapshotInstall: a node offline for 10k entries cannot be caught up from
// the log (the leader has discarded it), so it must receive a snapshot.
func TestSnapshotInstall(t *testing.T) {
	sc := newSnapCluster(t, 3, 4096)
	leader := sc.CheckOneLeader()
	var off raft.NodeID
	for _, id := range sc.IDs() {
		if id != leader {
			off = id
			break
		}
	}
	sc.One(snapCmd(0), 3, true)
	sc.Disconnect(off)

	const n = 10000
	for i := 1; i <= n; i++ {
		if sc.Propose(snapCmd(i)) == 0 {
			waitFor(t, sc.Cluster, "a leader", func() bool { sc.Cluster.Advance(10 * time.Millisecond); return sc.Propose(snapCmd(i)) != 0 })
		}
		if i%100 == 0 {
			advance(sc.Cluster, 20*time.Millisecond)
		}
	}
	others := []raft.NodeID{}
	for _, id := range sc.IDs() {
		if id != off {
			others = append(others, id)
		}
	}
	sc.waitCount(t, n+1, others...)
	offLast := status(sc.Cluster, off).LastIndex
	if s := status(sc.Cluster, others[0]).SnapIndex; s <= offLast {
		t.Fatalf("leader snapshot index %d does not pass the offline node's log (%d): nothing to install", s, offLast)
	}

	sc.Connect(off)
	sc.waitCount(t, n+1) // all three, same hash
	if _, _, r := sc.sm(off).get(); r == 0 {
		t.Fatalf("%s caught up without ever restoring a snapshot", off)
	}
	if got := len(sc.Node(off).Log()); got > 200 {
		t.Fatalf("%s holds %d entries after catching up; the log should be compacted", off, got)
	}
	// Normal replication resumes after the install.
	for i := n + 1; i < n+21; i++ {
		sc.One(snapCmd(i), 3, true)
	}
	sc.waitCount(t, n+21)
	sc.CheckLogsAgree()
}

// TestSnapshotRestart: nodes restart from snapshot + log tail. The state
// machine is restored from the snapshot before the tail is replayed.
func TestSnapshotRestart(t *testing.T) {
	sc := newSnapCluster(t, 3, 2048)
	sc.CheckOneLeader()
	for i := 0; i < 300; i++ {
		sc.One(snapCmd(i), 3, true)
	}
	sc.waitCount(t, 300)

	// One node alone.
	victim := sc.IDs()[1]
	sc.Crash(victim)
	for i := 300; i < 350; i++ {
		sc.One(snapCmd(i), 2, true)
	}
	sc.Restart(victim)
	if sc.raft(victim).Status().SnapIndex == 0 {
		t.Fatalf("%s restarted without its snapshot", victim)
	}
	sc.waitCount(t, 350)
	if _, _, r := sc.sm(victim).get(); r == 0 {
		t.Fatalf("%s state machine was not restored from the snapshot", victim)
	}

	// Everybody.
	for _, id := range sc.IDs() {
		sc.Crash(id)
	}
	for _, id := range sc.IDs() {
		sc.Restart(id)
	}
	sc.CheckOneLeader()
	for i := 350; i < 400; i++ {
		sc.One(snapCmd(i), 3, true)
	}
	sc.waitCount(t, 400)
	sc.CheckLogsAgree()
}

// TestSnapshotChurn: crashes and partitions for 20 s of fake time with
// continuous proposals and snapshots. Every node must end with the same state
// machine, and the state must contain every entry any node applied.
func TestSnapshotChurn(t *testing.T) {
	sc := newSnapCluster(t, 5, 1500)
	sc.Net.SetDelay(0, 20*time.Millisecond)
	sc.Net.SetDropProb(0.05)
	sc.Net.SetDupProb(0.05)
	sc.CheckOneLeader()
	ids := sc.IDs()
	seed := sc.Seed
	rnd := uint64(seed)*6364136223846793005 + 1442695040888963407
	next := func(n int) int {
		rnd = rnd*6364136223846793005 + 1442695040888963407
		return int(rnd>>33) % n
	}
	down := map[raft.NodeID]int{}
	part := 0
	proposed := 0
	for step := 0; step < 2000; step++ { // 20 s of fake time; the 60 s run is the KV-level TestSnapshotChurn
		for id, until := range down {
			if step >= until {
				sc.Restart(id)
				delete(down, id)
			}
		}
		if step%100 == 50 && len(down) < 2 {
			id := ids[next(len(ids))]
			if _, ok := down[id]; !ok {
				sc.Crash(id)
				down[id] = step + 30 + next(200)
			}
		}
		if part == 0 && step%500 == 250 {
			a, b := []raft.NodeID{}, []raft.NodeID{}
			for k, p := range ids {
				if (k+next(2))%2 == 0 || len(a) < 2 {
					a = append(a, p)
				} else {
					b = append(b, p)
				}
			}
			sc.Partition(a, b)
			part = step + 80 + next(150)
		}
		if part != 0 && step >= part {
			sc.Heal()
			part = 0
		}
		for k := 0; k < 3; k++ { // a steady trickle of proposals
			if sc.Propose(snapCmd(proposed)) != 0 {
				proposed++
			}
		}
		advance(sc.Cluster, 10*time.Millisecond)
	}
	sc.Heal()
	sc.Net.SetDelay(0, 0)
	sc.Net.SetDropProb(0)
	sc.Net.SetDupProb(0)
	for id := range down {
		sc.Restart(id)
	}
	sc.CheckOneLeader()
	sc.One([]byte("final"), 5, true)
	// Whatever was committed, all five must agree on it. (Some proposals were
	// lost on deposed leaders, so the count is not `proposed`.)
	waitFor(t, sc.Cluster, "convergence", func() bool {
		c0, h0, _ := sc.sm(ids[0]).get()
		for _, id := range ids[1:] {
			if c, h, _ := sc.sm(id).get(); c != c0 || h != h0 {
				return false
			}
		}
		return c0 > 0
	})
	sc.CheckLogsAgree()
	t.Logf("seed=%d proposed=%d", seed, proposed)
}

// --- InstallSnapshot handler rules, driven directly ---

type lone struct {
	r   *raft.Raft
	st  *storage.MemStorage
	sm  *snapSM
	clk *clock.FakeClock
}

// newLone starts node n0 of {n0,n1,n2} on a fake clock that never advances, so
// it never campaigns; tests play the leader n1 by hand.
func newLone(t *testing.T) *lone {
	t.Helper()
	clk := clock.NewFake()
	st := storage.NewMem()
	sm := &snapSM{}
	r := raft.New(raft.Config{
		ID: "n0", Peers: []raft.NodeID{"n0", "n1", "n2"},
		Transport: transport.NewMemnet(1, clk).Transport("n0"),
		Clock:     clk, Storage: st, SM: sm, Seed: 1,
	})
	sm.rf = r
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Stop)
	return &lone{r: r, st: st, sm: sm, clk: clk}
}

func ents(from, to, term uint64) []raft.Entry {
	var es []raft.Entry
	for i := from; i <= to; i++ {
		es = append(es, raft.Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("e%d", i))})
	}
	return es
}

func (l *lone) appendFrom(t *testing.T, term, prev, prevTerm uint64, es []raft.Entry, commit uint64) {
	t.Helper()
	rep, err := l.r.AppendEntries(nil, &raft.AppendEntriesArgs{Term: term, LeaderID: "n1",
		PrevLogIndex: prev, PrevLogTerm: prevTerm, Entries: es, LeaderCommit: commit})
	if err != nil || !rep.Success {
		t.Fatalf("append: %+v %v", rep, err)
	}
}

func (l *lone) install(t *testing.T, term, idx, idxTerm uint64) *raft.InstallSnapshotReply {
	t.Helper()
	data := binary.LittleEndian.AppendUint64(nil, idx)
	data = binary.LittleEndian.AppendUint64(data, idx*7)
	rep, err := l.r.InstallSnapshot(nil, &raft.InstallSnapshotArgs{Term: term, LeaderID: "n1",
		LastIncludedIndex: idx, LastIncludedTerm: idxTerm, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	// Real-time poll with a generous cap: it returns as soon as cond holds, and
	// a CPU-starved machine must not turn slow progress into a failure.
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A snapshot that matches an entry we hold keeps the entries after it.
func TestInstallSnapshotKeepsMatchingSuffix(t *testing.T) {
	l := newLone(t)
	l.appendFrom(t, 1, 0, 0, ents(1, 10, 1), 3)
	l.install(t, 1, 6, 1)
	log := l.r.Log()
	if len(log) != 4 || log[0].Index != 7 || log[3].Index != 10 {
		t.Fatalf("log after install = %+v, want entries 7..10", log)
	}
	s := l.r.Status()
	if s.SnapIndex != 6 || s.LastIndex != 10 || s.CommitIndex < 6 {
		t.Fatalf("status %+v", s)
	}
	eventually(t, "snapshot applied", func() bool { _, _, r := l.sm.get(); return r == 1 && l.r.Status().LastApplied >= 6 })
	_, disk, snap, _ := l.st.Load()
	if snap == nil || snap.LastIncludedIndex != 6 || len(disk) != 4 || disk[0].Index != 7 {
		t.Fatalf("disk: snap=%+v log=%+v", snap, disk)
	}
}

// A snapshot that conflicts with (or lies beyond) our log discards the log.
func TestInstallSnapshotDiscardsConflictingLog(t *testing.T) {
	for name, tc := range map[string]struct{ idx, term uint64 }{
		"term mismatch": {idx: 8, term: 2},
		"beyond log":    {idx: 15, term: 1},
	} {
		t.Run(name, func(t *testing.T) {
			l := newLone(t)
			l.appendFrom(t, 1, 0, 0, ents(1, 10, 1), 3)
			l.install(t, 2, tc.idx, tc.term)
			if log := l.r.Log(); len(log) != 0 {
				t.Fatalf("log not discarded: %+v", log)
			}
			if s := l.r.Status(); s.LastIndex != tc.idx || s.SnapIndex != tc.idx {
				t.Fatalf("status %+v", s)
			}
			_, disk, snap, _ := l.st.Load()
			if snap == nil || snap.LastIncludedTerm != tc.term || len(disk) != 0 {
				t.Fatalf("disk: snap=%+v log=%+v", snap, disk)
			}
			// The follower can continue from the snapshot boundary.
			l.appendFrom(t, 2, tc.idx, tc.term, ents(tc.idx+1, tc.idx+2, 2), 0)
			if log := l.r.Log(); len(log) != 2 || log[0].Index != tc.idx+1 {
				t.Fatalf("log after append = %+v", log)
			}
		})
	}
}

// Stale-term snapshots are rejected; snapshots at or below our commit index
// are ignored without touching the state machine.
func TestInstallSnapshotStale(t *testing.T) {
	l := newLone(t)
	l.appendFrom(t, 3, 0, 0, ents(1, 10, 3), 8)
	eventually(t, "apply 8", func() bool { return l.r.Status().LastApplied == 8 })

	if rep := l.install(t, 2, 20, 2); rep.Term != 3 {
		t.Fatalf("stale term reply %d, want 3", rep.Term)
	}
	if s := l.r.Status(); s.SnapIndex != 0 || s.LastIndex != 10 {
		t.Fatalf("stale-term snapshot changed state: %+v", s)
	}
	l.install(t, 3, 8, 3) // == commitIndex
	l.install(t, 3, 5, 3) // < commitIndex
	if s := l.r.Status(); s.SnapIndex != 0 {
		t.Fatalf("old snapshot installed: %+v", s)
	}
	if _, _, r := l.sm.get(); r != 0 {
		t.Fatalf("state machine restored from an old snapshot")
	}
}

// Across an install the apply stream never repeats or reorders an index:
// entries, then a snapshot message that jumps forward, then entries.
func TestInstallSnapshotApplyOrder(t *testing.T) {
	l := newLone(t)
	l.appendFrom(t, 1, 0, 0, ents(1, 10, 1), 3)
	l.install(t, 1, 6, 1)
	l.appendFrom(t, 1, 10, 1, ents(11, 12, 1), 12)
	var last uint64
	sawSnap := false
	timeout := time.After(5 * time.Second)
	for last < 12 {
		select {
		case m := <-l.r.ApplyCh():
			if m.Snapshot != nil {
				if m.Snapshot.LastIncludedIndex <= last {
					t.Fatalf("snapshot %d delivered after index %d", m.Snapshot.LastIncludedIndex, last)
				}
				last, sawSnap = m.Snapshot.LastIncludedIndex, true
				continue
			}
			if m.Entry.Index != last+1 {
				t.Fatalf("applied %d after %d", m.Entry.Index, last)
			}
			last = m.Entry.Index
		case <-timeout:
			t.Fatalf("stuck at %d", last)
		}
	}
	if !sawSnap {
		t.Fatal("no snapshot message delivered")
	}
}

// Raft.Snapshot: discards covered entries, keeps the term of the boundary,
// persists, and ignores indexes that are uncommitted or already covered.
func TestRaftSnapshotAPI(t *testing.T) {
	l := newLone(t)
	l.appendFrom(t, 1, 0, 0, ents(1, 6, 1), 4)
	l.appendFrom(t, 2, 6, 1, ents(7, 10, 2), 10)
	eventually(t, "apply 10", func() bool { return l.r.Status().LastApplied == 10 })
	before := l.r.LogSize()

	l.r.Snapshot(12, []byte("ahead")) // beyond commit: ignored
	if l.r.Status().SnapIndex != 0 {
		t.Fatal("snapshot past commitIndex accepted")
	}
	l.r.Snapshot(7, []byte("seven"))
	s := l.r.Status()
	if s.SnapIndex != 7 || s.LastIndex != 10 {
		t.Fatalf("status %+v", s)
	}
	if log := l.r.Log(); len(log) != 3 || log[0].Index != 8 {
		t.Fatalf("log %+v", log)
	}
	if l.r.LogSize() >= before {
		t.Fatalf("LogSize did not shrink: %d -> %d", before, l.r.LogSize())
	}
	l.r.Snapshot(5, []byte("old")) // already covered: ignored
	if l.r.Status().SnapIndex != 7 {
		t.Fatal("older snapshot replaced a newer one")
	}
	_, disk, snap, _ := l.st.Load()
	if snap == nil || snap.LastIncludedIndex != 7 || snap.LastIncludedTerm != 2 || string(snap.Data) != "seven" || len(disk) != 3 {
		t.Fatalf("disk: snap=%+v log=%+v", snap, disk)
	}
	// Entries after the boundary keep working: append, then compact again.
	l.appendFrom(t, 2, 10, 2, ents(11, 11, 2), 11)
	eventually(t, "apply 11", func() bool { return l.r.Status().LastApplied == 11 })
	l.r.Snapshot(11, []byte("eleven"))
	if l.r.Status().SnapIndex != 11 || len(l.r.Log()) != 0 {
		t.Fatalf("status %+v log %+v", l.r.Status(), l.r.Log())
	}
}
