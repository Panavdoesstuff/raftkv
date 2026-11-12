package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/raft"
)

// stubNode is a fake "Raft" node whose role/term/log are set by tests. It
// persists its term through Storage so we can verify crash/restart semantics.
type stubNode struct {
	cfg   Config
	mu    sync.Mutex
	st    raft.Status
	log   []raft.Entry
	stop  bool
	votes int
}

func stubFactory(cfg Config) Node {
	n := &stubNode{cfg: cfg}
	n.st.ID = cfg.ID
	return n
}

func (n *stubNode) Start() error {
	hs, log, _, err := n.cfg.Storage.Load()
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.st.Term = hs.Term // restored from disk; everything else is lost memory
	n.log = log
	return nil
}
func (n *stubNode) Stop() { n.mu.Lock(); n.stop = true; n.mu.Unlock() }
func (n *stubNode) Status() raft.Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.st
}
func (n *stubNode) Log() []raft.Entry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return raft.CloneEntries(n.log)
}
func (n *stubNode) set(role raft.Role, term, commit uint64) {
	n.mu.Lock()
	n.st.Role, n.st.Term, n.st.CommitIndex = role, term, commit
	n.mu.Unlock()
	n.cfg.Storage.SaveHardState(term, "")
}
func (n *stubNode) add(e raft.Entry) {
	n.mu.Lock()
	n.log = append(n.log, e)
	n.mu.Unlock()
	n.cfg.Storage.Append([]raft.Entry{e})
}
func (n *stubNode) RequestVote(_ context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votes++
	return &raft.RequestVoteReply{Term: n.st.Term, VoteGranted: true}, nil
}
func (n *stubNode) AppendEntries(context.Context, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return &raft.AppendEntriesReply{Success: true}, nil
}
func (n *stubNode) InstallSnapshot(context.Context, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{}, nil
}

func stub(c *Cluster, id raft.NodeID) *stubNode { return c.Node(id).(*stubNode) }

// failRecorder lets us assert that a check fails without failing the test.
type failRecorder struct {
	testing.TB
	msg string
}

type fatalSentinel struct{}

func (f *failRecorder) Fatalf(format string, args ...any) {
	f.msg = format
	panic(fatalSentinel{})
}
func (f *failRecorder) Helper() {}

func expectFatal(t *testing.T, fn func(tb testing.TB), mk func() *Cluster) {
	t.Helper()
	c := mk()
	rec := &failRecorder{TB: t}
	c.t = rec
	defer func() {
		r := recover()
		if _, ok := r.(fatalSentinel); !ok {
			t.Fatalf("expected check to fail, recover=%v", r)
		}
	}()
	fn(rec)
}

func TestStartClusterNamesAndAlive(t *testing.T) {
	c := StartCluster(t, 3, 1, stubFactory)
	ids := c.IDs()
	if len(ids) != 3 || ids[0] != "n0" || ids[2] != "n2" {
		t.Fatalf("ids %v", ids)
	}
	for _, id := range ids {
		if !c.Alive(id) {
			t.Fatalf("%s not alive", id)
		}
	}
}

func TestCheckOneLeaderFindsHighestTerm(t *testing.T) {
	c := StartCluster(t, 3, 1, stubFactory)
	stub(c, "n0").set(raft.Leader, 2, 0) // stale leader in older term is allowed
	stub(c, "n1").set(raft.Leader, 5, 0)
	if got := c.CheckOneLeader(); got != "n1" {
		t.Fatalf("got %s want n1", got)
	}
}

func TestCheckOneLeaderWaitsForElection(t *testing.T) {
	c := StartCluster(t, 3, 1, stubFactory)
	go func() {
		time.Sleep(50 * time.Millisecond)
		stub(c, "n2").set(raft.Leader, 1, 0)
	}()
	if got := c.CheckOneLeader(); got != "n2" {
		t.Fatalf("got %s", got)
	}
}

func TestCheckOneLeaderRejectsTwoLeadersSameTerm(t *testing.T) {
	var c *Cluster
	expectFatal(t, func(testing.TB) { c.CheckOneLeader() }, func() *Cluster {
		c = StartCluster(t, 3, 1, stubFactory)
		stub(c, "n0").set(raft.Leader, 3, 0)
		stub(c, "n1").set(raft.Leader, 3, 0)
		return c
	})
}

func TestCheckNoLeader(t *testing.T) {
	c := StartCluster(t, 3, 1, stubFactory)
	c.CheckNoLeader()
	stub(c, "n0").set(raft.Leader, 1, 0)
	c.Crash("n0") // crashed leaders don't count
	c.CheckNoLeader()
}

func TestCrashRestartKeepsDiskLosesMemory(t *testing.T) {
	c := StartCluster(t, 3, 1, stubFactory)
	stub(c, "n0").set(raft.Leader, 4, 2)
	c.Crash("n0")
	if c.Alive("n0") {
		t.Fatal("still alive")
	}
	c.Restart("n0")
	s := stub(c, "n0").Status()
	if s.Term != 4 {
		t.Fatalf("term not restored from disk: %d", s.Term)
	}
	if s.Role != raft.Follower || s.CommitIndex != 0 {
		t.Fatalf("volatile state survived crash: %+v", s)
	}
}

func TestCrashedNodeIsUnreachable(t *testing.T) {
	c := StartCluster(t, 3, 1, stubFactory)
	p := c.Net.Transport("n1").Peer("n0")
	if _, err := p.RequestVote(context.Background(), &raft.RequestVoteArgs{}); err != nil {
		t.Fatal(err)
	}
	c.Crash("n0")
	if _, err := p.RequestVote(context.Background(), &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("crashed node answered")
	}
	c.Restart("n0")
	if _, err := p.RequestVote(context.Background(), &raft.RequestVoteArgs{}); err != nil {
		t.Fatalf("restarted node unreachable: %v", err)
	}
}

func TestPartitionAndHeal(t *testing.T) {
	c := StartCluster(t, 5, 1, stubFactory)
	c.Partition([]raft.NodeID{"n0", "n1", "n2"}, []raft.NodeID{"n3", "n4"})
	rv := func(from, to raft.NodeID) error {
		_, err := c.Net.Transport(from).Peer(to).RequestVote(context.Background(), &raft.RequestVoteArgs{})
		return err
	}
	if err := rv("n0", "n1"); err != nil {
		t.Fatalf("same side: %v", err)
	}
	if err := rv("n0", "n3"); err == nil {
		t.Fatal("crossed partition")
	}
	if err := rv("n4", "n2"); err == nil {
		t.Fatal("crossed partition (reverse)")
	}
	c.Heal()
	if err := rv("n0", "n3"); err != nil {
		t.Fatalf("after heal: %v", err)
	}
}

func TestDisconnectConnect(t *testing.T) {
	c := StartCluster(t, 3, 1, stubFactory)
	c.Disconnect("n1")
	_, err := c.Net.Transport("n0").Peer("n1").RequestVote(context.Background(), &raft.RequestVoteArgs{})
	if err == nil {
		t.Fatal("disconnected node reachable")
	}
	c.Connect("n1")
	if _, err = c.Net.Transport("n0").Peer("n1").RequestVote(context.Background(), &raft.RequestVoteArgs{}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckLogsAgree(t *testing.T) {
	c := StartCluster(t, 3, 1, stubFactory)
	for _, id := range c.IDs() {
		for i := uint64(1); i <= 3; i++ {
			stub(c, id).add(raft.Entry{Index: i, Term: 1, Data: []byte{byte(i)}})
		}
		stub(c, id).set(raft.Follower, 1, 3)
	}
	c.CheckLogsAgree() // identical: passes
	// An uncommitted divergent tail is fine.
	stub(c, "n0").add(raft.Entry{Index: 4, Term: 1, Data: []byte("a")})
	stub(c, "n1").add(raft.Entry{Index: 4, Term: 2, Data: []byte("b")})
	c.CheckLogsAgree()
}

func TestCheckLogsAgreeDetectsCommittedDivergence(t *testing.T) {
	var c *Cluster
	expectFatal(t, func(testing.TB) { c.CheckLogsAgree() }, func() *Cluster {
		c = StartCluster(t, 3, 1, stubFactory)
		stub(c, "n0").add(raft.Entry{Index: 1, Term: 1, Data: []byte("a")})
		stub(c, "n1").add(raft.Entry{Index: 1, Term: 2, Data: []byte("b")})
		stub(c, "n0").set(raft.Follower, 2, 1)
		stub(c, "n1").set(raft.Follower, 2, 1)
		return c
	})
}

func TestFakeClockCluster(t *testing.T) {
	c := StartCluster(t, 3, 1, stubFactory, WithFakeClock())
	go func() {
		time.Sleep(20 * time.Millisecond)
		stub(c, "n1").set(raft.Leader, 1, 0)
	}()
	if got := c.CheckOneLeader(); got != "n1" {
		t.Fatalf("got %s", got)
	}
	if c.fake == nil || c.fake.Now().Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("fake clock not advanced")
	}
}
