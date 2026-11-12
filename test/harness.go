// Package test is the memnet cluster harness: start, crash, restart and
// partition nodes, and check cluster-wide invariants. It doesn't know about the
// Raft implementation; the caller supplies a Factory that builds a node from
// the injected dependencies (Transport, Clock, Storage, StateMachine).
package test

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/storage"
	"github.com/panavdoesstuff/raftkv/internal/transport"
)

// Config is everything a node needs; the harness owns all of it.
type Config struct {
	ID        raft.NodeID
	Peers     []raft.NodeID // all node IDs in the cluster, including this one
	Transport raft.Transport
	Clock     clock.Clock
	Storage   raft.Storage
	SM        raft.StateMachine
	Seed      int64 // per-node seed, for election timeout jitter
}

// Node is what the harness needs from a Raft node.
type Node interface {
	raft.Peer
	Start() error
	Stop()
	Status() raft.Status
	// Propose appends cmd to the log if this node is the leader.
	Propose(cmd []byte) (index, term uint64, isLeader bool)
	// ApplyCh delivers committed entries in order, once. It is closed when the
	// node stops.
	ApplyCh() <-chan raft.ApplyMsg
	// Log returns the entries currently held in the log (after any snapshot).
	Log() []raft.Entry
}

type Factory func(Config) Node

type nopSM struct{}

func (nopSM) Apply(raft.Entry) []byte   { return nil }
func (nopSM) Snapshot() ([]byte, error) { return nil, nil }
func (nopSM) Restore([]byte) error      { return nil }

type member struct {
	id      raft.NodeID
	node    Node
	storage *storage.MemStorage
	alive   bool
	applied map[uint64]raft.Entry // entries this incarnation applied (guarded by Cluster.amu)
	drained chan struct{}         // closed when the ApplyCh drain goroutine exits
}

type Cluster struct {
	t       testing.TB
	Seed    int64
	Net     *transport.Memnet
	Clock   clock.Clock
	fake    *clock.FakeClock // non-nil when running on a FakeClock
	factory Factory
	ids     []raft.NodeID
	members map[raft.NodeID]*member
	newSM   func() raft.StateMachine

	amu       sync.Mutex
	committed map[uint64]raft.Entry // first entry any node applied at each index
}

type Option func(*Cluster)

// WithFakeClock runs the whole cluster on one FakeClock. The harness advances
// it itself while waiting in CheckOneLeader and friends.
func WithFakeClock() Option {
	return func(c *Cluster) { f := clock.NewFake(); c.fake, c.Clock = f, f }
}

// WithStateMachine sets the per-node state machine constructor.
func WithStateMachine(f func() raft.StateMachine) Option {
	return func(c *Cluster) { c.newSM = f }
}

// StartCluster creates n nodes named n0..n{n-1} and starts them. The seed is
// logged if the test fails; rerun with the same seed to reproduce.
func StartCluster(t testing.TB, n int, seed int64, f Factory, opts ...Option) *Cluster {
	t.Helper()
	c := &Cluster{
		t: t, Seed: seed, factory: f,
		Clock:     clock.RealClock{},
		members:   map[raft.NodeID]*member{},
		committed: map[uint64]raft.Entry{},
		newSM:     func() raft.StateMachine { return nopSM{} },
	}
	for _, o := range opts {
		o(c)
	}
	c.Net = transport.NewMemnet(seed, c.Clock)
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, raft.NodeID(fmt.Sprintf("n%d", i)))
	}
	for _, id := range c.ids {
		c.members[id] = &member{id: id, storage: storage.NewMem()}
		c.boot(id)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("reproduce with seed=%d", seed)
		}
		for _, m := range c.members {
			if m.alive {
				m.node.Stop()
				<-m.drained
			}
		}
	})
	return c
}

func (c *Cluster) boot(id raft.NodeID) {
	m := c.members[id]
	m.node = c.factory(Config{
		ID: id, Peers: c.ids,
		Transport: c.Net.Transport(id),
		Clock:     c.Clock,
		Storage:   m.storage, // the "disk" survives crashes
		SM:        c.newSM(), // memory does not
		Seed:      c.Seed*1000 + int64(len(id)) + int64(id[len(id)-1]),
	})
	if err := c.Net.Transport(id).Serve(id, m.node); err != nil {
		c.t.Fatalf("serve %s: %v", id, err)
	}
	c.Net.SetNodeConnected(id, true)
	if err := m.node.Start(); err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	m.alive = true
	c.amu.Lock()
	m.applied = map[uint64]raft.Entry{}
	c.amu.Unlock()
	m.drained = make(chan struct{})
	go c.drain(m, m.node.ApplyCh(), m.drained)
}

// drain consumes a node's ApplyCh, records what it applied and checks State
// Machine Safety (Figure 3): every node applies the same entry at an index,
// and each incarnation applies indexes in order, once. It runs off the test
// goroutine, so it uses Errorf, never Fatalf.
func (c *Cluster) drain(m *member, ch <-chan raft.ApplyMsg, done chan<- struct{}) {
	defer close(done)
	var last uint64
	for msg := range ch {
		e := msg.Entry
		c.amu.Lock()
		if e.Index != last+1 {
			c.t.Errorf("%s applied index %d after %d (must be in order, exactly once)", m.id, e.Index, last)
		}
		last = e.Index
		if prev, ok := c.committed[e.Index]; ok {
			if prev.Term != e.Term || prev.Type != e.Type || !bytes.Equal(prev.Data, e.Data) {
				c.t.Errorf("state machine safety violated at index %d: %s applied %+v, another node applied %+v",
					e.Index, m.id, e, prev)
			}
		} else {
			c.committed[e.Index] = e.Clone()
		}
		m.applied[e.Index] = e.Clone()
		c.amu.Unlock()
	}
}

func (c *Cluster) IDs() []raft.NodeID { return append([]raft.NodeID(nil), c.ids...) }

// Node returns the current incarnation of a node (it changes on Restart).
func (c *Cluster) Node(id raft.NodeID) Node { return c.members[id].node }

func (c *Cluster) Alive(id raft.NodeID) bool { return c.members[id].alive }

// Storage exposes a node's "disk" so tests can inspect or corrupt it.
func (c *Cluster) Storage(id raft.NodeID) *storage.MemStorage { return c.members[id].storage }

// Crash stops the node and drops its memory. Its storage is kept.
func (c *Cluster) Crash(id raft.NodeID) {
	m := c.members[id]
	if !m.alive {
		return
	}
	c.Net.SetNodeConnected(id, false)
	c.Net.Unserve(id)
	m.node.Stop()
	<-m.drained
	m.alive = false
}

// Restart boots a fresh node on the old storage. It starts as a follower.
func (c *Cluster) Restart(id raft.NodeID) {
	if c.members[id].alive {
		c.Crash(id)
	}
	c.boot(id)
}

// Disconnect cuts a live node off the network without stopping it.
func (c *Cluster) Disconnect(id raft.NodeID) { c.Net.SetNodeConnected(id, false) }
func (c *Cluster) Connect(id raft.NodeID)    { c.Net.SetNodeConnected(id, true) }

// Partition splits the network into the given groups; nodes in different
// groups can't talk. Nodes not listed in any group keep full connectivity.
func (c *Cluster) Partition(groups ...[]raft.NodeID) { c.Net.Partition(groups...) }

// Heal removes all partitions and reconnects live nodes. Crashed nodes stay
// down (they have no handler until Restart).
func (c *Cluster) Heal() { c.Net.Heal() }

// sleep waits d of cluster time: advances the FakeClock, or really sleeps.
func (c *Cluster) sleep(d time.Duration) {
	if c.fake != nil {
		c.fake.Advance(d)
		time.Sleep(time.Millisecond) // let woken goroutines run
		runtime.Gosched()
		return
	}
	time.Sleep(d)
}

const (
	pollStep    = 10 * time.Millisecond
	pollTimeout = 5 * time.Second // cluster time
)

// leaders returns live leaders grouped by term.
func (c *Cluster) leaders() map[uint64][]raft.NodeID {
	out := map[uint64][]raft.NodeID{}
	for _, id := range c.ids {
		m := c.members[id]
		if !m.alive {
			continue
		}
		if s := m.node.Status(); s.Role == raft.Leader {
			out[s.Term] = append(out[s.Term], id)
		}
	}
	return out
}

// CheckTermLeaders fails if any term has more than one leader (Election
// Safety, Figure 3). Call it at any time; it's cheap.
func (c *Cluster) CheckTermLeaders() {
	c.t.Helper()
	for term, ls := range c.leaders() {
		if len(ls) > 1 {
			c.t.Fatalf("election safety violated: term %d has leaders %v", term, ls)
		}
	}
}

// CheckOneLeader waits (up to 5 s of cluster time) for a leader and returns
// the one with the highest term. A stale leader in an old term on the wrong
// side of a partition is allowed; two leaders in one term never are.
func (c *Cluster) CheckOneLeader() raft.NodeID {
	c.t.Helper()
	for waited := time.Duration(0); waited < pollTimeout; waited += pollStep {
		c.CheckTermLeaders()
		ls := c.leaders()
		if len(ls) > 0 {
			var terms []uint64
			for term := range ls {
				terms = append(terms, term)
			}
			sort.Slice(terms, func(i, j int) bool { return terms[i] > terms[j] })
			return ls[terms[0]][0]
		}
		c.sleep(pollStep)
	}
	c.t.Fatalf("no leader elected within %v", pollTimeout)
	return ""
}

// CheckNoLeader fails if any live node is currently a leader.
func (c *Cluster) CheckNoLeader() {
	c.t.Helper()
	for term, ls := range c.leaders() {
		c.t.Fatalf("unexpected leader(s) %v in term %d", ls, term)
	}
}

// CheckLogsAgree verifies Log Matching on committed state: for every pair of
// live nodes, all entries up to min(commitIndex) that both still hold are
// identical (same term and data).
func (c *Cluster) CheckLogsAgree() {
	c.t.Helper()
	type view struct {
		id     raft.NodeID
		commit uint64
		byIdx  map[uint64]raft.Entry
	}
	var views []view
	for _, id := range c.ids {
		m := c.members[id]
		if !m.alive {
			continue
		}
		v := view{id: id, commit: m.node.Status().CommitIndex, byIdx: map[uint64]raft.Entry{}}
		for _, e := range m.node.Log() {
			v.byIdx[e.Index] = e
		}
		views = append(views, v)
	}
	for i := range views {
		for j := i + 1; j < len(views); j++ {
			a, b := views[i], views[j]
			upto := a.commit
			if b.commit < upto {
				upto = b.commit
			}
			for idx := uint64(1); idx <= upto; idx++ {
				ea, oka := a.byIdx[idx]
				eb, okb := b.byIdx[idx]
				if !oka || !okb {
					continue // compacted into a snapshot on one side
				}
				if ea.Term != eb.Term || string(ea.Data) != string(eb.Data) || ea.Type != eb.Type {
					c.t.Fatalf("logs disagree at committed index %d: %s has %+v, %s has %+v",
						idx, a.id, ea, b.id, eb)
				}
			}
		}
	}
}

// Applied returns what node id's current incarnation applied at index.
func (c *Cluster) Applied(id raft.NodeID, index uint64) (raft.Entry, bool) {
	c.amu.Lock()
	defer c.amu.Unlock()
	e, ok := c.members[id].applied[index]
	return e, ok
}

// NCommitted counts live nodes that applied index, and returns the command
// they applied. It fails the test if two nodes applied different commands.
func (c *Cluster) NCommitted(index uint64) (int, []byte) {
	c.t.Helper()
	n := 0
	var cmd []byte
	for _, id := range c.ids {
		if !c.members[id].alive {
			continue
		}
		e, ok := c.Applied(id, index)
		if !ok {
			continue
		}
		if n > 0 && !bytes.Equal(cmd, e.Data) {
			c.t.Fatalf("nodes applied different commands at index %d: %q vs %q", index, cmd, e.Data)
		}
		n, cmd = n+1, e.Data
	}
	return n, cmd
}

// Propose offers cmd to the live leader with the highest term, then to every
// other live node that thinks it leads. It returns the index it was given, or
// 0 if nobody accepted. Acceptance is not commitment.
func (c *Cluster) Propose(cmd []byte) uint64 {
	var terms []uint64
	byTerm := c.leaders()
	for term := range byTerm {
		terms = append(terms, term)
	}
	sort.Slice(terms, func(i, j int) bool { return terms[i] > terms[j] })
	for _, term := range terms {
		for _, id := range byTerm[term] {
			if idx, _, ok := c.members[id].node.Propose(cmd); ok {
				return idx
			}
		}
	}
	return 0
}

// One submits cmd and waits (cluster time) until at least expected live nodes
// applied it at the same index, which it returns. With retry it resubmits
// after a leader change or a lost proposal, for up to 10 s.
func (c *Cluster) One(cmd []byte, expected int, retry bool) uint64 {
	c.t.Helper()
	const total = 10 * time.Second
	for waited := time.Duration(0); waited < total; {
		idx := c.Propose(cmd)
		if idx == 0 {
			c.sleep(50 * time.Millisecond)
			waited += 50 * time.Millisecond
			continue
		}
		for w := time.Duration(0); w < 2*time.Second; w += pollStep {
			if n, got := c.NCommitted(idx); n >= expected && bytes.Equal(got, cmd) {
				return idx
			}
			c.sleep(pollStep)
			waited += pollStep
		}
		if !retry {
			c.t.Fatalf("one(%q): index %d did not commit on %d nodes", cmd, idx, expected)
		}
	}
	c.t.Fatalf("one(%q): no agreement on %d nodes within %v", cmd, expected, total)
	return 0
}

// Advance moves cluster time forward by d (FakeClock) or sleeps (real clock).
func (c *Cluster) Advance(d time.Duration) {
	for ; d > 0; d -= pollStep {
		c.sleep(pollStep)
	}
}
