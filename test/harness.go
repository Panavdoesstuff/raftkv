// Package test is the memnet cluster harness: start, crash, restart and
// partition nodes, and check cluster-wide invariants. It doesn't know about the
// Raft implementation; the caller supplies a Factory that builds a node from
// the injected dependencies (Transport, Clock, Storage, StateMachine).
package test

import (
	"fmt"
	"runtime"
	"sort"
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
		Clock:   clock.RealClock{},
		members: map[raft.NodeID]*member{},
		newSM:   func() raft.StateMachine { return nopSM{} },
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
