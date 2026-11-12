// Package transport holds Transport implementations: memnet (in-process),
// and later grpc and faulty.
package transport

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
)

var (
	ErrUnreachable = errors.New("memnet: unreachable")
	ErrDropped     = errors.New("memnet: message dropped")
)

type link struct{ from, to raft.NodeID }

// Memnet is an in-process network for tests. All randomness (drops, delays,
// duplicates) comes from one seeded RNG. Random per-message delay also yields
// reordering. Delays use the supplied Clock, so FakeClock tests control them.
type Memnet struct {
	clk clock.Clock

	mu       sync.Mutex
	rng      *rand.Rand
	handlers map[raft.NodeID]raft.Peer
	down     map[raft.NodeID]bool
	blocked  map[link]bool
	dropProb float64
	dupProb  float64
	delayMin time.Duration
	delayMax time.Duration

	appendSent atomic.Uint64
}

// AppendEntriesSent counts AppendEntries calls attempted so far (dropped or
// not). Tests use it to check that catch-up takes few round trips.
func (n *Memnet) AppendEntriesSent() uint64 { return n.appendSent.Load() }

func NewMemnet(seed int64, clk clock.Clock) *Memnet {
	return &Memnet{
		clk:      clk,
		rng:      rand.New(rand.NewSource(seed)),
		handlers: map[raft.NodeID]raft.Peer{},
		down:     map[raft.NodeID]bool{},
		blocked:  map[link]bool{},
	}
}

// Transport returns the network as seen by node self.
func (n *Memnet) Transport(self raft.NodeID) raft.Transport { return &memTransport{n: n, self: self} }

// SetNodeConnected cuts a node off from (or reattaches it to) everyone.
func (n *Memnet) SetNodeConnected(id raft.NodeID, connected bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = !connected
}

// Unserve removes a node's handlers (used when a node crashes).
func (n *Memnet) Unserve(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

// Block cuts the directed link from->to (one-way partition).
func (n *Memnet) Block(from, to raft.NodeID, blocked bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[link{from, to}] = blocked
}

// Partition blocks every link between different groups, both directions.
// Links inside a group are left untouched.
func (n *Memnet) Partition(groups ...[]raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, g := range groups {
		for j, h := range groups {
			if i == j {
				continue
			}
			for _, a := range g {
				for _, b := range h {
					n.blocked[link{a, b}] = true
				}
			}
		}
	}
}

// Heal removes all blocked links and reconnects all nodes.
func (n *Memnet) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = map[link]bool{}
	n.down = map[raft.NodeID]bool{}
}

func (n *Memnet) SetDropProb(p float64) { n.mu.Lock(); n.dropProb = p; n.mu.Unlock() }
func (n *Memnet) SetDupProb(p float64)  { n.mu.Lock(); n.dupProb = p; n.mu.Unlock() }
func (n *Memnet) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	n.delayMin, n.delayMax = min, max
	n.mu.Unlock()
}

func (n *Memnet) reachableLocked(from, to raft.NodeID) bool {
	return !n.down[from] && !n.down[to] && !n.blocked[link{from, to}]
}

// plan decides, under the lock, the fate of one message leg.
func (n *Memnet) plan(from, to raft.NodeID) (ok bool, delay time.Duration, dup bool, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.reachableLocked(from, to) {
		return false, 0, false, ErrUnreachable
	}
	if n.dropProb > 0 && n.rng.Float64() < n.dropProb {
		return false, 0, false, ErrDropped
	}
	if n.delayMax > 0 {
		delay = n.delayMin
		if span := n.delayMax - n.delayMin; span > 0 {
			delay += time.Duration(n.rng.Int63n(int64(span)))
		}
	}
	dup = n.dupProb > 0 && n.rng.Float64() < n.dupProb
	return true, delay, dup, nil
}

func (n *Memnet) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := n.clk.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Memnet) handler(to raft.NodeID) raft.Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.handlers[to]
}

func (n *Memnet) stillReachable(from, to raft.NodeID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reachableLocked(from, to)
}

// call delivers one RPC: request leg, handler, reply leg. Each leg can be
// dropped or delayed; a partition during flight drops the reply.
func call[A, R any](n *Memnet, ctx context.Context, from, to raft.NodeID, args A,
	clone func(A) A, invoke func(raft.Peer, context.Context, A) (R, error)) (R, error) {
	var zero R
	ok, delay, dup, err := n.plan(from, to)
	if !ok {
		return zero, err
	}
	if err := n.sleep(ctx, delay); err != nil {
		return zero, err
	}
	h := n.handler(to)
	if h == nil || !n.stillReachable(from, to) {
		return zero, ErrUnreachable
	}
	if dup {
		a2 := clone(args)
		go invoke(h, context.Background(), a2) // duplicate delivery; reply discarded
	}
	reply, herr := invoke(h, ctx, clone(args))
	ok, delay, _, err = n.plan(to, from) // reply leg
	if !ok {
		return zero, err
	}
	if err := n.sleep(ctx, delay); err != nil {
		return zero, err
	}
	if !n.stillReachable(to, from) {
		return zero, ErrUnreachable
	}
	return reply, herr
}

type memTransport struct {
	n    *Memnet
	self raft.NodeID
}

func (t *memTransport) Serve(id raft.NodeID, h raft.Peer) error {
	t.n.mu.Lock()
	defer t.n.mu.Unlock()
	t.n.handlers[id] = h
	return nil
}

func (t *memTransport) Peer(id raft.NodeID) raft.Peer { return &memPeer{n: t.n, from: t.self, to: id} }

type memPeer struct {
	n        *Memnet
	from, to raft.NodeID
}

func (p *memPeer) RequestVote(ctx context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return call(p.n, ctx, p.from, p.to, a,
		func(a *raft.RequestVoteArgs) *raft.RequestVoteArgs { c := *a; return &c },
		func(h raft.Peer, ctx context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return h.RequestVote(ctx, a)
		})
}

func (p *memPeer) AppendEntries(ctx context.Context, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	p.n.appendSent.Add(1)
	return call(p.n, ctx, p.from, p.to, a,
		(*raft.AppendEntriesArgs).Clone,
		func(h raft.Peer, ctx context.Context, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return h.AppendEntries(ctx, a)
		})
}

func (p *memPeer) InstallSnapshot(ctx context.Context, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return call(p.n, ctx, p.from, p.to, a,
		(*raft.InstallSnapshotArgs).Clone,
		func(h raft.Peer, ctx context.Context, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
			return h.InstallSnapshot(ctx, a)
		})
}
