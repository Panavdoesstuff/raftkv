package transport

import (
	"context"
	"errors"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/adminpb"
)

var (
	ErrBlocked       = errors.New("faulty: link blocked")
	ErrFaultDropped  = errors.New("faulty: message dropped")
	ErrFaultyClosed  = errors.New("faulty: network closed")
	errInvalidRule   = errors.New("faulty: invalid rule")
	reorderSpreadDef = 100 * time.Millisecond
)

// Rule is the fault applied to RPCs sent from From to To (the direction of the
// request). An empty From or To matches any node. The zero rule means "no
// fault".
type Rule struct {
	From, To raft.NodeID

	// Block fails every RPC without delivering it (a cut link; set only one
	// direction for a one-way partition).
	Block bool
	// Drop is the probability that an RPC is lost. Half of the losses happen
	// before delivery (the handler never runs); the other half after (the
	// handler runs and its reply is lost), which is the nastier case.
	Drop float64
	// DelayMin..DelayMax is a uniform random delay added to the request leg and
	// again, independently, to the reply leg. It is waited on the Clock, so on
	// a FakeClock it costs no real time and stays deterministic.
	DelayMin, DelayMax time.Duration
	// Dup is the probability that the request is delivered twice; the second
	// copy arrives after an extra random delay and its reply is discarded.
	Dup float64
	// Reorder adds a random extra delay (up to max(DelayMax, 100 ms)) to the
	// request leg. RPCs are sent concurrently, so unequal delays let a later
	// message overtake an earlier one.
	Reorder bool
}

func (r Rule) isZero() bool {
	return !r.Block && r.Drop == 0 && r.DelayMax == 0 && r.DelayMin == 0 && r.Dup == 0 && !r.Reorder
}

// Stats counts what the network has done to messages.
type Stats struct {
	Blocked, Dropped, Delayed, Duplicated, Reordered uint64
}

type ruleKey struct{ from, to raft.NodeID }

// FaultNet holds the rule set shared by the faulty Transports wrapped from it.
// On memnet one FaultNet serves the whole cluster (rules for every link). In a
// real server each process owns one with SetLocal(self), so it keeps only the
// rules for its own outgoing links; the chaos controller sends each node the
// rules it needs. FaultNet implements admin.NetworkRules.
//
// Randomness: each directed link has its own rand.Rand derived from the seed
// and the link, so a link's loss/delay pattern depends only on the seed and
// on how many messages crossed that link.
type FaultNet struct {
	clk  clock.Clock
	seed int64
	done chan struct{}
	once sync.Once

	mu    sync.Mutex
	self  raft.NodeID // "" = shared, applies all rules
	rules map[ruleKey]Rule
	rngs  map[ruleKey]*rand.Rand

	blocked, dropped, delayed, duplicated, reordered atomic.Uint64
}

func NewFaultNet(seed int64, clk clock.Clock) *FaultNet {
	return &FaultNet{clk: clk, seed: seed, done: make(chan struct{}),
		rules: map[ruleKey]Rule{}, rngs: map[ruleKey]*rand.Rand{}}
}

// SetLocal makes this FaultNet belong to node self: rules whose From is another
// node are ignored (they are some other process's business).
func (n *FaultNet) SetLocal(self raft.NodeID) {
	n.mu.Lock()
	n.self = self
	n.mu.Unlock()
}

// Close wakes every pending delay and rejects new RPCs.
func (n *FaultNet) Close() { n.once.Do(func() { close(n.done) }) }

// Stats returns the counters so far.
func (n *FaultNet) Stats() Stats {
	return Stats{n.blocked.Load(), n.dropped.Load(), n.delayed.Load(), n.duplicated.Load(), n.reordered.Load()}
}

// Wrap returns the network as node self sees it: every RPC to a peer goes
// through the rules first, then to inner.
func (n *FaultNet) Wrap(self raft.NodeID, inner raft.Transport) raft.Transport {
	return &faultyTransport{n: n, self: self, inner: inner}
}

// Apply installs rules. A rule replaces the one for the same (From, To) key; a
// zero rule deletes it.
func (n *FaultNet) Apply(rules ...Rule) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, r := range rules {
		if n.self != "" && r.From != "" && r.From != n.self {
			continue
		}
		k := ruleKey{r.From, r.To}
		if r.isZero() {
			delete(n.rules, k)
		} else {
			n.rules[k] = r
		}
	}
}

// Clear removes every rule (heals everything).
func (n *FaultNet) Clear() {
	n.mu.Lock()
	n.rules = map[ruleKey]Rule{}
	n.mu.Unlock()
}

// Rules returns the active rules, sorted by (From, To).
func (n *FaultNet) Rules() []Rule {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Rule, 0, len(n.rules))
	for _, r := range n.rules {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}

// SetRules implements admin.NetworkRules.
func (n *FaultNet) SetRules(clearAll bool, rules []*adminpb.NetRule) error {
	rs := make([]Rule, 0, len(rules))
	for _, p := range rules {
		r, err := RuleFromProto(p)
		if err != nil {
			return err
		}
		rs = append(rs, r)
	}
	if clearAll {
		n.Clear()
	}
	n.Apply(rs...)
	return nil
}

// RuleFromProto validates and converts an admin NetRule.
func RuleFromProto(p *adminpb.NetRule) (Rule, error) {
	r := Rule{
		From: raft.NodeID(p.From), To: raft.NodeID(p.To), Block: p.Block,
		Drop: p.DropProb, Dup: p.DupProb, Reorder: p.Reorder,
		DelayMin: time.Duration(p.DelayMinMs) * time.Millisecond,
		DelayMax: time.Duration(p.DelayMaxMs) * time.Millisecond,
	}
	if r.Drop < 0 || r.Drop > 1 || r.Dup < 0 || r.Dup > 1 || r.DelayMin < 0 || r.DelayMax < r.DelayMin {
		return Rule{}, errInvalidRule
	}
	return r, nil
}

// ToProto converts a rule for the admin SetNetRules RPC.
func (r Rule) ToProto() *adminpb.NetRule {
	return &adminpb.NetRule{
		From: string(r.From), To: string(r.To), Block: r.Block, DropProb: r.Drop,
		DelayMinMs: r.DelayMin.Milliseconds(), DelayMaxMs: r.DelayMax.Milliseconds(),
		DupProb: r.Dup, Reorder: r.Reorder,
	}
}

// ---- partition helpers ----

// Link returns the rule that cuts the directed link from -> to.
func Link(from, to raft.NodeID) Rule { return Rule{From: from, To: to, Block: true} }

// OneWay cuts only from -> to: from's RPCs to `to` are never delivered, while
// `to` can still send RPCs to from (and gets replies to them).
func OneWay(from, to raft.NodeID) []Rule { return []Rule{Link(from, to)} }

// Partition cuts every link between different groups, in both directions.
// Links inside a group, and nodes in no group, are untouched.
func Partition(groups ...[]raft.NodeID) []Rule {
	var out []Rule
	for i, g := range groups {
		for j, h := range groups {
			if i == j {
				continue
			}
			for _, a := range g {
				for _, b := range h {
					out = append(out, Link(a, b))
				}
			}
		}
	}
	return out
}

// Isolate cuts id off from every other node in all, in both directions.
func Isolate(id raft.NodeID, all []raft.NodeID) []Rule {
	rest := without(all, id)
	return Partition([]raft.NodeID{id}, rest)
}

// Bridge splits the other nodes into the two sides a and b (cut from each
// other) while bridge, in neither, keeps a link to both: the classic
// non-transitive partition. It returns the rules for all of ids.
func Bridge(bridge raft.NodeID, a, b []raft.NodeID) []Rule { return Partition(a, b) }

// SplitMajority splits ids into a majority and a minority of random members
// (the minority has 1..(n-1)/2 nodes), using rng. ids order is irrelevant.
func SplitMajority(ids []raft.NodeID, rng *rand.Rand) (major, minor []raft.NodeID) {
	s := append([]raft.NodeID(nil), ids...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	rng.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
	k := 1
	if m := (len(s) - 1) / 2; m > 1 {
		k = 1 + rng.Intn(m)
	}
	if k >= len(s) {
		k = len(s) / 2
	}
	return s[k:], s[:k]
}

// Heal returns zero rules for every directed pair in ids, which deletes the
// pairwise rules when applied (wildcard rules are untouched).
func Heal(ids []raft.NodeID) []Rule {
	var out []Rule
	for _, a := range ids {
		for _, b := range ids {
			if a != b {
				out = append(out, Rule{From: a, To: b})
			}
		}
	}
	return out
}

func without(ids []raft.NodeID, drop raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for _, id := range ids {
		if id != drop {
			out = append(out, id)
		}
	}
	return out
}

// ---- the wrapper ----

// plan is one RPC's fate, drawn under the lock from the link's own RNG.
type plan struct {
	block      bool
	loseReq    bool
	loseReply  bool
	reqDelay   time.Duration
	replyDelay time.Duration
	dup        bool
	dupDelay   time.Duration
	reordered  bool
	anyDelay   bool
}

func (n *FaultNet) lookupLocked(from, to raft.NodeID) (Rule, bool) {
	for _, k := range []ruleKey{{from, to}, {from, ""}, {"", to}, {"", ""}} {
		if r, ok := n.rules[k]; ok {
			return r, true
		}
	}
	return Rule{}, false
}

func (n *FaultNet) rngLocked(from, to raft.NodeID) *rand.Rand {
	k := ruleKey{from, to}
	r := n.rngs[k]
	if r == nil {
		h := fnv.New64a()
		h.Write([]byte(from))
		h.Write([]byte{0})
		h.Write([]byte(to))
		r = rand.New(rand.NewSource(n.seed ^ int64(h.Sum64())))
		n.rngs[k] = r
	}
	return r
}

func uniform(rng *rand.Rand, lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rng.Int63n(int64(hi-lo)))
}

func (n *FaultNet) decide(from, to raft.NodeID) plan {
	n.mu.Lock()
	defer n.mu.Unlock()
	r, ok := n.lookupLocked(from, to)
	if !ok {
		return plan{}
	}
	var p plan
	if r.Block {
		p.block = true
		return p
	}
	rng := n.rngLocked(from, to)
	if r.Drop > 0 && rng.Float64() < r.Drop {
		if rng.Intn(2) == 0 {
			p.loseReq = true
			return p
		}
		p.loseReply = true
	}
	if r.DelayMax > 0 {
		p.reqDelay = uniform(rng, r.DelayMin, r.DelayMax)
		p.replyDelay = uniform(rng, r.DelayMin, r.DelayMax)
	}
	if r.Reorder {
		spread := r.DelayMax
		if spread < reorderSpreadDef {
			spread = reorderSpreadDef
		}
		p.reqDelay += uniform(rng, 0, spread)
		p.reordered = true
	}
	if r.Dup > 0 && rng.Float64() < r.Dup {
		p.dup = true
		p.dupDelay = uniform(rng, 0, reorderSpreadDef)
	}
	return p
}

func (n *FaultNet) sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-n.done:
		return ErrFaultyClosed
	default:
	}
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
	case <-n.done:
		return ErrFaultyClosed
	}
}

func faultCall[A, R any](n *FaultNet, ctx context.Context, from, to raft.NodeID, args A,
	clone func(A) A, call func(context.Context, A) (R, error)) (R, error) {
	var zero R
	p := n.decide(from, to)
	switch {
	case p.block:
		n.blocked.Add(1)
		return zero, ErrBlocked
	case p.loseReq:
		n.dropped.Add(1)
		return zero, ErrFaultDropped
	}
	if p.reqDelay > 0 || p.replyDelay > 0 {
		n.delayed.Add(1)
	}
	if p.reordered {
		n.reordered.Add(1)
	}
	if err := n.sleep(ctx, p.reqDelay); err != nil {
		return zero, err
	}
	if p.dup {
		n.duplicated.Add(1)
		a2 := clone(args)
		bg := context.WithoutCancel(ctx)
		go func() {
			if n.sleep(bg, p.dupDelay) == nil {
				call(bg, a2) // second delivery; its reply is discarded
			}
		}()
	}
	reply, err := call(ctx, args)
	if p.loseReply {
		n.dropped.Add(1)
		return zero, ErrFaultDropped
	}
	if serr := n.sleep(ctx, p.replyDelay); serr != nil {
		return zero, serr
	}
	return reply, err
}

type faultyTransport struct {
	n     *FaultNet
	self  raft.NodeID
	inner raft.Transport
}

func (t *faultyTransport) Serve(id raft.NodeID, h raft.Peer) error { return t.inner.Serve(id, h) }
func (t *faultyTransport) Peer(id raft.NodeID) raft.Peer {
	return &faultyPeer{n: t.n, from: t.self, to: id, inner: t.inner}
}

// SetAddrs passes membership address updates through (raft.AddrAware).
func (t *faultyTransport) SetAddrs(addrs map[raft.NodeID]string) {
	if a, ok := t.inner.(raft.AddrAware); ok {
		a.SetAddrs(addrs)
	}
}

type faultyPeer struct {
	n        *FaultNet
	from, to raft.NodeID
	inner    raft.Transport
}

func (p *faultyPeer) RequestVote(ctx context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return faultCall(p.n, ctx, p.from, p.to, a,
		func(a *raft.RequestVoteArgs) *raft.RequestVoteArgs { c := *a; return &c },
		func(ctx context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return p.inner.Peer(p.to).RequestVote(ctx, a)
		})
}

func (p *faultyPeer) AppendEntries(ctx context.Context, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return faultCall(p.n, ctx, p.from, p.to, a, (*raft.AppendEntriesArgs).Clone,
		func(ctx context.Context, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return p.inner.Peer(p.to).AppendEntries(ctx, a)
		})
}

func (p *faultyPeer) InstallSnapshot(ctx context.Context, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return faultCall(p.n, ctx, p.from, p.to, a, (*raft.InstallSnapshotArgs).Clone,
		func(ctx context.Context, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
			return p.inner.Peer(p.to).InstallSnapshot(ctx, a)
		})
}
