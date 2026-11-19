package transport

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/adminpb"
)

// direct is a Transport that calls handlers in-process with no faults.
type direct struct {
	mu sync.Mutex
	h  map[raft.NodeID]raft.Peer
}

func newDirect() *direct { return &direct{h: map[raft.NodeID]raft.Peer{}} }
func (d *direct) Serve(id raft.NodeID, h raft.Peer) error {
	d.mu.Lock()
	d.h[id] = h
	d.mu.Unlock()
	return nil
}
func (d *direct) Peer(id raft.NodeID) raft.Peer {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.h[id]
}

// counter is a handler that counts deliveries and records the Term order.
type counter struct {
	mu    sync.Mutex
	votes int
	order []uint64
}

func (c *counter) RequestVote(_ context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c.mu.Lock()
	c.votes++
	c.order = append(c.order, a.Term)
	c.mu.Unlock()
	return &raft.RequestVoteReply{Term: a.Term, VoteGranted: true}, nil
}
func (c *counter) AppendEntries(context.Context, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return &raft.AppendEntriesReply{Success: true}, nil
}
func (c *counter) InstallSnapshot(context.Context, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{}, nil
}
func (c *counter) n() int { c.mu.Lock(); defer c.mu.Unlock(); return c.votes }

type rig struct {
	net   *FaultNet
	clk   *clock.FakeClock
	trs   map[raft.NodeID]raft.Transport
	hs    map[raft.NodeID]*counter
	inner *direct
}

func newRig(t *testing.T, seed int64, ids ...raft.NodeID) *rig {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed=%d", seed)
		}
	})
	r := &rig{clk: clock.NewFake(), inner: newDirect(), trs: map[raft.NodeID]raft.Transport{}, hs: map[raft.NodeID]*counter{}}
	r.net = NewFaultNet(seed, r.clk)
	t.Cleanup(r.net.Close)
	for _, id := range ids {
		r.hs[id] = &counter{}
		r.inner.Serve(id, r.hs[id])
		r.trs[id] = r.net.Wrap(id, r.inner)
	}
	return r
}

func (r *rig) vote(from, to raft.NodeID, term uint64) error {
	_, err := r.trs[from].Peer(to).RequestVote(context.Background(), &raft.RequestVoteArgs{Term: term})
	return err
}

// asyncVote runs a vote on its own goroutine; the result arrives on the channel.
func (r *rig) asyncVote(from, to raft.NodeID, term uint64) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- r.vote(from, to, term) }()
	return ch
}

// settle advances fake time in small steps until done returns true or d passed.
func (r *rig) settle(d time.Duration, done func() bool) {
	for waited := time.Duration(0); waited < d && !done(); waited += time.Millisecond {
		r.clk.Advance(time.Millisecond)
		time.Sleep(50 * time.Microsecond)
	}
}

func TestFaultyNoRulesPassesThrough(t *testing.T) {
	r := newRig(t, 1, "a", "b")
	for i := 0; i < 50; i++ {
		if err := r.vote("a", "b", 1); err != nil {
			t.Fatal(err)
		}
	}
	if r.hs["b"].n() != 50 {
		t.Fatalf("delivered %d of 50", r.hs["b"].n())
	}
}

func TestFaultyBlockIsDirectional(t *testing.T) {
	r := newRig(t, 2, "a", "b")
	r.net.Apply(Link("a", "b"))
	if err := r.vote("a", "b", 1); err != ErrBlocked {
		t.Fatalf("a->b err = %v, want ErrBlocked", err)
	}
	if r.hs["b"].n() != 0 {
		t.Fatal("blocked RPC was delivered")
	}
	if err := r.vote("b", "a", 1); err != nil {
		t.Fatalf("b->a should work (one-way block): %v", err)
	}
	r.net.Apply(Rule{From: "a", To: "b"}) // zero rule heals
	if err := r.vote("a", "b", 1); err != nil {
		t.Fatalf("after heal: %v", err)
	}
	if s := r.net.Stats(); s.Blocked != 1 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestFaultyDropRate(t *testing.T) {
	r := newRig(t, 3, "a", "b")
	r.net.Apply(Rule{From: "a", To: "b", Drop: 1})
	for i := 0; i < 20; i++ {
		if err := r.vote("a", "b", 1); err != ErrFaultDropped {
			t.Fatalf("drop=1: err = %v", err)
		}
	}
	r.net.Apply(Rule{From: "a", To: "b", Drop: 0.3})
	const N = 2000
	failed := 0
	before := r.hs["b"].n()
	for i := 0; i < N; i++ {
		if r.vote("a", "b", 1) != nil {
			failed++
		}
	}
	if failed < N*22/100 || failed > N*38/100 {
		t.Fatalf("drop 0.3 failed %d of %d", failed, N)
	}
	// Half the losses are reply losses: the handler ran but the caller saw an error.
	delivered := r.hs["b"].n() - before
	if delivered <= N-failed {
		t.Fatalf("handler ran %d times for %d successes: reply-loss never happened", delivered, N-failed)
	}
	r.net.Apply(Rule{From: "a", To: "b", Drop: 0.0001})
	r.net.Apply(Rule{From: "a", To: "b"})
	if err := r.vote("a", "b", 1); err != nil {
		t.Fatalf("no rule: %v", err)
	}
}

func TestFaultyDropDeterministicPerSeed(t *testing.T) {
	run := func(seed int64) []bool {
		r := newRig(t, seed, "a", "b")
		r.net.Apply(Rule{From: "a", To: "b", Drop: 0.5})
		out := make([]bool, 64)
		for i := range out {
			out[i] = r.vote("a", "b", 1) != nil
		}
		return out
	}
	a, b, c := run(7), run(7), run(8)
	same := func(x, y []bool) bool {
		for i := range x {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}
	if !same(a, b) {
		t.Fatal("same seed gave different drop patterns")
	}
	if same(a, c) {
		t.Fatal("different seeds gave the same drop pattern")
	}
}

func TestFaultyDelayUsesClock(t *testing.T) {
	r := newRig(t, 4, "a", "b")
	r.net.Apply(Rule{From: "a", To: "b", DelayMin: 100 * time.Millisecond, DelayMax: 200 * time.Millisecond})
	ch := r.asyncVote("a", "b", 1)
	select {
	case <-ch:
		t.Fatal("returned without any fake time passing")
	case <-time.After(30 * time.Millisecond):
	}
	if r.hs["b"].n() != 0 {
		t.Fatal("delivered before the request delay")
	}
	var fin bool
	r.settle(500*time.Millisecond, func() bool {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatal(err)
			}
			fin = true
		default:
		}
		return fin
	})
	if !fin {
		t.Fatal("never returned")
	}
	// request leg 100-200 ms plus reply leg 100-200 ms
	if el := r.clk.Now().Sub(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)); el < 200*time.Millisecond || el > 405*time.Millisecond {
		t.Fatalf("round trip took %v of fake time, want 200..400ms", el)
	}
	if r.net.Stats().Delayed != 1 {
		t.Fatalf("stats: %+v", r.net.Stats())
	}
}

func TestFaultyDelayRespectsContext(t *testing.T) {
	r := newRig(t, 5, "a", "b")
	r.net.Apply(Rule{From: "a", To: "b", DelayMin: time.Hour, DelayMax: 2 * time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() {
		_, err := r.trs["a"].Peer("b").RequestVote(ctx, &raft.RequestVoteArgs{})
		ch <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-ch:
		if err == nil {
			t.Fatal("want ctx error")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled RPC kept waiting")
	}
}

func TestFaultyDuplicate(t *testing.T) {
	r := newRig(t, 6, "a", "b")
	r.net.Apply(Rule{From: "a", To: "b", Dup: 1})
	if err := r.vote("a", "b", 9); err != nil {
		t.Fatal(err)
	}
	r.settle(300*time.Millisecond, func() bool { return r.hs["b"].n() >= 2 })
	if got := r.hs["b"].n(); got != 2 {
		t.Fatalf("Dup=1 delivered %d times, want 2", got)
	}
	r.hs["b"].mu.Lock()
	o := r.hs["b"].order
	r.hs["b"].mu.Unlock()
	if o[0] != 9 || o[1] != 9 {
		t.Fatalf("copies differ: %v", o)
	}
	if r.net.Stats().Duplicated != 1 {
		t.Fatalf("stats %+v", r.net.Stats())
	}
}

func TestFaultyReorder(t *testing.T) {
	r := newRig(t, 7, "a", "b")
	r.net.Apply(Rule{From: "a", To: "b", Reorder: true})
	const N = 30
	var wg sync.WaitGroup
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(term uint64) { defer wg.Done(); r.vote("a", "b", term) }(uint64(i))
	}
	r.settle(2*time.Second, func() bool { return r.hs["b"].n() >= N })
	wg.Wait()
	r.hs["b"].mu.Lock()
	o := append([]uint64(nil), r.hs["b"].order...)
	r.hs["b"].mu.Unlock()
	if len(o) != N {
		t.Fatalf("delivered %d of %d", len(o), N)
	}
	// Goroutine start order is not guaranteed, so compare against the
	// arrival times instead: with random delays up to 100 ms and 1 ms steps
	// the arrival order must not be monotone in launch order for 30 messages
	// unless the delays are all equal.
	if sort.SliceIsSorted(o, func(i, j int) bool { return o[i] < o[j] }) {
		t.Fatalf("30 reordered messages arrived in launch order: %v", o)
	}
	if r.net.Stats().Reordered != N {
		t.Fatalf("stats %+v", r.net.Stats())
	}
}

func TestFaultyWildcardPrecedence(t *testing.T) {
	r := newRig(t, 8, "a", "b", "c")
	r.net.Apply(Rule{Drop: 1})                         // everything
	r.net.Apply(Rule{From: "a", To: "b", Block: true}) // exact wins over wildcard
	if err := r.vote("a", "b", 1); err != ErrBlocked {
		t.Fatalf("exact: %v", err)
	}
	if err := r.vote("a", "c", 1); err != ErrFaultDropped {
		t.Fatalf("wildcard: %v", err)
	}
	r.net.Apply(Rule{From: "a", Block: true}) // from-a wildcard beats the global one
	if err := r.vote("a", "c", 1); err != ErrBlocked {
		t.Fatalf("from-wildcard: %v", err)
	}
	r.net.Clear()
	if err := r.vote("a", "c", 1); err != nil {
		t.Fatalf("after Clear: %v", err)
	}
}

func TestFaultyLocalIgnoresOtherSenders(t *testing.T) {
	r := newRig(t, 9, "a", "b")
	r.net.SetLocal("a")
	r.net.Apply(Link("b", "a"), Link("a", "b"))
	if err := r.vote("b", "a", 1); err != nil {
		t.Fatalf("b's rule should be ignored on a's process: %v", err)
	}
	if err := r.vote("a", "b", 1); err != ErrBlocked {
		t.Fatalf("a's own rule: %v", err)
	}
}

func partitionedPairs(t *testing.T, rules []Rule) map[ruleKey]bool {
	t.Helper()
	m := map[ruleKey]bool{}
	for _, r := range rules {
		if !r.Block {
			t.Fatalf("helper emitted non-block rule %+v", r)
		}
		m[ruleKey{r.From, r.To}] = true
	}
	return m
}

func TestPartitionHelpers(t *testing.T) {
	ids := []raft.NodeID{"n0", "n1", "n2", "n3", "n4"}

	// majority / minority split
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 50; i++ {
		maj, min := SplitMajority(ids, rng)
		if len(min) < 1 || len(maj) <= len(min) || len(maj)+len(min) != 5 {
			t.Fatalf("split %v | %v", maj, min)
		}
		got := partitionedPairs(t, Partition(maj, min))
		if len(got) != 2*len(maj)*len(min) {
			t.Fatalf("partition cut %d links, want %d", len(got), 2*len(maj)*len(min))
		}
		for _, a := range maj {
			for _, b := range min {
				if !got[ruleKey{a, b}] || !got[ruleKey{b, a}] {
					t.Fatalf("link %s<->%s not cut both ways", a, b)
				}
			}
		}
		for _, a := range maj {
			for _, b := range maj {
				if got[ruleKey{a, b}] {
					t.Fatalf("link inside the majority cut: %s->%s", a, b)
				}
			}
		}
	}
	if maj, min := SplitMajority(ids[:3], rng); len(maj) != 2 || len(min) != 1 {
		t.Fatalf("3-node split %v | %v", maj, min)
	}

	// isolate
	got := partitionedPairs(t, Isolate("n2", ids))
	if len(got) != 8 {
		t.Fatalf("isolate cut %d links, want 8", len(got))
	}
	for _, o := range without(ids, "n2") {
		if !got[ruleKey{"n2", o}] || !got[ruleKey{o, "n2"}] {
			t.Fatalf("n2<->%s not cut", o)
		}
	}

	// bridge: n2 reaches both halves, the halves cannot reach each other
	got = partitionedPairs(t, Bridge("n2", []raft.NodeID{"n0", "n1"}, []raft.NodeID{"n3", "n4"}))
	for k := range got {
		if k.from == "n2" || k.to == "n2" {
			t.Fatalf("bridge node has a cut link %v", k)
		}
	}
	if !got[ruleKey{"n0", "n3"}] || !got[ruleKey{"n4", "n1"}] || got[ruleKey{"n0", "n1"}] || len(got) != 8 {
		t.Fatalf("bridge links: %v", got)
	}

	// one-way
	got = partitionedPairs(t, OneWay("n0", "n1"))
	if len(got) != 1 || !got[ruleKey{"n0", "n1"}] {
		t.Fatalf("one-way: %v", got)
	}

	// Heal deletes pairwise rules
	r := newRig(t, 10, "a", "b", "c")
	r.net.Apply(Partition([]raft.NodeID{"a"}, []raft.NodeID{"b", "c"})...)
	if r.vote("a", "b", 1) != ErrBlocked || r.vote("c", "a", 1) != ErrBlocked {
		t.Fatal("partition not in effect")
	}
	if r.vote("b", "c", 1) != nil {
		t.Fatal("link inside the group should work")
	}
	r.net.Apply(Heal([]raft.NodeID{"a", "b", "c"})...)
	if r.vote("a", "b", 1) != nil || r.vote("c", "a", 1) != nil || len(r.net.Rules()) != 0 {
		t.Fatalf("heal left rules: %v", r.net.Rules())
	}
}

func TestFaultNetSetRulesProto(t *testing.T) {
	r := newRig(t, 11, "a", "b")
	err := r.net.SetRules(false, []*adminpb.NetRule{{From: "a", To: "b", DropProb: 0.5, DelayMinMs: 1, DelayMaxMs: 5, DupProb: 0.25, Reorder: true}})
	if err != nil {
		t.Fatal(err)
	}
	rs := r.net.Rules()
	want := Rule{From: "a", To: "b", Drop: 0.5, DelayMin: time.Millisecond, DelayMax: 5 * time.Millisecond, Dup: 0.25, Reorder: true}
	if len(rs) != 1 || rs[0] != want {
		t.Fatalf("rules: %+v", rs)
	}
	if got, _ := RuleFromProto(want.ToProto()); got != want {
		t.Fatalf("round trip: %+v", got)
	}
	if err := r.net.SetRules(false, []*adminpb.NetRule{{From: "a", To: "b", DropProb: 2}}); err == nil {
		t.Fatal("drop prob 2 accepted")
	}
	if err := r.net.SetRules(false, []*adminpb.NetRule{{DelayMinMs: 9, DelayMaxMs: 1}}); err == nil {
		t.Fatal("inverted delay accepted")
	}
	r.net.SetRules(true, nil) // clear_all
	if len(r.net.Rules()) != 0 {
		t.Fatal("clear_all kept rules")
	}
}

func TestFaultyCloseWakesDelays(t *testing.T) {
	r := newRig(t, 12, "a", "b")
	r.net.Apply(Rule{From: "a", To: "b", DelayMin: time.Hour, DelayMax: 2 * time.Hour})
	ch := r.asyncVote("a", "b", 1)
	time.Sleep(10 * time.Millisecond)
	r.net.Close()
	select {
	case err := <-ch:
		if err != ErrFaultyClosed {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake the delayed RPC")
	}
}

type addrRec struct {
	*direct
	got atomic.Value
}

func (a *addrRec) SetAddrs(m map[raft.NodeID]string) { a.got.Store(m) }

func TestFaultyForwardsAddrs(t *testing.T) {
	in := &addrRec{direct: newDirect()}
	n := NewFaultNet(1, clock.NewFake())
	tr := n.Wrap("a", in)
	tr.(raft.AddrAware).SetAddrs(map[raft.NodeID]string{"x": "h:1"})
	if m, _ := in.got.Load().(map[raft.NodeID]string); m["x"] != "h:1" {
		t.Fatal("SetAddrs not forwarded")
	}
}
