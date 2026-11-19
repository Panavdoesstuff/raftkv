package kv_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
)

// AddServer and RemoveServer make callerFor a kv.MembershipCaller, over the
// same lossy client network as Command.
func (c callerFor) AddServer(ctx context.Context, to raft.NodeID, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	srv, err := c.kc.reachable(c.id, to)
	if err != nil {
		return nil, err
	}
	return srv.AddServer(ctx, a)
}

func (c callerFor) RemoveServer(ctx context.Context, to raft.NodeID, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	srv, err := c.kc.reachable(c.id, to)
	if err != nil {
		return nil, err
	}
	return srv.RemoveServer(ctx, a)
}

func (kc *kvCluster) reachable(clientID string, to raft.NodeID) (*kv.Server, error) {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	if m, ok := kc.reach[clientID]; ok && !m[to] {
		return nil, errors.New("unreachable")
	}
	if kc.down[to] || kc.servers[to] == nil {
		return nil, errors.New("down")
	}
	return kc.servers[to], nil
}

// loadGen runs closed-loop clients appending unique tokens until stopped, and
// records errors and the slowest op (cluster time).
type loadGen struct {
	kc      *kvCluster
	t       *testing.T
	stop    atomic.Bool
	wg      sync.WaitGroup
	clients []*kv.Client

	ops    atomic.Int64
	errs   atomic.Int64
	maxLat atomic.Int64 // ns

	mu     sync.Mutex
	tokens map[string][]string // key -> tokens in the order the client appended them
}

func newLoadGen(t *testing.T, kc *kvCluster, n int) *loadGen {
	g := &loadGen{kc: kc, t: t, tokens: map[string][]string{}}
	for i := 0; i < n; i++ {
		cl := kc.client(fmt.Sprintf("load%d", i))
		g.clients = append(g.clients, cl)
		g.wg.Add(1)
		go func(i int, cl *kv.Client) {
			defer g.wg.Done()
			key := fmt.Sprintf("k%d", i)
			for j := 0; !g.stop.Load(); j++ {
				tok := fmt.Sprintf("<%d.%d>", i, j)
				t0 := kc.Clock.Now()
				if err := cl.Append(ctxBG(), key, tok); err != nil {
					g.errs.Add(1)
					t.Errorf("client %d append %d: %v", i, j, err)
					return
				}
				if lat := int64(kc.Clock.Now().Sub(t0)); lat > g.maxLat.Load() {
					g.maxLat.Store(lat)
				}
				g.mu.Lock()
				g.tokens[key] = append(g.tokens[key], tok)
				g.mu.Unlock()
				g.ops.Add(1)
			}
		}(i, cl)
	}
	return g
}

// setServers points every load client at the given servers.
func (g *loadGen) setServers(ids []raft.NodeID) {
	for _, cl := range g.clients {
		cl.SetServers(ids)
	}
}

// finish stops the load (advancing time until the clients return) and checks
// that every appended token is in the store exactly once, in order.
func (g *loadGen) finish(t *testing.T, reader *kv.Client) {
	t.Helper()
	g.stop.Store(true)
	t.Logf("load: %d ops acknowledged, slowest %v", g.ops.Load(), time.Duration(g.maxLat.Load()))
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	g.kc.until(t, done, 60*time.Second, "load clients to stop")
	if g.errs.Load() != 0 {
		t.Fatalf("%d client ops failed (seed=%d)", g.errs.Load(), g.kc.seed)
	}
	g.kc.do(t, "final reads", func() {
		for key, toks := range g.tokens {
			v, err := reader.Get(ctxBG(), key)
			if err != nil {
				t.Errorf("get %s: %v", key, err)
				return
			}
			if want := strings.Join(toks, ""); v != want {
				t.Errorf("key %s: value does not hold each acknowledged append exactly once, in order\n got %q\nwant %q", key, v, want)
			}
		}
	})
}

// voterIDs returns the leader's current voters.
func (kc *kvCluster) voterIDs(leader raft.NodeID) []raft.NodeID {
	return kc.Node(leader).(*kvNode).Config().Voters
}

// TestAddServerUnderLoad: clients keep writing while a fourth server joins
// (learner, catch-up, promotion). No op fails or stalls, and the new server
// ends up with the same data.
func TestAddServerUnderLoad(t *testing.T) {
	kc := newKVCluster(t, 3)
	leader := kc.CheckOneLeader()
	g := newLoadGen(t, kc, 4)
	kc.advance(500 * time.Millisecond)
	before := g.ops.Load()
	if before == 0 {
		t.Fatalf("no ops completed before the add (seed=%d)", kc.seed)
	}

	kc.AddNode("n3")
	admin := kc.client("admin")
	kc.do(t, "AddServer", func() {
		if err := admin.AddServer(ctxBG(), "n3", "mem:n3"); err != nil {
			t.Errorf("AddServer: %v", err)
		}
	})
	kc.advance(time.Second)
	if g.ops.Load() == before {
		t.Fatalf("no client op completed during or after the add (seed=%d)", kc.seed)
	}
	if got := fmt.Sprint(kc.voterIDs(kc.CheckOneLeader())); got != "[n0 n1 n2 n3]" {
		t.Fatalf("voters %s (leader was %s)", got, leader)
	}
	g.finish(t, admin)
	if lat := time.Duration(g.maxLat.Load()); lat > 3*time.Second {
		t.Fatalf("an op took %v of cluster time during the add (seed=%d)", lat, kc.seed)
	}
	// The new server converges to the same state as the others.
	waitConverged(t, kc, g)
}

// waitConverged waits until every live node's store has every acknowledged key.
func waitConverged(t *testing.T, kc *kvCluster, g *loadGen) {
	t.Helper()
	want := map[string]string{}
	for k, toks := range g.tokens {
		want[k] = strings.Join(toks, "")
	}
	start := time.Now()
	for waited := time.Duration(0); ; waited += 10 * time.Millisecond {
		ok := true
		for _, id := range kc.IDs() {
			if !kc.Alive(id) || !kc.Node(id).(*kvNode).Config().IsVoter(id) {
				continue
			}
			for k, w := range want {
				if v, _ := kc.Node(id).(*kvNode).srv.Store().Peek(k); v != w {
					ok = false
				}
			}
		}
		if ok {
			return
		}
		if !kc.Within(start, waited, 20*time.Second) {
			t.Fatalf("stores did not converge (seed=%d)", kc.seed)
		}
		kc.advance(10 * time.Millisecond)
	}
}

// TestGrowShrink: 3 -> 5 -> 3 under load, with partitions, removing the
// current leader on the way down.
func TestGrowShrink(t *testing.T) {
	kc := newKVCluster(t, 3)
	rng := rand.New(rand.NewSource(kc.seed))
	kc.CheckOneLeader()
	g := newLoadGen(t, kc, 4)
	admin := kc.client("admin")
	kc.advance(300 * time.Millisecond)

	partitionOne := func(what string) {
		var all []raft.NodeID
		for _, id := range kc.IDs() {
			if kc.Alive(id) && kc.Node(id).(*kvNode).Config().IsVoter(id) {
				all = append(all, id)
			}
		}
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		victim := all[rng.Intn(len(all))]
		var rest []raft.NodeID
		for _, id := range kc.IDs() {
			if id != victim {
				rest = append(rest, id)
			}
		}
		kc.Partition([]raft.NodeID{victim}, rest)
		kc.advance(time.Second + time.Duration(rng.Intn(1500))*time.Millisecond)
		kc.Heal()
		kc.advance(time.Second)
		kc.CheckTermLeaders()
		t.Logf("%s: partitioned %s", what, victim)
	}

	// Grow to 5.
	for _, id := range []raft.NodeID{"n3", "n4"} {
		kc.AddNode(id)
		kc.do(t, "AddServer "+string(id), func() {
			if err := admin.AddServer(ctxBG(), id, "mem:"+string(id)); err != nil {
				t.Errorf("AddServer %s: %v", id, err)
			}
		})
		l := kc.CheckOneLeader()
		g.setServers(kc.voterIDs(l))
		kc.advance(300 * time.Millisecond)
	}
	if got := fmt.Sprint(kc.voterIDs(kc.CheckOneLeader())); got != "[n0 n1 n2 n3 n4]" {
		t.Fatalf("voters after growing: %s", got)
	}
	partitionOne("5 nodes")

	// Shrink to 3: first the current leader, then another voter.
	for i := 0; i < 2; i++ {
		l := kc.CheckOneLeader()
		victim := l
		if i == 1 {
			for _, id := range kc.voterIDs(l) {
				if id != l {
					victim = id
					break
				}
			}
		}
		kc.do(t, "RemoveServer "+string(victim), func() {
			if err := admin.RemoveServer(ctxBG(), victim); err != nil {
				t.Errorf("RemoveServer %s: %v", victim, err)
			}
		})
		kc.Crash(victim) // decommission it
		l = kc.CheckOneLeader()
		g.setServers(kc.voterIDs(l))
		kc.advance(500 * time.Millisecond)
	}
	if n := len(kc.voterIDs(kc.CheckOneLeader())); n != 3 {
		t.Fatalf("%d voters after shrinking, want 3", n)
	}
	partitionOne("3 nodes")

	g.finish(t, admin)
	waitConverged(t, kc, g)
	kc.CheckLogsAgree()
	kc.CheckTermLeaders()
}

// TestMembershipRejectionsThroughClient: the failure modes reach the client as
// errors it can act on.
func TestMembershipRejectionsThroughClient(t *testing.T) {
	kc := newKVCluster(t, 3)
	kc.CheckOneLeader()
	admin := kc.client("admin")
	kc.AddNode("n3")
	kc.Disconnect("n3")
	kc.do(t, "AddServer to unreachable node", func() {
		err := admin.AddServer(ctxBG(), "n3", "mem:n3")
		if !errors.Is(err, kv.ErrMembershipFailed) {
			t.Errorf("AddServer to a node that never catches up: %v, want ErrMembershipFailed", err)
		}
	})
	kc.do(t, "RemoveServer of a learner", func() {
		if err := admin.RemoveServer(ctxBG(), "n3"); err != nil {
			t.Errorf("RemoveServer of the stuck learner: %v", err)
		}
	})
	kc.do(t, "idempotent remove", func() {
		if err := admin.RemoveServer(ctxBG(), "n3"); err != nil {
			t.Errorf("repeated RemoveServer: %v", err)
		}
	})
}
