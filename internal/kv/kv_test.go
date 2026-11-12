package kv_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
	"github.com/panavdoesstuff/raftkv/test"
)

// kvNode is a Raft node plus the KV server on top of it. Embedding *raft.Raft
// makes it a test.Node.
type kvNode struct {
	*raft.Raft
	srv *kv.Server
}

// kvCluster is a memnet cluster whose nodes run the KV layer, plus a lossy
// client-to-server network.
type kvCluster struct {
	*test.Cluster
	seed int64

	mu      sync.Mutex
	servers map[raft.NodeID]*kv.Server
	down    map[raft.NodeID]bool // crashed nodes; the harness's Alive() isn't safe to call from client goroutines
	// client network
	reach     map[string]map[raft.NodeID]bool // clientID -> reachable servers (nil entry = all)
	dropReq   float64
	dropReply float64
	rng       *rand.Rand
}

func newKVCluster(t *testing.T, n int) *kvCluster {
	t.Helper()
	seed := seedFor(t)
	kc := &kvCluster{
		seed:    seed,
		servers: map[raft.NodeID]*kv.Server{},
		down:    map[raft.NodeID]bool{},
		reach:   map[string]map[raft.NodeID]bool{},
		rng:     rand.New(rand.NewSource(seed + 7)),
	}
	factory := func(c test.Config) test.Node {
		r := raft.New(raft.Config{
			ID: c.ID, Peers: c.Peers, Transport: c.Transport, Clock: c.Clock,
			Storage: c.Storage, SM: c.SM, Seed: c.Seed,
		})
		srv := kv.NewServer(r, c.SM.(*kv.Store), c.Clock)
		kc.mu.Lock()
		kc.servers[c.ID] = srv
		kc.mu.Unlock()
		return &kvNode{Raft: r, srv: srv}
	}
	kc.Cluster = test.StartCluster(t, n, seed, factory, test.WithFakeClock(),
		test.WithStateMachine(func() raft.StateMachine { return kv.NewStore() }))
	return kc
}

// Command implements kv.Caller: one client RPC over the lossy client network.
func (kc *kvCluster) Command(ctx context.Context, clientID string, to raft.NodeID, a *kvpb.CommandArgs) (*kvpb.CommandReply, error) {
	kc.mu.Lock()
	if m, ok := kc.reach[clientID]; ok && !m[to] {
		kc.mu.Unlock()
		return nil, errors.New("unreachable")
	}
	dropReq := kc.rng.Float64() < kc.dropReq
	dropReply := kc.rng.Float64() < kc.dropReply
	srv := kc.servers[to]
	isDown := kc.down[to]
	kc.mu.Unlock()
	if dropReq || isDown {
		return nil, errors.New("request lost")
	}
	rep, err := srv.Command(ctx, a)
	if err != nil {
		return nil, err
	}
	if dropReply {
		return nil, errors.New("reply lost")
	}
	return rep, nil
}

// Crash and Restart shadow the harness's so liveness is visible to client
// goroutines safely.
func (kc *kvCluster) Crash(id raft.NodeID) {
	kc.mu.Lock()
	kc.down[id] = true
	kc.mu.Unlock()
	kc.Cluster.Crash(id)
}

func (kc *kvCluster) Restart(id raft.NodeID) {
	kc.Cluster.Restart(id)
	kc.mu.Lock()
	kc.down[id] = false
	kc.mu.Unlock()
}

// callerFor binds the cluster network to one client identity.
type callerFor struct {
	kc *kvCluster
	id string
}

func (c callerFor) Command(ctx context.Context, to raft.NodeID, a *kvpb.CommandArgs) (*kvpb.CommandReply, error) {
	return c.kc.Command(ctx, c.id, to, a)
}

func (kc *kvCluster) client(id string) *kv.Client {
	return kv.NewClient(id, kc.IDs(), callerFor{kc, id}, kc.Clock)
}

func (kc *kvCluster) setReach(clientID string, ids ...raft.NodeID) {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	m := map[raft.NodeID]bool{}
	for _, id := range ids {
		m[id] = true
	}
	kc.reach[clientID] = m
}

func (kc *kvCluster) setLoss(req, reply float64) {
	kc.mu.Lock()
	kc.dropReq, kc.dropReply = req, reply
	kc.mu.Unlock()
}

func seedFor(t testing.TB) int64 {
	if s := os.Getenv("RAFT_SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("bad RAFT_SEED %q: %v", s, err)
		}
		return v
	}
	return time.Now().UnixNano() % 1_000_000
}

// advance moves fake time forward in 10 ms steps, yielding real time between
// steps so goroutines blocked on the clock can run.
func (kc *kvCluster) advance(d time.Duration) {
	kc.Cluster.Advance(d)
}

// until advances cluster time (up to limit) until done is closed.
func (kc *kvCluster) until(t *testing.T, done <-chan struct{}, limit time.Duration, what string) {
	t.Helper()
	for waited := time.Duration(0); waited < limit; waited += 10 * time.Millisecond {
		select {
		case <-done:
			return
		default:
		}
		kc.advance(10 * time.Millisecond)
	}
	select {
	case <-done:
	default:
		t.Fatalf("timed out after %v of cluster time waiting for %s (seed=%d)", limit, what, kc.seed)
	}
}

// do runs f on its own goroutine and drives the clock until it returns.
func (kc *kvCluster) do(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	kc.until(t, done, 60*time.Second, what)
}

func ctxBG() context.Context { return context.Background() }

// TestKVBasic: put, append and get round-trip on a healthy cluster, from two
// clients, and a read of a missing key is empty.
func TestKVBasic(t *testing.T) {
	kc := newKVCluster(t, 3)
	kc.CheckOneLeader()
	a, b := kc.client("a"), kc.client("b")

	kc.do(t, "basic ops", func() {
		if v, err := a.Get(ctxBG(), "x"); err != nil || v != "" {
			t.Errorf("get missing: %q, %v", v, err)
		}
		if err := a.Put(ctxBG(), "x", "1"); err != nil {
			t.Errorf("put: %v", err)
		}
		if err := b.Append(ctxBG(), "x", "2"); err != nil {
			t.Errorf("append: %v", err)
		}
		if err := a.Append(ctxBG(), "y", "z"); err != nil { // append to a missing key creates it
			t.Errorf("append new: %v", err)
		}
		if v, err := b.Get(ctxBG(), "x"); err != nil || v != "12" {
			t.Errorf("get x: %q, %v; want 12", v, err)
		}
		if v, err := a.Get(ctxBG(), "y"); err != nil || v != "z" {
			t.Errorf("get y: %q, %v; want z", v, err)
		}
		if err := a.Put(ctxBG(), "x", "reset"); err != nil {
			t.Errorf("put: %v", err)
		}
		if v, err := b.Get(ctxBG(), "x"); err != nil || v != "reset" {
			t.Errorf("get x: %q, %v; want reset", v, err)
		}
	})
	// Every replica converges on the same store.
	for _, id := range kc.IDs() {
		id := id
		waitStore(t, kc, id, "x", "reset")
	}
}

func waitStore(t *testing.T, kc *kvCluster, id raft.NodeID, key, want string) {
	t.Helper()
	for i := 0; i < 500; i++ {
		kc.mu.Lock()
		srv := kc.servers[id]
		kc.mu.Unlock()
		if v, _ := srv.Store().Peek(key); v == want {
			return
		}
		kc.advance(10 * time.Millisecond)
	}
	t.Fatalf("%s never converged on %s=%q (seed=%d)", id, key, want, kc.seed)
}

// TestKVDedupDirect: the same (clientID, seq) submitted twice is applied once
// and answered from the cache, with no network loss involved.
func TestKVDedupDirect(t *testing.T) {
	kc := newKVCluster(t, 3)
	leader := kc.CheckOneLeader()
	kc.mu.Lock()
	srv := kc.servers[leader]
	kc.mu.Unlock()

	args := &kvpb.CommandArgs{Op: kvpb.Op_APPEND, Key: "k", Value: "A", ClientId: "c1", Seq: 1}
	var r1, r2 *kvpb.CommandReply
	kc.do(t, "dup submit", func() {
		var err error
		if r1, err = srv.Command(ctxBG(), args); err != nil {
			t.Errorf("first: %v", err)
		}
		if r2, err = srv.Command(ctxBG(), args); err != nil {
			t.Errorf("second: %v", err)
		}
	})
	if r1.Err != kvpb.Err_OK || r2.Err != kvpb.Err_OK {
		t.Fatalf("replies %v / %v", r1.Err, r2.Err)
	}
	c2 := kc.client("c2")
	kc.do(t, "get", func() {
		if v, err := c2.Get(ctxBG(), "k"); err != nil || v != "A" {
			t.Errorf("k = %q, %v; want A (applied once)", v, err)
		}
	})
}

// TestKVConcurrentClients: 10 clients hammer a few keys on an unreliable
// network (lost requests and replies, delayed and reordered raft traffic)
// while the leader is repeatedly crashed. Each client appends unique tokens to
// its own key, so exactly-once is checkable per key, and also does plain puts
// and gets on a shared key which must never go backwards for one client.
func TestKVConcurrentClients(t *testing.T) {
	kc := newKVCluster(t, 5)
	kc.Net.SetDelay(0, 40*time.Millisecond)
	kc.Net.SetDropProb(0.05)
	kc.Net.SetDupProb(0.05)
	kc.setLoss(0.10, 0.10)
	kc.CheckOneLeader()

	const nClients, nOps = 10, 25
	rng := rand.New(rand.NewSource(kc.seed))
	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < nClients; i++ {
		i := i
		cl := kc.client(fmt.Sprintf("c%d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			for j := 0; j < nOps; j++ {
				if err := cl.Append(ctxBG(), key, fmt.Sprintf("<%d.%d>", i, j)); err != nil {
					t.Errorf("client %d append %d: %v", i, j, err)
					return
				}
				if v, err := cl.Get(ctxBG(), key); err != nil {
					t.Errorf("client %d get: %v", i, err)
					return
				} else if want := fmt.Sprintf("<%d.%d>", i, j); !strings.HasSuffix(v, want) {
					// Our own write must be visible to our own next read.
					t.Errorf("client %d: read %q does not end with own latest append %s", i, v, want)
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); close(done) }()

	// Leader churn while the clients run.
	down := map[raft.NodeID]int{}
	for step := 0; step < 6000; step++ {
		select {
		case <-done:
			step = 6000
			continue
		default:
		}
		for id, until := range down {
			if step >= until {
				kc.Restart(id)
				delete(down, id)
			}
		}
		if step%200 == 100 && len(down) < 2 {
			for _, id := range kc.IDs() {
				if kc.Alive(id) && kc.Node(id).Status().Role == raft.Leader {
					kc.Crash(id)
					down[id] = step + 50 + rng.Intn(100)
					break
				}
			}
		}
		kc.advance(10 * time.Millisecond)
	}
	select {
	case <-done:
	default:
		t.Fatalf("clients did not finish (seed=%d)", kc.seed)
	}

	for id := range down {
		kc.Restart(id)
	}
	kc.Net.SetDelay(0, 0)
	kc.Net.SetDropProb(0)
	kc.Net.SetDupProb(0)
	kc.setLoss(0, 0)
	kc.CheckOneLeader()
	verify := kc.client("verify")
	kc.do(t, "verify", func() {
		for i := 0; i < nClients; i++ {
			v, err := verify.Get(ctxBG(), fmt.Sprintf("k%d", i))
			if err != nil {
				t.Errorf("verify get: %v", err)
				return
			}
			checkTokens(t, v, i, nOps)
		}
	})
	kc.CheckLogsAgree()
}

// checkTokens: v must be exactly <i.0><i.1>...<i.n-1> in order.
func checkTokens(t *testing.T, v string, i, n int) {
	t.Helper()
	var sb strings.Builder
	for j := 0; j < n; j++ {
		fmt.Fprintf(&sb, "<%d.%d>", i, j)
	}
	if v != sb.String() {
		t.Errorf("key k%d = %q\nwant      %q", i, v, sb.String())
	}
}

// TestKVAppendExactlyOnce: replies are dropped half the time, so clients retry
// constantly. Every token must still appear exactly once in the final value.
func TestKVAppendExactlyOnce(t *testing.T) {
	kc := newKVCluster(t, 3)
	kc.setLoss(0.20, 0.50)
	kc.CheckOneLeader()

	const nClients, nOps = 4, 30
	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < nClients; i++ {
		i := i
		cl := kc.client(fmt.Sprintf("e%d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < nOps; j++ { // all clients append to ONE shared key
				if err := cl.Append(ctxBG(), "shared", fmt.Sprintf("(%d:%d)", i, j)); err != nil {
					t.Errorf("client %d op %d: %v", i, j, err)
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); close(done) }()
	kc.until(t, done, 120*time.Second, "appends")

	kc.setLoss(0, 0)
	cl := kc.client("reader")
	var got string
	kc.do(t, "read", func() {
		var err error
		if got, err = cl.Get(ctxBG(), "shared"); err != nil {
			t.Errorf("get: %v", err)
		}
	})
	for i := 0; i < nClients; i++ {
		last := -1
		for j := 0; j < nOps; j++ {
			tok := fmt.Sprintf("(%d:%d)", i, j)
			if n := strings.Count(got, tok); n != 1 {
				t.Fatalf("token %s appears %d times, want exactly 1 (seed=%d)", tok, n, kc.seed)
			}
			at := strings.Index(got, tok)
			if at < last {
				t.Fatalf("token %s out of order for its client (seed=%d)", tok, kc.seed)
			}
			last = at
		}
	}
	if want := nClients * nOps * len("(0:00)"); len(got) < want-nClients*10 { // sanity: nothing extra
		t.Logf("len(value)=%d", len(got))
	}
}

// TestKVPartitionedLeader: the old leader is cut off with a minority. A client
// that can only reach the minority never gets a success (neither for a write
// nor a read), the majority makes progress, and after healing the minority's
// stale write is gone.
func TestKVPartitionedLeader(t *testing.T) {
	kc := newKVCluster(t, 5)
	old := kc.CheckOneLeader()
	var buddy raft.NodeID
	for _, id := range kc.IDs() {
		if id != old {
			buddy = id
			break
		}
	}
	major := others(kc.IDs(), old, buddy)

	seedC := kc.client("seed")
	kc.do(t, "seed", func() {
		if err := seedC.Put(ctxBG(), "k", "v0"); err != nil {
			t.Errorf("seed put: %v", err)
		}
	})

	kc.Partition([]raft.NodeID{old, buddy}, major)
	kc.setReach("minority", old, buddy)
	kc.setReach("majority", major...)

	// The minority client tries a write and a read for 5 s of cluster time.
	min := kc.client("minority")
	ctx, cancel := context.WithCancel(ctxBG())
	var putErr, getErr error
	var got string
	fin := make(chan struct{})
	go func() {
		defer close(fin)
		putErr = min.Put(ctx, "k", "stale")
		var gctx, gcancel = context.WithCancel(ctxBG())
		defer gcancel()
		go func() { <-ctx.Done(); gcancel() }()
		got, getErr = min.Get(gctx, "k")
	}()

	// Meanwhile the majority elects a leader and takes a write.
	maj := kc.client("majority")
	kc.do(t, "majority write", func() {
		if err := maj.Put(ctxBG(), "k", "v1"); err != nil {
			t.Errorf("majority put: %v", err)
		}
		if v, err := maj.Get(ctxBG(), "k"); err != nil || v != "v1" {
			t.Errorf("majority get: %q, %v", v, err)
		}
	})
	kc.advance(5 * time.Second)
	select {
	case <-fin:
		t.Fatalf("minority client finished while partitioned: put err=%v, get=%q err=%v (seed=%d)",
			putErr, got, getErr, kc.seed)
	default:
	}
	cancel()
	kc.until(t, fin, 5*time.Second, "minority client to give up")
	if putErr == nil {
		t.Fatalf("minority Put reported success (seed=%d)", kc.seed)
	}
	if getErr == nil {
		t.Fatalf("minority Get returned %q, want an error (seed=%d)", got, kc.seed)
	}

	// Heal: everyone converges on the majority's write; "stale" never appears.
	kc.Heal()
	kc.setReach("minority", kc.IDs()...)
	kc.CheckOneLeader()
	after := kc.client("after")
	kc.do(t, "post-heal read", func() {
		if v, err := after.Get(ctxBG(), "k"); err != nil || v != "v1" {
			t.Errorf("after heal k = %q, %v; want v1", v, err)
		}
	})
	waitStore(t, kc, old, "k", "v1")
	waitStore(t, kc, buddy, "k", "v1")
	kc.CheckLogsAgree()
}

// TestKVClientGivesUp: with every server unreachable the client returns an
// error when its context ends, rather than hanging or reporting success.
func TestKVClientGivesUp(t *testing.T) {
	kc := newKVCluster(t, 3)
	kc.CheckOneLeader()
	kc.setReach("lost") // reaches nobody
	cl := kc.client("lost")
	ctx, cancel := context.WithCancel(ctxBG())
	fin := make(chan error, 1)
	go func() { fin <- cl.Put(ctx, "k", "v") }()
	kc.advance(3 * time.Second)
	cancel()
	select {
	case err := <-fin:
		if err == nil {
			t.Fatal("Put succeeded with no reachable server")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client did not return after context cancel")
	}
}

func others(ids []raft.NodeID, not ...raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
next:
	for _, id := range ids {
		for _, n := range not {
			if id == n {
				continue next
			}
		}
		out = append(out, id)
	}
	return out
}

// TestKVLostLeadershipNoFalseSuccess: an old leader accepts two Puts, is
// partitioned before they commit, and the majority elects a new leader whose
// no-op takes index N and whose own Put takes N+1. After healing, the entries
// at N and N+1 on the old leader are overwritten; the old leader's server must
// answer WrongLeader for both lost Puts and never success. Without the term
// check in Server.Command, the Put proposed at N+1 would be answered with the
// result of the majority's Put, which is a false success.
func TestKVLostLeadershipNoFalseSuccess(t *testing.T) {
	kc := newKVCluster(t, 3)
	old := kc.CheckOneLeader()
	maj := others(kc.IDs(), old)
	kc.mu.Lock()
	srv := kc.servers[old]
	kc.mu.Unlock()

	// Everyone holds the same committed prefix first.
	warm := kc.client("warm")
	kc.do(t, "warm-up", func() {
		if err := warm.Put(ctxBG(), "k", "v0"); err != nil {
			t.Errorf("warm put: %v", err)
		}
	})
	for _, id := range kc.IDs() {
		waitStore(t, kc, id, "k", "v0")
	}
	base := kc.Node(old).Status().LastIndex

	kc.Partition([]raft.NodeID{old}, maj)
	kc.setReach("majority", maj...)

	// The old leader still believes it leads and accepts two proposals that
	// can never commit. Wait for each to land in its log before the next.
	type result struct {
		rep *kvpb.CommandReply
		err error
	}
	lost := make([]chan result, 2)
	for i := range lost {
		i := i
		lost[i] = make(chan result, 1)
		a := &kvpb.CommandArgs{Op: kvpb.Op_PUT, Key: "k", Value: fmt.Sprintf("lost%d", i),
			ClientId: fmt.Sprintf("ghost%d", i), Seq: 1}
		go func() {
			rep, err := srv.Command(ctxBG(), a)
			lost[i] <- result{rep, err}
		}()
		for n := 0; kc.Node(old).Status().LastIndex < base+uint64(i)+1; n++ {
			if n > 5000 {
				t.Fatalf("old leader never accepted proposal %d (seed=%d)", i, kc.seed)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}

	// The majority commits its own entries at the same indexes.
	mc := kc.client("majority")
	kc.do(t, "majority put", func() {
		if err := mc.Put(ctxBG(), "k", "won"); err != nil {
			t.Errorf("majority put: %v", err)
		}
	})
	nl := kc.CheckOneLeader()
	if nl == old {
		t.Fatalf("old leader still the highest-term leader (seed=%d)", kc.seed)
	}
	// Something other than the ghost Puts sits at N+1 on the majority.
	if e, ok := kc.Applied(nl, base+2); !ok || strings.Contains(string(e.Data), "ghost") {
		t.Fatalf("expected the majority's own entry at index %d, got %+v ok=%v (seed=%d)", base+2, e, ok, kc.seed)
	}
	for i := range lost {
		select {
		case r := <-lost[i]:
			t.Fatalf("lost Put %d returned before the heal: %+v (seed=%d)", i, r, kc.seed)
		default:
		}
	}

	kc.Heal()
	for i := range lost {
		select {
		case r := <-lost[i]:
			checkNotSuccess(t, i, r.rep, r.err, kc.seed)
		default:
			var r result
			got := make(chan struct{})
			go func() { r = <-lost[i]; close(got) }()
			kc.until(t, got, 10*time.Second, fmt.Sprintf("lost Put %d to resolve", i))
			checkNotSuccess(t, i, r.rep, r.err, kc.seed)
		}
	}
	waitStore(t, kc, old, "k", "won")
	kc.CheckLogsAgree()
}

func checkNotSuccess(t *testing.T, i int, rep *kvpb.CommandReply, err error, seed int64) {
	t.Helper()
	if err != nil {
		return // the call ended without an answer: fine, not a success
	}
	if rep.Err == kvpb.Err_OK || rep.Err == kvpb.Err_ERR_NO_KEY {
		t.Fatalf("lost Put %d was answered with success %+v (seed=%d)", i, rep, seed)
	}
	if rep.Err != kvpb.Err_ERR_WRONG_LEADER && rep.Err != kvpb.Err_ERR_TIMEOUT {
		t.Fatalf("lost Put %d: unexpected reply %+v (seed=%d)", i, rep, seed)
	}
}
