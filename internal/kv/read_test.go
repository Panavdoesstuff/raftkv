package kv_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
	"github.com/panavdoesstuff/raftkv/test"
)

// skewCluster is a KV cluster whose nodes each run on a SkewedClock over the
// shared FakeClock, so a test can slow one node's clock at an exact moment.
type skewCluster struct {
	*kvCluster
	mu  sync.Mutex
	clk map[raft.NodeID]*clock.SkewedClock
}

func newSkewCluster(t *testing.T, n int, mode kv.ReadMode) *skewCluster {
	t.Helper()
	sc := &skewCluster{clk: map[raft.NodeID]*clock.SkewedClock{}}
	nodeClock := func(id raft.NodeID, base clock.Clock) clock.Clock {
		c := clock.NewSkewed(base, 0, 1)
		sc.mu.Lock()
		sc.clk[id] = c
		sc.mu.Unlock()
		return c
	}
	sc.kvCluster = newKVClusterWith(t, n, kv.DefaultMaxLogBytes,
		[]kv.ServerOption{kv.WithReadMode(mode)},
		[]test.Option{test.WithNodeClock(nodeClock)})
	return sc
}

func (sc *skewCluster) skew(id raft.NodeID) *clock.SkewedClock {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.clk[id]
}

// directGet sends a Get straight to one server (no client retries or hints),
// on its own goroutine so the caller can drive fake time.
func (kc *kvCluster) directGet(ctx context.Context, id raft.NodeID, key string) <-chan getRes {
	ch := make(chan getRes, 1)
	go func() {
		rep, err := kc.servers[id].Command(ctx, &kvpb.CommandArgs{Op: kvpb.Op_GET, Key: key, ClientId: "direct-" + string(id), Seq: 1})
		ch <- getRes{rep, err}
	}()
	return ch
}

type getRes struct {
	rep *kvpb.CommandReply
	err error
}

// TestKVReadModes: in each fast-read mode a Get right after a Put always sees
// it, including right after a leader change.
func TestKVReadModes(t *testing.T) {
	for _, mode := range []kv.ReadMode{kv.ReadIndex, kv.ReadLease} {
		t.Run(mode.String(), func(t *testing.T) {
			kc := newKVClusterWith(t, 3, kv.DefaultMaxLogBytes, []kv.ServerOption{kv.WithReadMode(mode)}, nil)
			old := kc.CheckOneLeader()
			c := kc.client("c")
			kc.do(t, "ops", func() {
				for i := 0; i < 10; i++ {
					v := string(rune('a' + i))
					if err := c.Put(ctxBG(), "k", v); err != nil {
						t.Errorf("put: %v", err)
						return
					}
					if got, err := c.Get(ctxBG(), "k"); err != nil || got != v {
						t.Errorf("get after put %q = %q, %v (seed=%d)", v, got, err, kc.seed)
						return
					}
				}
			})
			// New leader: reads must still reflect the last write.
			kc.Crash(old)
			kc.CheckOneLeader()
			kc.do(t, "after failover", func() {
				if got, err := c.Get(ctxBG(), "k"); err != nil || got != "j" {
					t.Errorf("get after failover = %q, %v; want j (seed=%d)", got, err, kc.seed)
				}
			})
		})
	}
}

// staleScenario partitions the leader of a 3-node cluster, slows its clock to a
// crawl at that instant, lets the majority elect a new leader and overwrite
// k, and then asks the OLD leader for k directly. It returns the old leader's
// answer channel (after advancing the stated cluster time).
func staleScenario(t *testing.T, sc *skewCluster, rate float64) (old raft.NodeID, ans <-chan getRes, cancel context.CancelFunc) {
	t.Helper()
	old = sc.CheckOneLeader()
	rest := others(sc.IDs(), old)
	seed := sc.client("seed")
	sc.do(t, "seed write", func() {
		if err := seed.Put(ctxBG(), "k", "v0"); err != nil {
			t.Errorf("seed put: %v", err)
		}
	})
	sc.setReach("majority", rest...)
	// The clock slows and the network splits at the same instant: from now on
	// the old leader can neither hear anyone nor tell time properly.
	sc.skew(old).SetRate(rate)
	sc.Partition([]raft.NodeID{old}, rest)
	maj := sc.client("majority")
	sc.do(t, "majority write", func() {
		if err := maj.Put(ctxBG(), "k", "v1"); err != nil {
			t.Errorf("majority put: %v", err)
		}
	})
	if got := sc.skew(old).Now().Sub(sc.Clock.Now()); got > time.Hour {
		t.Fatalf("clock arithmetic broke: %v", got)
	}
	ctx, cancel := context.WithCancel(ctxBG())
	return old, sc.directGet(ctx, old, "k"), cancel
}

// TestLeaseReadSkew shows why leases need bounded clock drift. The old leader's
// clock runs at 5% speed, so after it is partitioned it believes its lease
// (150 ms - margin from its last majority ack) is still valid long after the
// followers' real election timeouts have passed. A new leader is elected and
// overwrites k, yet the old leader serves the old value from its lease.
// Everything is on FakeClock, so this is deterministic: the stale read is the
// expected, asserted outcome.
func TestLeaseReadSkew(t *testing.T) {
	sc := newSkewCluster(t, 3, kv.ReadLease)
	old, ans, cancel := staleScenario(t, sc, 0.05)
	defer cancel()
	{
		r, ok := <-waitRes(sc, ans, 2*time.Second)
		if !ok {
			t.Fatalf("old leader never answered the lease read (seed=%d)", sc.seed)
		}
		if r.err != nil || r.rep.Err != kvpb.Err_OK {
			t.Fatalf("lease read on the old leader failed (%+v, %v); the skew test needs it to answer (seed=%d)", r.rep, r.err, sc.seed)
		}
		if r.rep.Value != "v0" {
			t.Fatalf("old leader %s returned %q, want the stale value v0 (seed=%d)", old, r.rep.Value, sc.seed)
		}
		// And the cluster really has moved on.
		if st := sc.Node(old).Status(); st.Role != raft.Leader {
			t.Fatalf("old leader stepped down; the read was not stale-while-leading (seed=%d)", sc.seed)
		}
		t.Logf("STALE READ: old leader %s answered k=%q after the majority wrote v1 (seed=%d)", old, r.rep.Value, sc.seed)
	}
}

// waitRes drives cluster time until ans delivers or d of cluster time passes
// (then it delivers nothing). The result comes on the returned channel.
func waitRes(kc *skewCluster, ans <-chan getRes, d time.Duration) <-chan getRes {
	out := make(chan getRes, 1)
	go func() {
		defer close(out)
		for waited := time.Duration(0); waited < d; waited += 10 * time.Millisecond {
			select {
			case r := <-ans:
				out <- r
				return
			default:
			}
			kc.Cluster.Advance(10 * time.Millisecond)
		}
	}()
	return out
}

// TestReadIndexRefusesUnderSkew is the control for TestLeaseReadSkew: same
// partition, same crawling clock, but ReadIndex needs a majority round, so the
// old leader cannot answer at all.
func TestReadIndexRefusesUnderSkew(t *testing.T) {
	sc := newSkewCluster(t, 3, kv.ReadIndex)
	old, ans, cancel := staleScenario(t, sc, 0.05)
	defer cancel()
	for r := range waitRes(sc, ans, 3*time.Second) {
		if r.err == nil && r.rep.Err == kvpb.Err_OK {
			t.Fatalf("old leader %s answered a ReadIndex Get %q while partitioned (seed=%d)", old, r.rep.Value, sc.seed)
		}
	}
	// Heal: it learns of the new term and the read fails over, never stale.
	sc.Heal()
	sc.setReach("majority", sc.IDs()...)
	r, ok := <-waitRes(sc, ans, 5*time.Second)
	if !ok {
		t.Fatalf("old leader's read never ended after the heal (seed=%d)", sc.seed)
	}
	if r.err == nil && r.rep.Err == kvpb.Err_OK {
		t.Fatalf("old leader answered %q after losing leadership (seed=%d)", r.rep.Value, sc.seed)
	}
}

// TestLeaseReadSafeWithoutSkew is the other control: with an honest clock the
// same partition lets the lease run out (150 ms - margin after the last ack),
// and the old leader falls back to ReadIndex, which cannot finish.
func TestLeaseReadSafeWithoutSkew(t *testing.T) {
	sc := newSkewCluster(t, 3, kv.ReadLease)
	old, ans, cancel := staleScenario(t, sc, 1)
	defer cancel()
	for r := range waitRes(sc, ans, 3*time.Second) {
		if r.err == nil && r.rep.Err == kvpb.Err_OK { // a TIMEOUT or WRONG_LEADER is a refusal, which is fine
			t.Fatalf("old leader %s answered %q with an honest clock and an expired lease (seed=%d)", old, r.rep.Value, sc.seed)
		}
	}
}
