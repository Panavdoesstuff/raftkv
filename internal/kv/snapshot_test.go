package kv_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
)

func entry(idx uint64, a *kvpb.CommandArgs) raft.Entry {
	return raft.Entry{Index: idx, Term: 1, Type: raft.EntryNormal, Data: kv.Encode(a)}
}

// TestKVSnapshotKeepsDedup: a store restored from a snapshot must still
// recognise a retry of a request applied before the snapshot. Without the
// dedup table in the snapshot the append is applied twice (DESIGN, Phase 5).
func TestKVSnapshotKeepsDedup(t *testing.T) {
	a := kv.NewStore()
	app := &kvpb.CommandArgs{Op: kvpb.Op_APPEND, Key: "k", Value: "A", ClientId: "c1", Seq: 1}
	a.Apply(entry(1, app))
	data, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	b := kv.NewStore()
	if err := b.Restore(data); err != nil {
		t.Fatal(err)
	}
	if v, _ := b.Peek("k"); v != "A" {
		t.Fatalf("restored value %q, want A", v)
	}
	b.Apply(entry(2, app)) // the retry, arriving after the "restart"
	if v, _ := b.Peek("k"); v != "A" {
		t.Fatalf("retry applied twice after restore: k=%q, want A", v)
	}
}

// stubRaft lets a Server be driven without a cluster.
type stubRaft struct{ index uint64 }

func (s *stubRaft) Propose([]byte) (uint64, uint64, bool) { return s.index, 1, true }
func (s *stubRaft) Status() raft.Status {
	return raft.Status{Role: raft.Leader, Term: 1}
}
func (s *stubRaft) LogSize() int            { return 0 }
func (s *stubRaft) Snapshot(uint64, []byte) {}

// TestKVRestoreFailsWaiters: a request waiting on an index that a snapshot
// install jumps over must be answered ErrWrongLeader, not left to hang.
func TestKVRestoreFailsWaiters(t *testing.T) {
	st := kv.NewStore()
	srv := kv.NewServer(&stubRaft{index: 5}, st, clock.NewFake()) // fake clock never fires
	res := make(chan *kvpb.CommandReply, 1)
	go func() {
		rep, _ := srv.Command(context.Background(), &kvpb.CommandArgs{Op: kvpb.Op_PUT, Key: "k", Value: "v", ClientId: "c", Seq: 1})
		res <- rep
	}()
	time.Sleep(50 * time.Millisecond) // let it register
	data, _ := kv.NewStore().Snapshot()
	if err := st.Restore(data); err != nil {
		t.Fatal(err)
	}
	select {
	case rep := <-res:
		if rep.Err != kvpb.Err_ERR_WRONG_LEADER {
			t.Fatalf("reply %v, want WRONG_LEADER", rep.Err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiter still hanging after Restore")
	}
}

// TestKVSnapshotBoundedLog: under continuous load the log of every node stays
// near the limit, and reads still see everything.
func TestKVSnapshotBoundedLog(t *testing.T) {
	const maxLog = 2048
	kc := newKVClusterSnap(t, 3, maxLog)
	kc.CheckOneLeader()
	cl := kc.client("c")
	worst := 0
	kc.do(t, "load", func() {
		for i := 0; i < 400; i++ {
			if err := cl.Append(ctxBG(), "k", fmt.Sprintf("<%d>", i)); err != nil {
				t.Errorf("append: %v", err)
				return
			}
		}
	})
	for _, id := range kc.IDs() {
		st := kc.Node(id).Status()
		if st.SnapIndex == 0 {
			t.Errorf("%s never snapshotted", id)
		}
		if n := kc.Node(id).(*kvNode).LogSize(); n > worst {
			worst = n
		}
	}
	if worst > 2*maxLog {
		t.Fatalf("log size %d, limit %d (seed=%d)", worst, maxLog, kc.seed)
	}
	kc.do(t, "get", func() {
		v, err := cl.Get(ctxBG(), "k")
		if err != nil || !strings.HasSuffix(v, "<399>") || strings.Count(v, "<") != 400 {
			t.Errorf("get: len=%d err=%v", len(v), err)
		}
	})
}

// TestKVSnapshotRestart: every node restarts from snapshot + log tail, and a
// retried append (same seq) from before the restart is not applied again.
func TestKVSnapshotRestart(t *testing.T) {
	kc := newKVClusterSnap(t, 3, 1024)
	kc.CheckOneLeader()
	cl := kc.client("c")
	kc.do(t, "load", func() {
		for i := 0; i < 200; i++ {
			if err := cl.Append(ctxBG(), "k", fmt.Sprintf("<%d>", i)); err != nil {
				t.Errorf("append: %v", err)
				return
			}
		}
	})
	for _, id := range kc.IDs() {
		kc.Crash(id)
	}
	for _, id := range kc.IDs() {
		kc.Restart(id)
	}
	kc.CheckOneLeader()
	for _, id := range kc.IDs() {
		if kc.Node(id).Status().SnapIndex == 0 {
			t.Fatalf("%s restarted without a snapshot", id)
		}
	}
	// Replay the last request of client "c" by hand: same clientID and seq.
	retry := &kvpb.CommandArgs{Op: kvpb.Op_APPEND, Key: "k", Value: "<199>", ClientId: "c", Seq: 200}
	leader := kc.CheckOneLeader()
	kc.mu.Lock()
	srv := kc.servers[leader]
	kc.mu.Unlock()
	kc.do(t, "retry", func() {
		if rep, err := srv.Command(ctxBG(), retry); err != nil || rep.Err != kvpb.Err_OK {
			t.Errorf("retry: %v %v", rep, err)
		}
	})
	kc.do(t, "get", func() {
		v, err := kc.client("r").Get(ctxBG(), "k")
		if err != nil {
			t.Errorf("get: %v", err)
			return
		}
		for i := 0; i < 200; i++ {
			if n := strings.Count(v, fmt.Sprintf("<%d>", i)); n != 1 {
				t.Errorf("token <%d> appears %d times after restart (seed=%d)", i, n, kc.seed)
				return
			}
		}
	})
}

// TestSnapshotChurn: 60 s of fake time with crashes, partitions, lossy
// links and snapshots every ~20 entries. Clients append unique tokens to their
// own keys and to one shared key; at the end every acknowledged token is there
// exactly once, on every replica.
func TestSnapshotChurn(t *testing.T) {
	kc := newKVClusterSnap(t, 5, 1500)
	kc.Net.SetDelay(0, 30*time.Millisecond)
	kc.Net.SetDropProb(0.03)
	kc.Net.SetDupProb(0.03)
	kc.setLoss(0.10, 0.10)
	kc.CheckOneLeader()

	const nClients = 8
	var stop atomic.Bool
	acked := make([]int, nClients)
	sharedAcked := make([]int, nClients)
	var wg sync.WaitGroup
	for i := 0; i < nClients; i++ {
		i := i
		cl := kc.client(fmt.Sprintf("c%d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; !stop.Load(); j++ {
				if err := cl.Append(ctxBG(), fmt.Sprintf("k%d", i), fmt.Sprintf("<%d.%d>", i, j)); err != nil {
					t.Errorf("client %d append: %v", i, err)
					return
				}
				acked[i] = j + 1
				if err := cl.Append(ctxBG(), "shared", fmt.Sprintf("(%d:%d)", i, j)); err != nil {
					t.Errorf("client %d shared append: %v", i, err)
					return
				}
				sharedAcked[i] = j + 1
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	rng := rand.New(rand.NewSource(kc.seed + 99))
	ids := kc.IDs()
	down := map[raft.NodeID]int{}
	partitionedUntil := 0
	for step := 0; step < 6000; step++ {
		for id, until := range down {
			if step >= until {
				kc.Restart(id)
				delete(down, id)
			}
		}
		if step%150 == 75 && len(down) < 2 {
			id := ids[rng.Intn(len(ids))]
			if _, already := down[id]; !already {
				kc.Crash(id)
				down[id] = step + 50 + rng.Intn(250)
			}
		}
		if partitionedUntil == 0 && step%400 == 200 {
			perm := rng.Perm(len(ids))
			var a, b []raft.NodeID
			for k, p := range perm {
				if k < 3 {
					a = append(a, ids[p])
				} else {
					b = append(b, ids[p])
				}
			}
			kc.Partition(a, b)
			partitionedUntil = step + 100 + rng.Intn(200)
		}
		if partitionedUntil != 0 && step >= partitionedUntil {
			kc.Heal()
			partitionedUntil = 0
		}
		kc.advance(10 * time.Millisecond)
	}

	// Quiesce: heal everything, then let the clients finish their last ops.
	stop.Store(true)
	kc.Heal()
	for id := range down {
		kc.Restart(id)
	}
	kc.Net.SetDelay(0, 0)
	kc.Net.SetDropProb(0)
	kc.Net.SetDupProb(0)
	kc.setLoss(0, 0)
	kc.until(t, done, 60*time.Second, "clients to finish")
	if t.Failed() {
		return
	}

	total := 0
	for i := 0; i < nClients; i++ {
		total += acked[i]
	}
	snapped := 0
	for _, id := range ids {
		if kc.Node(id).Status().SnapIndex > 0 {
			snapped++
		}
	}
	t.Logf("seed=%d acked=%d nodes-with-snapshot=%d", kc.seed, total, snapped)
	if snapped == 0 {
		t.Fatalf("no node ever snapshotted (seed=%d)", kc.seed)
	}

	kc.CheckOneLeader()
	for i := 0; i < nClients; i++ {
		var sb strings.Builder
		for j := 0; j < acked[i]; j++ {
			fmt.Fprintf(&sb, "<%d.%d>", i, j)
		}
		for _, id := range ids {
			waitStore(t, kc, id, fmt.Sprintf("k%d", i), sb.String())
		}
	}
	// Shared key: every token exactly once, in order per client.
	reader := kc.client("reader")
	var got string
	kc.do(t, "read shared", func() {
		var err error
		if got, err = reader.Get(ctxBG(), "shared"); err != nil {
			t.Errorf("get: %v", err)
		}
	})
	for i := 0; i < nClients; i++ {
		last := -1
		for j := 0; j < sharedAcked[i]; j++ {
			tok := fmt.Sprintf("(%d:%d)", i, j)
			if n := strings.Count(got, tok); n != 1 {
				t.Fatalf("token %s appears %d times, want 1 (seed=%d)", tok, n, kc.seed)
			}
			if at := strings.Index(got, tok); at < last {
				t.Fatalf("token %s out of order (seed=%d)", tok, kc.seed)
			} else {
				last = at
			}
		}
	}
	kc.CheckLogsAgree()
}
