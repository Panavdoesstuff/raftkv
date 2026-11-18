package raft_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/test"
)

type readRes struct {
	idx uint64
	err error
}

func asRaft(c *test.Cluster, id raft.NodeID) *raft.Raft { return c.Node(id).(*raft.Raft) }

// readAsync starts r.ReadIndex on its own goroutine.
func readAsync(ctx context.Context, r *raft.Raft) <-chan readRes {
	ch := make(chan readRes, 1)
	go func() {
		idx, err := r.ReadIndex(ctx)
		ch <- readRes{idx, err}
	}()
	return ch
}

// await advances cluster (fake) time until the read finishes.
func await(t *testing.T, c *test.Cluster, ch <-chan readRes, what string) readRes {
	t.Helper()
	for i := 0; i < 500; i++ {
		select {
		case r := <-ch:
			return r
		default:
		}
		advance(c, 10*time.Millisecond)
	}
	t.Fatalf("%s: read did not finish in 5 s of cluster time (seed=%d)", what, c.Seed)
	return readRes{}
}

// settle waits (real time, no fake-time advance) until cond holds. It is for
// goroutine hand-off only, never for protocol timing.
func settle(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 100000; i++ {
		if cond() {
			return
		}
		runtime.Gosched()
		time.Sleep(100 * time.Microsecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A healthy leader serves a read that reflects everything committed before it,
// after lastApplied has caught up; followers refuse.
func TestReadIndexBasic(t *testing.T) {
	c := newCluster(t, 3)
	l := c.CheckOneLeader()
	var last uint64
	for i := 0; i < 5; i++ {
		last = c.One(cmd("w%d", i), 3, true)
	}
	r := asRaft(c, l)
	res := await(t, c, readAsync(context.Background(), r), "leader read")
	if res.err != nil {
		t.Fatalf("ReadIndex on leader: %v (seed=%d)", res.err, c.Seed)
	}
	if res.idx < last {
		t.Fatalf("readIndex %d is behind committed write %d (seed=%d)", res.idx, last, c.Seed)
	}
	if st := r.Status(); st.LastApplied < res.idx {
		t.Fatalf("ReadIndex returned %d but lastApplied is %d (seed=%d)", res.idx, st.LastApplied, c.Seed)
	}
	for _, id := range others(c, l) {
		res := await(t, c, readAsync(context.Background(), asRaft(c, id)), "follower read")
		if !errors.Is(res.err, raft.ErrNotLeader) {
			t.Fatalf("follower %s ReadIndex = %+v, want ErrNotLeader (seed=%d)", id, res, c.Seed)
		}
	}
	// The read works repeatedly and the index never goes backwards.
	prev := res.idx
	for i := 0; i < 5; i++ {
		res := await(t, c, readAsync(context.Background(), r), "repeat read")
		if res.err != nil || res.idx < prev {
			t.Fatalf("repeat read %d: %+v after %d (seed=%d)", i, res, prev, c.Seed)
		}
		prev = res.idx
	}
}

// A leader that has not committed an entry of its own term cannot know the
// true commit point (thesis 6.4 step 1): ReadIndex must refuse.
func TestReadIndexNeedsCurrentTermCommit(t *testing.T) {
	l := newLone(t)
	l.appendFrom(t, 1, 0, 0, ents(1, 3, 1), 3) // committed, but all from term 1
	// Bump to term 2 and take office without a no-op, as if just elected.
	l.appendFrom(t, 2, 3, 1, nil, 3)
	l.r.ForceLeader()
	if st := l.r.Status(); st.Role != raft.Leader || st.Term != 2 || st.CommitIndex != 3 {
		t.Fatalf("setup: %+v", st)
	}
	_, err := l.r.ReadIndex(context.Background())
	if !errors.Is(err, raft.ErrNotReady) {
		t.Fatalf("ReadIndex before a current-term commit = %v, want ErrNotReady", err)
	}
	if _, err := l.r.LeaseRead(context.Background()); !errors.Is(err, raft.ErrNotReady) {
		t.Fatalf("LeaseRead before a current-term commit = %v, want ErrNotReady", err)
	}
}

// Concurrent reads share one heartbeat round. With both followers cut off the
// first read starts a round; the other 19 queue behind it without sending
// another. Once the network is back, the next round answers all 20.
func TestReadIndexBatching(t *testing.T) {
	c := newCluster(t, 3)
	l := c.CheckOneLeader()
	c.One(cmd("warm"), 3, true)
	r := asRaft(c, l)
	for _, id := range others(c, l) {
		c.Disconnect(id)
	}
	before := r.Status().Rounds

	const n = 20
	chans := make([]<-chan readRes, n)
	for i := range chans {
		chans[i] = readAsync(context.Background(), r)
	}
	settle(t, "20 reads to queue", func() bool { return r.Status().PendingReads == n })
	if got := r.Status().Rounds - before; got != 1 {
		t.Fatalf("%d concurrent reads used %d heartbeat rounds, want 1 (seed=%d)", n, got, c.Seed)
	}
	for i, ch := range chans {
		select {
		case res := <-ch:
			t.Fatalf("read %d finished with no majority reachable: %+v (seed=%d)", i, res, c.Seed)
		default:
		}
	}

	for _, id := range others(c, l) {
		c.Connect(id)
	}
	for i, ch := range chans {
		if res := await(t, c, ch, "batched read"); res.err != nil {
			t.Fatalf("read %d: %v (seed=%d)", i, res.err, c.Seed)
		}
	}
	// The queued reads rode one more round (maybe two with a periodic
	// heartbeat), not one per read.
	if got := r.Status().Rounds - before; got > 3 {
		t.Fatalf("%d reads used %d rounds in total (seed=%d)", n, got, c.Seed)
	}
}

// An isolated old leader still thinks it leads, but it cannot get a majority
// to confirm it, so it must never serve a read. Once it hears of the new term
// the pending read fails; it never succeeds.
func TestReadIndexPartitionedLeader(t *testing.T) {
	c := newCluster(t, 3)
	old := c.CheckOneLeader()
	c.One(cmd("before"), 3, true)
	rest := others(c, old)
	c.Partition([]raft.NodeID{old}, rest)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pending := readAsync(ctx, asRaft(c, old))

	// 3 s of cluster time: the majority elects a new leader; the old one is
	// still in office on its side.
	for i := 0; i < 300; i++ {
		advance(c, 10*time.Millisecond)
		select {
		case res := <-pending:
			t.Fatalf("isolated old leader served a read: %+v (seed=%d)", res, c.Seed)
		default:
		}
	}
	if st := status(c, old); st.Role != raft.Leader {
		t.Fatalf("old leader stepped down by itself (role %v); test needs it to still think it leads (seed=%d)", st.Role, c.Seed)
	}
	var nl raft.NodeID
	for _, id := range rest {
		if status(c, id).Role == raft.Leader {
			nl = id
		}
	}
	if nl == "" {
		t.Fatalf("majority side elected no leader (seed=%d)", c.Seed)
	}
	if res := await(t, c, readAsync(context.Background(), asRaft(c, nl)), "new leader read"); res.err != nil {
		t.Fatalf("new leader read: %v (seed=%d)", res.err, c.Seed)
	}

	c.Heal()
	res := await(t, c, pending, "old leader read after heal")
	if res.err == nil {
		t.Fatalf("old leader's read succeeded across a leadership change: %+v (seed=%d)", res, c.Seed)
	}
	if !errors.Is(res.err, raft.ErrNotLeader) {
		t.Fatalf("old leader's read failed with %v, want ErrNotLeader (seed=%d)", res.err, c.Seed)
	}
}

// A cancelled context ends a waiting read and leaves nothing queued.
func TestReadIndexContextCancel(t *testing.T) {
	c := newCluster(t, 3)
	l := c.CheckOneLeader()
	c.One(cmd("warm"), 3, true)
	r := asRaft(c, l)
	for _, id := range others(c, l) {
		c.Disconnect(id)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := readAsync(ctx, r)
	settle(t, "read to queue", func() bool { return r.Status().PendingReads == 1 })
	cancel()
	res := await(t, c, ch, "cancelled read")
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("cancelled read = %+v, want context.Canceled (seed=%d)", res, c.Seed)
	}
	if n := r.Status().PendingReads; n != 0 {
		t.Fatalf("%d reads still queued after cancel (seed=%d)", n, c.Seed)
	}
}
