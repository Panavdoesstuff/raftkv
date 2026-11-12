package transport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
)

// echo records calls and replies with the term it was sent.
type echo struct {
	votes   atomic.Int32
	appends atomic.Int32
	lastLen atomic.Int32
}

func (e *echo) RequestVote(_ context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	e.votes.Add(1)
	return &raft.RequestVoteReply{Term: a.Term, VoteGranted: true}, nil
}
func (e *echo) AppendEntries(_ context.Context, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	e.appends.Add(1)
	e.lastLen.Store(int32(len(a.Entries)))
	if len(a.Entries) > 0 {
		a.Entries[0].Data[0] = 0xFF // must not leak back to the sender
	}
	return &raft.AppendEntriesReply{Term: a.Term, Success: true}, nil
}
func (e *echo) InstallSnapshot(_ context.Context, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{Term: a.Term}, nil
}

func setup(t *testing.T, seed int64) (*Memnet, raft.Peer, *echo) {
	t.Helper()
	n := NewMemnet(seed, clock.RealClock{})
	h := &echo{}
	n.Transport("b").Serve("b", h)
	return n, n.Transport("a").Peer("b"), h
}

func vote(p raft.Peer) error {
	_, err := p.RequestVote(context.Background(), &raft.RequestVoteArgs{Term: 4})
	return err
}

func TestMemnetDelivers(t *testing.T) {
	_, p, h := setup(t, 1)
	r, err := p.RequestVote(context.Background(), &raft.RequestVoteArgs{Term: 4})
	if err != nil || r.Term != 4 || !r.VoteGranted || h.votes.Load() != 1 {
		t.Fatalf("r=%+v err=%v", r, err)
	}
}

func TestMemnetArgsAreCopied(t *testing.T) {
	_, p, _ := setup(t, 1)
	args := &raft.AppendEntriesArgs{Entries: []raft.Entry{{Index: 1, Data: []byte{1}}}}
	if _, err := p.AppendEntries(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if args.Entries[0].Data[0] != 1 {
		t.Fatal("handler mutated sender's entry")
	}
}

func TestMemnetPartitionAndHeal(t *testing.T) {
	n, p, h := setup(t, 1)
	n.Partition([]raft.NodeID{"a"}, []raft.NodeID{"b"})
	if err := vote(p); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("want unreachable, got %v", err)
	}
	if h.votes.Load() != 0 {
		t.Fatal("handler reached across partition")
	}
	n.Heal()
	if err := vote(p); err != nil {
		t.Fatal(err)
	}
}

func TestMemnetOneWayBlock(t *testing.T) {
	n, p, _ := setup(t, 1)
	n.Block("b", "a", true) // replies can't return, requests can
	if err := vote(p); err == nil {
		t.Fatal("reply should be lost")
	}
}

func TestMemnetNodeDisconnect(t *testing.T) {
	n, p, _ := setup(t, 1)
	n.SetNodeConnected("b", false)
	if err := vote(p); err == nil {
		t.Fatal("expected error")
	}
	n.SetNodeConnected("b", true)
	if err := vote(p); err != nil {
		t.Fatal(err)
	}
}

func TestMemnetDropsAreSeedDeterministic(t *testing.T) {
	run := func(seed int64) []bool {
		n, p, _ := setup(t, seed)
		n.SetDropProb(0.5)
		var out []bool
		for i := 0; i < 40; i++ {
			out = append(out, vote(p) == nil)
		}
		return out
	}
	a, b := run(42), run(42)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("seed 42 diverged at call %d", i)
		}
	}
	var ok int
	for _, v := range a {
		if v {
			ok++
		}
	}
	if ok == 0 || ok == len(a) {
		t.Fatalf("drop prob 0.5 gave %d/%d successes", ok, len(a))
	}
}

func TestMemnetDuplicate(t *testing.T) {
	n, p, h := setup(t, 1)
	n.SetDupProb(1)
	if err := vote(p); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for h.votes.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.votes.Load() != 2 {
		t.Fatalf("handler saw %d deliveries, want 2", h.votes.Load())
	}
}

func TestMemnetDelayUsesClock(t *testing.T) {
	fc := clock.NewFake()
	n := NewMemnet(1, fc)
	n.Transport("b").Serve("b", &echo{})
	n.SetDelay(100*time.Millisecond, 100*time.Millisecond)
	p := n.Transport("a").Peer("b")
	done := make(chan error, 1)
	go func() { done <- vote(p) }()
	fc.BlockUntil(1)
	select {
	case <-done:
		t.Fatal("returned before delay elapsed")
	case <-time.After(20 * time.Millisecond):
	}
	fc.Advance(100 * time.Millisecond) // request leg
	fc.BlockUntil(1)
	fc.Advance(100 * time.Millisecond) // reply leg
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestMemnetContextCancel(t *testing.T) {
	fc := clock.NewFake()
	n := NewMemnet(1, fc)
	n.Transport("b").Serve("b", &echo{})
	n.SetDelay(time.Hour, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := n.Transport("a").Peer("b").RequestVote(ctx, &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("expected deadline error")
	}
}
