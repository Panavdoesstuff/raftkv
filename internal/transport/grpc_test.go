package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/raft"
)

type echoPeer struct{}

func (echoPeer) RequestVote(_ context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return &raft.RequestVoteReply{Term: a.Term + 1, VoteGranted: a.PreVote}, nil
}
func (echoPeer) AppendEntries(_ context.Context, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return &raft.AppendEntriesReply{Term: a.Term, Success: len(a.Entries) == 2 && a.Entries[1].Type == raft.EntryConfig, ConflictIndex: a.PrevLogIndex}, nil
}
func (echoPeer) InstallSnapshot(_ context.Context, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{Term: uint64(len(a.Data) + len(a.Config))}, nil
}

func TestGRPCRoundTripAndAddrs(t *testing.T) {
	srv := NewGRPC("b", "127.0.0.1:0", nil)
	if err := srv.Serve("b", echoPeer{}); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	cli := NewGRPC("a", "127.0.0.1:0", nil)
	defer cli.Close()
	var _ raft.AddrAware = cli

	ctx := context.Background()
	if _, err := cli.Peer("b").RequestVote(ctx, &raft.RequestVoteArgs{Term: 1}); !errors.Is(err, ErrNoAddr) {
		t.Fatalf("before SetAddrs: %v, want ErrNoAddr", err)
	}
	cli.SetAddrs(map[raft.NodeID]string{"b": srv.Addr()})
	vr, err := cli.Peer("b").RequestVote(ctx, &raft.RequestVoteArgs{Term: 4, PreVote: true})
	if err != nil || vr.Term != 5 || !vr.VoteGranted {
		t.Fatalf("RequestVote: %+v %v", vr, err)
	}
	ar, err := cli.Peer("b").AppendEntries(ctx, &raft.AppendEntriesArgs{Term: 2, PrevLogIndex: 9,
		Entries: []raft.Entry{{Index: 10}, {Index: 11, Type: raft.EntryConfig, Data: []byte("x")}}})
	if err != nil || !ar.Success || ar.ConflictIndex != 9 {
		t.Fatalf("AppendEntries: %+v %v", ar, err)
	}
	sr, err := cli.Peer("b").InstallSnapshot(ctx, &raft.InstallSnapshotArgs{Data: make([]byte, 3<<20), Config: []byte("cc")})
	if err != nil || sr.Term != 3<<20+2 {
		t.Fatalf("InstallSnapshot: %+v %v", sr, err)
	}

	// A dead peer fails within the RPC deadline, and the same Peer works again
	// once the node is back on that address (lazy reconnect).
	addr := srv.Addr()
	srv.Close()
	start := time.Now()
	if _, err := cli.Peer("b").RequestVote(ctx, &raft.RequestVoteArgs{Term: 1}); err == nil {
		t.Fatal("RPC to a closed server succeeded")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("failed RPC took %v", d)
	}
	srv2 := NewGRPC("b", addr, nil)
	if err := srv2.Serve("b", echoPeer{}); err != nil {
		t.Skipf("could not rebind %s: %v", addr, err)
	}
	defer srv2.Close()
	var lastErr error
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if _, lastErr = cli.Peer("b").RequestVote(ctx, &raft.RequestVoteArgs{Term: 1}); lastErr == nil {
			return
		}
	}
	t.Fatalf("no reconnect after restart: %v", lastErr)
}
