package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/raftpb"
)

// Per-RPC deadlines. Votes and AppendEntries must fail fast: a heartbeat that
// takes longer than an election timeout is as good as lost, and Raft retries
// on its own schedule. InstallSnapshot ships a whole state image.
const (
	DefaultRPCTimeout      = 500 * time.Millisecond
	DefaultSnapshotTimeout = 10 * time.Second
	maxMsgSize             = 256 << 20
)

var ErrNoAddr = errors.New("grpc transport: no address for node")

// GRPC is the real-network Transport. It owns this node's Raft gRPC server and
// one lazily dialed client connection per peer. grpc.NewClient does not
// connect until the first RPC and reconnects with backoff by itself, so a peer
// that is down, restarting or not yet added costs nothing until it is used.
// Peers are found through an id -> address map that starts from the static
// --peers flag and is extended by SetAddrs from membership configs.
type GRPC struct {
	self        raft.NodeID
	listenAddr  string
	rpcTimeout  time.Duration
	snapTimeout time.Duration

	mu    sync.Mutex
	addrs map[raft.NodeID]string
	conns map[raft.NodeID]*grpc.ClientConn
	srv   *grpc.Server
	lis   net.Listener
}

// NewGRPC returns the transport for node self, which will listen on listenAddr
// once Serve is called. peers seeds the address map (self may be included).
func NewGRPC(self raft.NodeID, listenAddr string, peers map[raft.NodeID]string) *GRPC {
	t := &GRPC{
		self: self, listenAddr: listenAddr,
		rpcTimeout: DefaultRPCTimeout, snapTimeout: DefaultSnapshotTimeout,
		addrs: map[raft.NodeID]string{}, conns: map[raft.NodeID]*grpc.ClientConn{},
	}
	for id, a := range peers {
		t.addrs[id] = a
	}
	return t
}

// SetTimeouts overrides the per-RPC deadlines (call before Serve).
func (t *GRPC) SetTimeouts(rpc, snapshot time.Duration) {
	t.rpcTimeout, t.snapTimeout = rpc, snapshot
}

// SetAddrs adds or updates addresses (raft.AddrAware). A changed address drops
// the old connection; the next RPC dials the new one.
func (t *GRPC) SetAddrs(addrs map[raft.NodeID]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, a := range addrs {
		if t.addrs[id] == a {
			continue
		}
		t.addrs[id] = a
		if c := t.conns[id]; c != nil {
			delete(t.conns, id)
			go c.Close()
		}
	}
}

// Addr is the address the server listens on (useful with port 0). Valid after Serve.
func (t *GRPC) Addr() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lis == nil {
		return t.listenAddr
	}
	return t.lis.Addr().String()
}

// Serve registers h and starts the gRPC server in the background.
func (t *GRPC) Serve(id raft.NodeID, h raft.Peer) error {
	lis, err := net.Listen("tcp", t.listenAddr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(maxMsgSize), grpc.MaxSendMsgSize(maxMsgSize))
	raftpb.RegisterRaftServer(srv, &grpcHandler{h: h})
	t.mu.Lock()
	t.srv, t.lis = srv, lis
	t.mu.Unlock()
	go srv.Serve(lis)
	return nil
}

// Close stops the server and closes every connection.
func (t *GRPC) Close() {
	t.mu.Lock()
	srv := t.srv
	conns := t.conns
	t.srv, t.conns = nil, map[raft.NodeID]*grpc.ClientConn{}
	t.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
	for _, c := range conns {
		c.Close()
	}
}

func (t *GRPC) Peer(id raft.NodeID) raft.Peer { return &grpcPeer{t: t, id: id} }

func (t *GRPC) client(id raft.NodeID) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c := t.conns[id]; c != nil {
		return raftpb.NewRaftClient(c), nil
	}
	addr, ok := t.addrs[id]
	if !ok || addr == "" {
		return nil, fmt.Errorf("%w %q", ErrNoAddr, id)
	}
	c, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 500 * time.Millisecond},
			MinConnectTimeout: time.Second,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)),
	)
	if err != nil {
		return nil, err
	}
	t.conns[id] = c
	return raftpb.NewRaftClient(c), nil
}

type grpcPeer struct {
	t  *GRPC
	id raft.NodeID
}

func (p *grpcPeer) RequestVote(ctx context.Context, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := p.t.client(p.id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.t.rpcTimeout)
	defer cancel()
	r, err := c.RequestVote(ctx, &raftpb.RequestVoteArgs{
		Term: a.Term, CandidateId: string(a.CandidateID),
		LastLogIndex: a.LastLogIndex, LastLogTerm: a.LastLogTerm, PreVote: a.PreVote,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

func (p *grpcPeer) AppendEntries(ctx context.Context, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := p.t.client(p.id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.t.rpcTimeout)
	defer cancel()
	es := make([]*raftpb.Entry, len(a.Entries))
	for i, e := range a.Entries {
		es[i] = &raftpb.Entry{Index: e.Index, Term: e.Term, Type: raftpb.EntryType(e.Type), Data: e.Data}
	}
	r, err := c.AppendEntries(ctx, &raftpb.AppendEntriesArgs{
		Term: a.Term, LeaderId: string(a.LeaderID), PrevLogIndex: a.PrevLogIndex,
		PrevLogTerm: a.PrevLogTerm, Entries: es, LeaderCommit: a.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{Term: r.Term, Success: r.Success, ConflictTerm: r.ConflictTerm, ConflictIndex: r.ConflictIndex}, nil
}

func (p *grpcPeer) InstallSnapshot(ctx context.Context, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	c, err := p.t.client(p.id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.t.snapTimeout)
	defer cancel()
	r, err := c.InstallSnapshot(ctx, &raftpb.InstallSnapshotArgs{
		Term: a.Term, LeaderId: string(a.LeaderID), LastIncludedIndex: a.LastIncludedIndex,
		LastIncludedTerm: a.LastIncludedTerm, Data: a.Data, Config: a.Config,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotReply{Term: r.Term}, nil
}

// grpcHandler adapts a raft.Peer to the generated server interface.
type grpcHandler struct {
	raftpb.UnimplementedRaftServer
	h raft.Peer
}

func (g *grpcHandler) RequestVote(ctx context.Context, a *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	r, err := g.h.RequestVote(ctx, &raft.RequestVoteArgs{
		Term: a.Term, CandidateID: raft.NodeID(a.CandidateId),
		LastLogIndex: a.LastLogIndex, LastLogTerm: a.LastLogTerm, PreVote: a.PreVote,
	})
	if err != nil {
		return nil, err
	}
	return &raftpb.RequestVoteReply{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

func (g *grpcHandler) AppendEntries(ctx context.Context, a *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	es := make([]raft.Entry, len(a.Entries))
	for i, e := range a.Entries {
		es[i] = raft.Entry{Index: e.Index, Term: e.Term, Type: raft.EntryType(e.Type), Data: e.Data}
	}
	r, err := g.h.AppendEntries(ctx, &raft.AppendEntriesArgs{
		Term: a.Term, LeaderID: raft.NodeID(a.LeaderId), PrevLogIndex: a.PrevLogIndex,
		PrevLogTerm: a.PrevLogTerm, Entries: es, LeaderCommit: a.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &raftpb.AppendEntriesReply{Term: r.Term, Success: r.Success, ConflictTerm: r.ConflictTerm, ConflictIndex: r.ConflictIndex}, nil
}

func (g *grpcHandler) InstallSnapshot(ctx context.Context, a *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error) {
	r, err := g.h.InstallSnapshot(ctx, &raft.InstallSnapshotArgs{
		Term: a.Term, LeaderID: raft.NodeID(a.LeaderId), LastIncludedIndex: a.LastIncludedIndex,
		LastIncludedTerm: a.LastIncludedTerm, Data: a.Data, Config: a.Config,
	})
	if err != nil {
		return nil, err
	}
	return &raftpb.InstallSnapshotReply{Term: r.Term}, nil
}
