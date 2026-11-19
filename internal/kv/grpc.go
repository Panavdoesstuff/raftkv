package kv

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
)

// GRPCService exposes a Server as the KV gRPC service (the client port).
type GRPCService struct {
	kvpb.UnimplementedKVServer
	S *Server
}

func (g GRPCService) Command(ctx context.Context, a *kvpb.CommandArgs) (*kvpb.CommandReply, error) {
	return g.S.Command(ctx, a)
}
func (g GRPCService) AddServer(ctx context.Context, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	return g.S.AddServer(ctx, a)
}
func (g GRPCService) RemoveServer(ctx context.Context, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	return g.S.RemoveServer(ctx, a)
}

// GRPCCaller implements Caller and MembershipCaller over gRPC. Servers are
// found by node ID in an address map; connections are dialed lazily and
// reconnect by themselves.
type GRPCCaller struct {
	mu    sync.Mutex
	addrs map[raft.NodeID]string
	conns map[raft.NodeID]*grpc.ClientConn
}

func NewGRPCCaller(addrs map[raft.NodeID]string) *GRPCCaller {
	m := make(map[raft.NodeID]string, len(addrs))
	for k, v := range addrs {
		m[k] = v
	}
	return &GRPCCaller{addrs: m, conns: map[raft.NodeID]*grpc.ClientConn{}}
}

// SetAddr adds or updates a server's client address.
func (c *GRPCCaller) SetAddr(id raft.NodeID, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addrs[id] = addr
}

func (c *GRPCCaller) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.conns {
		cc.Close()
	}
	c.conns = map[raft.NodeID]*grpc.ClientConn{}
}

func (c *GRPCCaller) client(id raft.NodeID) (kvpb.KVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc := c.conns[id]; cc != nil {
		return kvpb.NewKVClient(cc), nil
	}
	addr, ok := c.addrs[id]
	if !ok {
		return nil, fmt.Errorf("kv: no address for server %q", id)
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 500 * time.Millisecond},
			MinConnectTimeout: time.Second,
		}))
	if err != nil {
		return nil, err
	}
	c.conns[id] = cc
	return kvpb.NewKVClient(cc), nil
}

func (c *GRPCCaller) Command(ctx context.Context, to raft.NodeID, a *kvpb.CommandArgs) (*kvpb.CommandReply, error) {
	cl, err := c.client(to)
	if err != nil {
		return nil, err
	}
	return cl.Command(ctx, a)
}

func (c *GRPCCaller) AddServer(ctx context.Context, to raft.NodeID, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	cl, err := c.client(to)
	if err != nil {
		return nil, err
	}
	return cl.AddServer(ctx, a)
}

func (c *GRPCCaller) RemoveServer(ctx context.Context, to raft.NodeID, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	cl, err := c.client(to)
	if err != nil {
		return nil, err
	}
	return cl.RemoveServer(ctx, a)
}
