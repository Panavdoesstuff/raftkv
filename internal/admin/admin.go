// Package admin is the control-plane gRPC service of a node: status, network
// rules and clock skew. The chaos runner (Phase 9) drives it.
package admin

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/adminpb"
)

// Node is the part of Raft the service reports on.
type Node interface {
	Status() raft.Status
	Config() raft.ClusterConfig
}

// NetworkRules applies per-link fault rules. Phase 9's faulty transport
// implements it; until then a node runs with StubNetwork.
type NetworkRules interface {
	// SetRules replaces the rule set (clearAll true: drop every rule first,
	// then apply rules, which may be empty).
	SetRules(clearAll bool, rules []*adminpb.NetRule) error
}

// StubNetwork accepts rules and remembers them but injects nothing. It lets the
// SetNetRules RPC be exercised end to end before the faulty transport exists.
type StubNetwork struct {
	mu    sync.Mutex
	rules []*adminpb.NetRule
}

func (s *StubNetwork) SetRules(clearAll bool, rules []*adminpb.NetRule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if clearAll {
		s.rules = nil
	}
	s.rules = append(s.rules, rules...)
	return nil
}

// Rules returns the rules received so far.
func (s *StubNetwork) Rules() []*adminpb.NetRule {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*adminpb.NetRule(nil), s.rules...)
}

// Service implements adminpb.AdminServer.
type Service struct {
	adminpb.UnimplementedAdminServer
	node Node
	clk  *clock.SkewedClock
	net  NetworkRules
	log  *slog.Logger
}

func New(n Node, clk *clock.SkewedClock, net NetworkRules, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{node: n, clk: clk, net: net, log: log}
}

func (s *Service) Status(ctx context.Context, _ *adminpb.StatusRequest) (*adminpb.StatusReply, error) {
	st := s.node.Status()
	cfg := s.node.Config()
	var members []string
	for _, id := range cfg.Voters {
		members = append(members, memberString(id, "voter", cfg.Addrs))
	}
	for _, id := range cfg.Learners {
		members = append(members, memberString(id, "learner", cfg.Addrs))
	}
	return &adminpb.StatusReply{
		Id: string(st.ID), Role: st.Role.String(), Term: st.Term,
		CommitIndex: st.CommitIndex, LastApplied: st.LastApplied,
		LeaderId: string(st.Leader), Config: members,
	}, nil
}

func memberString(id raft.NodeID, role string, addrs map[raft.NodeID]string) string {
	if a := addrs[id]; a != "" {
		return fmt.Sprintf("%s %s %s", id, role, a)
	}
	return fmt.Sprintf("%s %s", id, role)
}

func (s *Service) SetNetRules(ctx context.Context, r *adminpb.SetNetRulesRequest) (*adminpb.SetNetRulesReply, error) {
	if s.net == nil {
		return nil, status.Error(codes.Unimplemented, "no network rules plugged in")
	}
	if err := s.net.SetRules(r.ClearAll, r.Rules); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.log.Info("network rules set", "node", s.node.Status().ID, "rules", len(r.Rules), "clear_all", r.ClearAll)
	return &adminpb.SetNetRulesReply{}, nil
}

// SetClock jumps the node's clock by offset_ms (positive: forward; zero: no
// jump) and then sets the drift rate (1.0 is real time; <= 0 leaves the rate
// unchanged). Timers created before a rate change keep their old period.
func (s *Service) SetClock(ctx context.Context, r *adminpb.SetClockRequest) (*adminpb.SetClockReply, error) {
	if s.clk == nil {
		return nil, status.Error(codes.Unimplemented, "node clock is not skewable")
	}
	if r.OffsetMs != 0 {
		s.clk.SetOffset(time.Duration(r.OffsetMs) * time.Millisecond)
	}
	if r.DriftRate > 0 {
		s.clk.SetRate(r.DriftRate)
	}
	s.log.Info("clock skew set", "node", s.node.Status().ID, "offset_ms", r.OffsetMs, "rate", r.DriftRate)
	return &adminpb.SetClockReply{}, nil
}
