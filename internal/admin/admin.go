// Package admin is the control-plane gRPC service of a node: status, network
// rules and clock skew. The chaos runner (Phase 9) drives it.
package admin

import (
	"context"
	"fmt"
	"log/slog"
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

// NetworkRules applies per-link fault rules. transport.FaultNet (the faulty
// wrapper around the node's real transport) implements it.
type NetworkRules interface {
	// SetRules replaces the rule set (clearAll true: drop every rule first,
	// then apply rules, which may be empty). A rule replaces the rule for the
	// same (from, to) pair; an all-zero rule removes it.
	SetRules(clearAll bool, rules []*adminpb.NetRule) error
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

// SetClock sets the node's clock fault. offset_ms is ABSOLUTE: if set_offset
// is true or offset_ms is non-zero, the node's skew (its clock minus real
// time) becomes exactly offset_ms, so repeating a request changes nothing and
// offset_ms 0 with set_offset removes a jump. drift_rate sets the speed of the
// clock (1.0 is real time; <= 0 leaves the rate unchanged). A rate change also
// rescales timers and tickers that are already armed (clock.SkewedClock).
func (s *Service) SetClock(ctx context.Context, r *adminpb.SetClockRequest) (*adminpb.SetClockReply, error) {
	if s.clk == nil {
		return nil, status.Error(codes.Unimplemented, "node clock is not skewable")
	}
	if r.SetOffset || r.OffsetMs != 0 {
		s.clk.SetOffset(time.Duration(r.OffsetMs) * time.Millisecond)
	}
	if r.DriftRate > 0 {
		s.clk.SetRate(r.DriftRate)
	}
	s.log.Info("clock skew set", "node", s.node.Status().ID, "offset_ms", r.OffsetMs, "rate", r.DriftRate)
	return &adminpb.SetClockReply{}, nil
}
