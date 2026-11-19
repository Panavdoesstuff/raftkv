package admin

import (
	"context"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/adminpb"
)

type fakeNode struct{}

func (fakeNode) Status() raft.Status {
	return raft.Status{ID: "n1", Role: raft.Leader, Term: 3, CommitIndex: 7, LastApplied: 6, Leader: "n1"}
}
func (fakeNode) Config() raft.ClusterConfig {
	return raft.ClusterConfig{Voters: []raft.NodeID{"n1", "n2"}, Learners: []raft.NodeID{"n3"},
		Addrs: map[raft.NodeID]string{"n3": "h:1"}}
}

func TestStatus(t *testing.T) {
	s := New(fakeNode{}, nil, nil, nil)
	r, err := s.Status(context.Background(), &adminpb.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Role != "leader" || r.Term != 3 || r.CommitIndex != 7 || r.LastApplied != 6 || r.LeaderId != "n1" {
		t.Fatalf("status: %+v", r)
	}
	want := []string{"n1 voter", "n2 voter", "n3 learner h:1"}
	if len(r.Config) != 3 || r.Config[0] != want[0] || r.Config[1] != want[1] || r.Config[2] != want[2] {
		t.Fatalf("config: %v", r.Config)
	}
}

func TestSetClockAndNetRules(t *testing.T) {
	base := clock.NewFake()
	sk := clock.NewSkewed(base, 0, 1)
	stub := &StubNetwork{}
	s := New(fakeNode{}, sk, stub, nil)
	t0 := sk.Now()
	if _, err := s.SetClock(context.Background(), &adminpb.SetClockRequest{OffsetMs: 5000, DriftRate: 0.5}); err != nil {
		t.Fatal(err)
	}
	if d := sk.Now().Sub(t0); d != 5*time.Second {
		t.Fatalf("offset jump = %v, want 5s", d)
	}
	base.Advance(10 * time.Second)
	if d := sk.Now().Sub(t0); d != 10*time.Second { // 5 s jump + 10 s * 0.5
		t.Fatalf("after drift = %v, want 10s", d)
	}
	if _, err := s.SetNetRules(context.Background(), &adminpb.SetNetRulesRequest{Rules: []*adminpb.NetRule{{From: "n1", To: "n2", Block: true}}}); err != nil {
		t.Fatal(err)
	}
	if len(stub.Rules()) != 1 {
		t.Fatalf("rules: %v", stub.Rules())
	}
	s.SetNetRules(context.Background(), &adminpb.SetNetRulesRequest{ClearAll: true})
	if len(stub.Rules()) != 0 {
		t.Fatal("clear_all did not clear")
	}
}
