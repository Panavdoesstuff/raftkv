package chaos

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/transport"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
	"github.com/panavdoesstuff/raftkv/test"
)

// pausedRate is the clock rate of a paused node: effectively frozen. Armed
// timers and tickers are rescaled by SkewedClock.SetRate, so nothing on the
// node fires while it is paused.
const pausedRate = 1e-9

// MemOptions configure NewMemTarget.
type MemOptions struct {
	Nodes       int // default 3
	Seed        int64
	ReadMode    kv.ReadMode
	MaxLogBytes int // snapshot threshold; default 32 KiB so snapshots happen in a run
}

// fatalError is what the harness's Fatalf panics with, so a harness failure
// (no leader, state machine safety violated, ...) aborts the run with a message
// instead of killing the process.
type fatalError struct{ msg string }

func (e fatalError) Error() string { return e.msg }

// harnessT adapts the memnet harness (written against testing.T) to a process
// without one. Errorf is recorded (the harness uses it for safety violations
// seen on its own goroutines); Fatalf panics.
type harnessT struct {
	mu   sync.Mutex
	errs []string
	logs []string
	fail bool
}

func (h *harnessT) Helper() {}
func (h *harnessT) Logf(f string, a ...any) {
	h.mu.Lock()
	h.logs = append(h.logs, fmt.Sprintf(f, a...))
	h.mu.Unlock()
}
func (h *harnessT) Errorf(f string, a ...any) {
	h.mu.Lock()
	h.errs = append(h.errs, fmt.Sprintf(f, a...))
	h.fail = true
	h.mu.Unlock()
}
func (h *harnessT) Fatalf(f string, a ...any) {
	h.Errorf(f, a...)
	panic(fatalError{fmt.Sprintf(f, a...)})
}
func (h *harnessT) Cleanup(func()) {} // Close does the cleanup itself
func (h *harnessT) Failed() bool   { h.mu.Lock(); defer h.mu.Unlock(); return h.fail }
func (h *harnessT) errors() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.errs...)
}

// MemTarget is an in-process cluster on memnet and a FakeClock, built from
// the same pieces as the unit-test harness: every node runs Raft and the KV
// server over a faulty transport and a SkewedClock.
type MemTarget struct {
	opts MemOptions
	ht   *harnessT
	cl   *test.Cluster
	fn   *transport.FaultNet
	fake clock.Clock

	surg    sync.Mutex // serialises operations on the harness Cluster (it is not thread safe)
	mu      sync.Mutex // guards the maps below; short sections only (the node factory takes it too)
	servers map[raft.NodeID]*kv.Server
	clocks  map[raft.NodeID]*clock.SkewedClock
	down    map[raft.NodeID]bool
	paused  map[raft.NodeID]chan struct{} // closed on Resume
	pauseAt map[raft.NodeID]time.Time
	pauseRt map[raft.NodeID]float64

	stop    chan struct{}
	stopped sync.WaitGroup
}

// NewMemTarget builds and starts the cluster and the goroutine that advances
// its clock. The caller must Close it.
func NewMemTarget(o MemOptions) (m *MemTarget, err error) {
	if o.Nodes == 0 {
		o.Nodes = 3
	}
	if o.MaxLogBytes == 0 {
		o.MaxLogBytes = 32 << 10
	}
	m = &MemTarget{
		opts: o, ht: &harnessT{},
		servers: map[raft.NodeID]*kv.Server{}, clocks: map[raft.NodeID]*clock.SkewedClock{},
		down: map[raft.NodeID]bool{}, paused: map[raft.NodeID]chan struct{}{},
		pauseAt: map[raft.NodeID]time.Time{}, pauseRt: map[raft.NodeID]float64{},
		stop: make(chan struct{}),
	}
	defer func() {
		if r := recover(); r != nil {
			if fe, ok := r.(fatalError); ok {
				err = fe
				return
			}
			panic(r)
		}
	}()
	factory := func(c test.Config) test.Node {
		r := raft.New(raft.Config{
			ID: c.ID, Peers: c.Peers, Transport: m.fn.Wrap(c.ID, c.Transport), Clock: c.Clock,
			Storage: c.Storage, SM: c.SM, Seed: c.Seed,
		})
		srv := kv.NewServer(r, c.SM.(*kv.Store), c.Clock,
			kv.WithMaxLogBytes(o.MaxLogBytes), kv.WithReadMode(o.ReadMode))
		m.mu.Lock()
		m.servers[c.ID] = srv
		m.mu.Unlock()
		return &kvNode{Raft: r}
	}
	nodeClock := func(id raft.NodeID, base clock.Clock) clock.Clock {
		c := clock.NewSkewed(base, 0, 1)
		m.mu.Lock()
		m.clocks[id] = c
		m.mu.Unlock()
		return c
	}
	// The faulty network needs the cluster clock, which StartCluster makes;
	// build the clock first by creating the FaultNet lazily on the same fake.
	fake := clock.NewFake()
	m.fake = fake
	m.fn = transport.NewFaultNet(o.Seed, fake)
	m.cl = test.StartCluster(m.ht, o.Nodes, o.Seed, factory,
		test.WithClock(fake),
		test.WithNodeClock(nodeClock),
		test.WithStateMachine(func() raft.StateMachine { return kv.NewStore() }))
	m.stopped.Add(1)
	go m.drive()
	return m, nil
}

type kvNode struct{ *raft.Raft }

// drive advances the cluster clock in 10 ms steps, yielding real time between
// steps so the nodes' goroutines run (the same pacing as the unit-test
// harness).
func (m *MemTarget) drive() {
	defer m.stopped.Done()
	for {
		select {
		case <-m.stop:
			return
		default:
		}
		m.cl.Advance(10 * time.Millisecond)
	}
}

func (m *MemTarget) Name() string             { return "memnet" }
func (m *MemTarget) Clock() clock.Clock       { return m.fake }
func (m *MemTarget) Harness() []string        { return m.ht.errors() }
func (m *MemTarget) Net() *transport.FaultNet { return m.fn }

func (m *MemTarget) Nodes() []raft.NodeID {
	m.surg.Lock()
	defer m.surg.Unlock()
	return m.cl.IDs()
}

func (m *MemTarget) Leader() (raft.NodeID, bool) {
	m.surg.Lock()
	defer m.surg.Unlock()
	var best raft.NodeID
	var bestTerm uint64
	for _, id := range m.cl.IDs() {
		m.mu.Lock()
		frozen := m.paused[id] != nil
		m.mu.Unlock()
		if !m.cl.Alive(id) || frozen {
			continue
		}
		if st := m.cl.Node(id).Status(); st.Role == raft.Leader && st.Term >= bestTerm {
			best, bestTerm = id, st.Term
		}
	}
	return best, best != ""
}

func (m *MemTarget) SetRules(clearAll bool, rules []transport.Rule) error {
	if clearAll {
		m.fn.Clear()
	}
	m.fn.Apply(rules...)
	return nil
}

func (m *MemTarget) SetClock(id raft.NodeID, setOffset bool, offset time.Duration, rate float64) error {
	m.mu.Lock()
	c := m.clocks[id]
	frozen := m.paused[id] != nil
	m.mu.Unlock()
	if c == nil {
		return fmt.Errorf("no clock for %s", id)
	}
	if frozen {
		return fmt.Errorf("%s is paused", id)
	}
	if setOffset {
		c.SetOffset(offset)
	}
	if rate > 0 {
		c.SetRate(rate)
	}
	return nil
}

func (m *MemTarget) Crash(id raft.NodeID) error {
	m.surg.Lock()
	defer m.surg.Unlock()
	m.mu.Lock()
	if m.down[id] {
		m.mu.Unlock()
		return nil
	}
	if ch := m.paused[id]; ch != nil {
		close(ch) // release requests hanging on the frozen node; they find it down
		delete(m.paused, id)
	}
	m.down[id] = true
	m.mu.Unlock()
	m.cl.Crash(id)
	return nil
}

func (m *MemTarget) Restart(id raft.NodeID) (err error) {
	m.surg.Lock()
	defer m.surg.Unlock()
	m.mu.Lock()
	isDown := m.down[id]
	m.mu.Unlock()
	if !isDown {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			if fe, ok := r.(fatalError); ok {
				err = fe
				return
			}
			panic(r)
		}
	}()
	m.cl.Restart(id) // the node factory registers the new server under m.mu
	m.mu.Lock()
	m.down[id] = false
	m.mu.Unlock()
	return nil
}

// Pause freezes the node: its clock stops (so no timer, ticker, election or
// heartbeat fires and Now() does not move), it is cut off the network, and
// client requests to it hang. Memory is untouched. Messages sent to it while
// paused are lost (a real docker pause would queue them in socket buffers).
func (m *MemTarget) Pause(id raft.NodeID) error {
	m.surg.Lock()
	defer m.surg.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down[id] || m.paused[id] != nil {
		return nil
	}
	c := m.clocks[id]
	m.pauseAt[id], m.pauseRt[id] = m.fake.Now(), c.Rate()
	m.paused[id] = make(chan struct{})
	c.SetRate(pausedRate)
	m.cl.Disconnect(id)
	return nil
}

// Resume unfreezes the node. Wall time kept passing while it was frozen (as
// with a paused container, whose monotonic clock keeps running), so its clock
// jumps forward by the paused time (at its drift rate): election deadlines
// have passed, leases have lapsed.
func (m *MemTarget) Resume(id raft.NodeID) error {
	m.surg.Lock()
	defer m.surg.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := m.paused[id]
	if ch == nil {
		return nil
	}
	c := m.clocks[id]
	rate := m.pauseRt[id]
	frozenFor := m.fake.Now().Sub(m.pauseAt[id])
	c.SetRate(rate)
	c.Jump(time.Duration(float64(frozenFor) * rate))
	m.cl.Connect(id)
	delete(m.paused, id)
	close(ch)
	return nil
}

func (m *MemTarget) AddNode(id raft.NodeID) (addr string, err error) {
	m.surg.Lock()
	defer m.surg.Unlock()
	defer func() {
		if r := recover(); r != nil {
			if fe, ok := r.(fatalError); ok {
				err = fe
				return
			}
			panic(r)
		}
	}()
	m.cl.AddNode(id)
	return "mem:" + string(id), nil
}

func (m *MemTarget) RecoverAll() error {
	m.fn.Clear()
	for _, id := range m.Nodes() {
		if err := m.Resume(id); err != nil {
			return err
		}
		if err := m.Restart(id); err != nil {
			return err
		}
		m.mu.Lock()
		c := m.clocks[id]
		m.mu.Unlock()
		c.SetOffset(0)
		c.SetRate(1)
	}
	return nil
}

func (m *MemTarget) Close() {
	close(m.stop)
	m.stopped.Wait()
	m.fn.Close()
	m.surg.Lock()
	defer m.surg.Unlock()
	for _, id := range m.cl.IDs() {
		if m.cl.Alive(id) {
			m.cl.Crash(id)
		}
	}
}

// Caller delivers client RPCs straight to the node's KV server. A crashed node
// refuses; a paused one hangs until resumed or the request is cancelled.
func (m *MemTarget) Caller() kv.Caller { return memCaller{m} }

type memCaller struct{ m *MemTarget }

var errUnreachable = errors.New("chaos: server unreachable")

func (c memCaller) server(ctx context.Context, to raft.NodeID) (*kv.Server, error) {
	c.m.mu.Lock()
	srv, down, pause := c.m.servers[to], c.m.down[to], c.m.paused[to]
	c.m.mu.Unlock()
	if srv == nil || down {
		return nil, errUnreachable
	}
	if pause != nil {
		select {
		case <-pause:
			return c.server(ctx, to)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return srv, nil
}

func (c memCaller) Command(ctx context.Context, to raft.NodeID, a *kvpb.CommandArgs) (*kvpb.CommandReply, error) {
	srv, err := c.server(ctx, to)
	if err != nil {
		return nil, err
	}
	return srv.Command(ctx, a)
}

func (c memCaller) AddServer(ctx context.Context, to raft.NodeID, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	srv, err := c.server(ctx, to)
	if err != nil {
		return nil, err
	}
	return srv.AddServer(ctx, a)
}

func (c memCaller) RemoveServer(ctx context.Context, to raft.NodeID, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	srv, err := c.server(ctx, to)
	if err != nil {
		return nil, err
	}
	return srv.RemoveServer(ctx, a)
}
