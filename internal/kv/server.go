package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
)

// Proposer is the part of Raft the server needs.
type Proposer interface {
	Propose(cmd []byte) (index, term uint64, isLeader bool)
	Status() raft.Status
	// LogSize is the bytes of log held after the snapshot; Snapshot compacts
	// the log through index using the state machine image data.
	LogSize() int
	Snapshot(index uint64, data []byte)
	// ReadIndex and LeaseRead return once a local read is linearizable (see
	// raft/readindex.go and raft/lease.go).
	ReadIndex(ctx context.Context) (uint64, error)
	LeaseRead(ctx context.Context) (uint64, error)
}

// ReadMode selects how Gets are served.
type ReadMode int

const (
	// ReadLog sends every Get through the log (the default; always safe).
	ReadLog ReadMode = iota
	// ReadIndex serves Gets locally after a heartbeat-confirmed read index.
	ReadIndex
	// ReadLease serves Gets locally while the leader's lease is valid, falling
	// back to ReadIndex when it is not. Unsafe under clock drift.
	ReadLease
)

func (m ReadMode) String() string {
	switch m {
	case ReadIndex:
		return "readindex"
	case ReadLease:
		return "lease"
	}
	return "log"
}

// ParseReadMode parses the --read-mode flag value (log|readindex|lease).
func ParseReadMode(s string) (ReadMode, error) {
	switch s {
	case "log", "":
		return ReadLog, nil
	case "readindex":
		return ReadIndex, nil
	case "lease":
		return ReadLease, nil
	}
	return ReadLog, fmt.Errorf("kv: unknown read mode %q (want log|readindex|lease)", s)
}

// DefaultMaxLogBytes is the log size above which the server snapshots.
const DefaultMaxLogBytes = 1 << 20

const (
	// requestTimeout bounds how long the server waits for an index to apply.
	// The client then retries; dedup makes that safe.
	requestTimeout = 2 * time.Second
	// leaderCheckEvery is how often a waiting request re-checks that it still
	// leads the term it proposed in.
	leaderCheckEvery = 50 * time.Millisecond
)

type applied struct {
	term  uint64
	reply *kvpb.CommandReply
}

// Server turns client commands into log entries and answers when they apply.
type Server struct {
	rf    Proposer
	store *Store
	clk   clock.Clock

	maxLogBytes int // snapshot when the Raft log passes this; <= 0 disables
	readMode    ReadMode

	mu      sync.Mutex
	waiters map[uint64]chan applied // log index -> request waiting on it
}

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithMaxLogBytes sets the log size that triggers a snapshot (<= 0: never).
func WithMaxLogBytes(n int) ServerOption { return func(s *Server) { s.maxLogBytes = n } }

// WithReadMode sets how Gets are served (cmd/server's --read-mode, Phase 8).
func WithReadMode(m ReadMode) ServerOption { return func(s *Server) { s.readMode = m } }

func NewServer(rf Proposer, st *Store, clk clock.Clock, opts ...ServerOption) *Server {
	s := &Server{rf: rf, store: st, clk: clk, maxLogBytes: DefaultMaxLogBytes, waiters: map[uint64]chan applied{}}
	for _, o := range opts {
		o(s)
	}
	st.onApply = s.deliver
	st.onRestore = s.failWaiters
	return s
}

// failWaiters answers every waiting request ErrWrongLeader. It runs when a
// snapshot replaces the state: waiters on indexes the snapshot jumped over
// would otherwise never be woken (the entries are never applied here), and any
// waiter at all belongs to a leadership that just ended, since only a newer
// leader sends us a snapshot. The client retries; dedup makes that safe.
func (s *Server) failWaiters() {
	s.mu.Lock()
	ws := s.waiters
	s.waiters = map[uint64]chan applied{}
	s.mu.Unlock()
	for _, ch := range ws {
		ch <- applied{term: 0} // buffered (cap 1); term 0 never matches a proposal
	}
}

func (s *Server) Store() *Store { return s.store }

// deliver wakes the request waiting on index, if any.
func (s *Server) deliver(index, term uint64, rep *kvpb.CommandReply) {
	s.mu.Lock()
	ch, ok := s.waiters[index]
	if ok {
		delete(s.waiters, index)
	}
	s.mu.Unlock()
	if ok {
		ch <- applied{term, rep} // buffered (cap 1), never blocks
	}
	s.maybeSnapshot(index)
}

// maybeSnapshot compacts the Raft log once it passes maxLogBytes. It runs from
// the apply hook, on Raft's applier goroutine with no lock held, so the store
// is at exactly index: its image is the right one to pair with that index.
func (s *Server) maybeSnapshot(index uint64) {
	if s.maxLogBytes <= 0 || s.rf.LogSize() <= s.maxLogBytes {
		return
	}
	data, err := s.store.Snapshot()
	if err != nil {
		return // can't happen for gob of plain maps; try again at the next apply
	}
	s.rf.Snapshot(index, data)
}

// Command runs one request through the log. Every op, including Get, is
// logged (later phases speed up reads). The reply's Err says what happened:
// OK/NO_KEY (applied), WRONG_LEADER (retry elsewhere), TIMEOUT (unknown
// outcome: retry the same seq).
func (s *Server) Command(ctx context.Context, a *kvpb.CommandArgs) (*kvpb.CommandReply, error) {
	if a.Op == kvpb.Op_GET && s.readMode != ReadLog {
		return s.localRead(ctx, a)
	}
	// s.mu is held across Propose so that deliver (called when the entry
	// applies) can't run before the waiter is registered. Lock order is
	// s.mu -> raft.mu; the applier calls deliver with no raft lock held.
	ch := make(chan applied, 1)
	s.mu.Lock()
	index, term, ok := s.rf.Propose(Encode(a))
	if !ok {
		s.mu.Unlock()
		return &kvpb.CommandReply{Err: kvpb.Err_ERR_WRONG_LEADER, LeaderHint: string(s.rf.Status().Leader)}, nil
	}
	if old, dup := s.waiters[index]; dup {
		// A previous leader's request at this index was overwritten.
		old <- applied{term: 0}
	}
	s.waiters[index] = ch
	s.mu.Unlock()
	defer s.forget(index, ch)

	deadline := s.clk.NewTimer(requestTimeout)
	defer deadline.Stop()
	check := s.clk.NewTicker(leaderCheckEvery)
	defer check.Stop()
	for {
		select {
		case ap := <-ch:
			if ap.term != term {
				// A different entry took this index: we lost leadership
				// before ours committed. It may or may not commit later at
				// another index; the client retries and dedup covers it.
				return s.wrongLeader(), nil
			}
			return ap.reply, nil
		case <-check.C():
			if st := s.rf.Status(); st.Role != raft.Leader || st.Term != term {
				return s.wrongLeader(), nil
			}
		case <-deadline.C():
			return &kvpb.CommandReply{Err: kvpb.Err_ERR_TIMEOUT}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Membership is the part of Raft that changes the cluster (raft.Raft has it;
// test stubs need not).
type Membership interface {
	AddServer(ctx context.Context, id raft.NodeID, addr string) error
	RemoveServer(ctx context.Context, id raft.NodeID) error
}

// AddServer runs a membership change on this node, which must lead. Both calls
// are idempotent, so a client that timed out simply asks again.
func (s *Server) AddServer(ctx context.Context, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	return s.membership(func(m Membership) error { return m.AddServer(ctx, raft.NodeID(a.Id), a.Addr) }, ctx)
}

func (s *Server) RemoveServer(ctx context.Context, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error) {
	return s.membership(func(m Membership) error { return m.RemoveServer(ctx, raft.NodeID(a.Id)) }, ctx)
}

func (s *Server) membership(do func(Membership) error, ctx context.Context) (*kvpb.MembershipReply, error) {
	m, ok := s.rf.(Membership)
	if !ok {
		return &kvpb.MembershipReply{Err: kvpb.Err_ERR_FAILED, Message: "membership changes not supported"}, nil
	}
	err := do(m)
	hint := string(s.rf.Status().Leader)
	switch {
	case err == nil:
		return &kvpb.MembershipReply{Err: kvpb.Err_OK}, nil
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrStopped):
		return &kvpb.MembershipReply{Err: kvpb.Err_ERR_WRONG_LEADER, LeaderHint: hint}, nil
	case errors.Is(err, raft.ErrNotReady), errors.Is(err, raft.ErrConfigPending):
		return &kvpb.MembershipReply{Err: kvpb.Err_ERR_CONFIG_BUSY, Message: err.Error()}, nil
	case ctx.Err() != nil:
		return nil, ctx.Err()
	default:
		return &kvpb.MembershipReply{Err: kvpb.Err_ERR_FAILED, Message: err.Error()}, nil
	}
}

func (s *Server) wrongLeader() *kvpb.CommandReply {
	return &kvpb.CommandReply{Err: kvpb.Err_ERR_WRONG_LEADER, LeaderHint: string(s.rf.Status().Leader)}
}

func (s *Server) forget(index uint64, ch chan applied) {
	s.mu.Lock()
	if s.waiters[index] == ch {
		delete(s.waiters, index)
	}
	s.mu.Unlock()
}

// localRead serves a Get from the local store once Raft says that is
// linearizable. The Get is not logged, so it is not deduplicated; a Get is
// idempotent, so a retry is harmless.
func (s *Server) localRead(ctx context.Context, a *kvpb.CommandArgs) (*kvpb.CommandReply, error) {
	deadline := s.clk.NewTimer(requestTimeout)
	defer deadline.Stop()
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The read waits on Raft, not on a clock, so a timer goroutine turns the
	// request deadline (on our Clock, so FakeClock tests drive it) into a
	// context cancel.
	go func() {
		select {
		case <-deadline.C():
			cancel()
		case <-rctx.Done():
		}
	}()
	for {
		var err error
		if s.readMode == ReadLease {
			_, err = s.rf.LeaseRead(rctx)
			if errors.Is(err, raft.ErrLeaseExpired) {
				_, err = s.rf.ReadIndex(rctx) // lease lapsed: confirm with a round
			}
		} else {
			_, err = s.rf.ReadIndex(rctx)
		}
		switch {
		case err == nil:
			// lastApplied >= the read index, so the store holds every entry
			// that was committed when the read began.
			v, ok := s.store.Peek(a.Key)
			rep := &kvpb.CommandReply{Err: kvpb.Err_OK, Value: v}
			if !ok {
				rep.Err = kvpb.Err_ERR_NO_KEY
			}
			return rep, nil
		case errors.Is(err, raft.ErrNotReady) && rctx.Err() == nil:
			// Newly elected, no-op not committed yet: wait a beat and retry.
			t := s.clk.NewTimer(leaderCheckEvery)
			select {
			case <-t.C():
			case <-rctx.Done():
			}
			t.Stop()
		case errors.Is(err, raft.ErrNotLeader):
			return s.wrongLeader(), nil
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case errors.Is(err, raft.ErrStopped):
			return s.wrongLeader(), nil
		default: // our own deadline fired (or the leader is not ready in time)
			return &kvpb.CommandReply{Err: kvpb.Err_ERR_TIMEOUT}, nil
		}
	}
}
