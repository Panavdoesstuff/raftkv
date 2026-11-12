package kv

import (
	"context"
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
}

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

	mu      sync.Mutex
	waiters map[uint64]chan applied // log index -> request waiting on it
}

func NewServer(rf Proposer, st *Store, clk clock.Clock) *Server {
	s := &Server{rf: rf, store: st, clk: clk, waiters: map[uint64]chan applied{}}
	st.onApply = s.deliver
	return s
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
}

// Command runs one request through the log. Every op, including Get, is
// logged (later phases speed up reads). The reply's Err says what happened:
// OK/NO_KEY (applied), WRONG_LEADER (retry elsewhere), TIMEOUT (unknown
// outcome: retry the same seq).
func (s *Server) Command(ctx context.Context, a *kvpb.CommandArgs) (*kvpb.CommandReply, error) {
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
