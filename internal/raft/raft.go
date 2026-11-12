package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
	// tickInterval is the resolution of all timing decisions. The node checks
	// its deadlines against the Clock on every tick.
	tickInterval = 10 * time.Millisecond
)

var ErrStopped = errors.New("raft: node stopped")

// Config is everything a node needs. All of it is injected (BUILDMAP §1).
type Config struct {
	ID        NodeID
	Peers     []NodeID // every voting member, including this node
	Transport Transport
	Clock     clock.Clock
	Storage   Storage
	SM        StateMachine
	Seed      int64 // seeds election timeout jitter
}

// Raft is one node. Lock discipline (BUILDMAP §3): mu is never held across an
// RPC, a disk write or a channel send. After any unlock/relock, term and role
// are re-checked before a reply is used.
type Raft struct {
	id    NodeID
	cfg   Config
	peers []NodeID // everyone but us
	clk   clock.Clock
	tr    Transport
	slog  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	rng      *rand.Rand
	started  bool
	stopped  bool
	role     Role
	term     uint64
	votedFor NodeID
	leaderID NodeID

	// Log. entries[i] has Index snapIndex+1+i. Every index calculation goes
	// through lastIndex/lastTerm (and, from Phase 2, entryAt) so the snapshot
	// offset lives in one place.
	snapIndex, snapTerm uint64
	entries             []Entry

	commitIndex, lastApplied uint64

	electionDeadline time.Time
	nextHeartbeat    time.Time
	votes            int
	inflight         map[NodeID]bool // a heartbeat to this peer is outstanding
}

func New(cfg Config) *Raft {
	ctx, cancel := context.WithCancel(context.Background())
	level := slog.LevelError
	if os.Getenv("RAFT_DEBUG") == "1" {
		level = slog.LevelDebug
	}
	r := &Raft{
		id: cfg.ID, cfg: cfg, clk: cfg.Clock, tr: cfg.Transport,
		slog: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})),
		ctx:  ctx, cancel: cancel,
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		inflight: map[NodeID]bool{},
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			r.peers = append(r.peers, p)
		}
	}
	return r
}

// dbg logs at debug level with node, term and role. Caller holds mu.
func (r *Raft) dbg(msg string, args ...any) {
	r.slog.Debug(msg, append([]any{"node", r.id, "term", r.term, "role", r.role}, args...)...)
}

// Start loads durable state and begins as a follower.
func (r *Raft) Start() error {
	hs, entries, snap, err := r.cfg.Storage.Load()
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return errors.New("raft: already started")
	}
	r.started = true
	r.term, r.votedFor = hs.Term, hs.VotedFor
	if snap != nil {
		r.snapIndex, r.snapTerm = snap.LastIncludedIndex, snap.LastIncludedTerm
		r.commitIndex, r.lastApplied = snap.LastIncludedIndex, snap.LastIncludedIndex
	}
	r.entries = entries
	r.role = Follower
	r.resetElectionTimer()

	tk := r.clk.NewTicker(tickInterval)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer tk.Stop()
		for {
			select {
			case <-r.ctx.Done():
				return
			case <-tk.C():
				r.tick()
			}
		}
	}()
	return nil
}

// Stop halts the node and waits for its goroutines. Safe to call twice.
func (r *Raft) Stop() {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	r.cancel()
	r.wg.Wait()
}

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID: r.id, Role: r.role, Term: r.term,
		CommitIndex: r.commitIndex, LastApplied: r.lastApplied, LastIndex: r.lastIndex(),
	}
}

// Log returns a copy of the entries currently held (after any snapshot).
func (r *Raft) Log() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return CloneEntries(r.entries)
}

// --- log index helpers (the only place the snapshot offset is applied) ---

func (r *Raft) lastIndex() uint64 { return r.snapIndex + uint64(len(r.entries)) }

func (r *Raft) lastTerm() uint64 {
	if n := len(r.entries); n > 0 {
		return r.entries[n-1].Term
	}
	return r.snapTerm
}

func (r *Raft) majority() int { return len(r.cfg.Peers)/2 + 1 }

// resetElectionTimer pushes the deadline out by a fresh random timeout in
// [150, 300) ms. Call it ONLY when granting a vote, accepting AppendEntries
// from the current leader, or starting an election (Figure 2, Followers).
// Caller holds mu.
func (r *Raft) resetElectionTimer() {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	d := electionTimeoutMin + time.Duration(r.rng.Int63n(span))
	r.electionDeadline = r.clk.Now().Add(d)
}

// stepDown moves to follower. A higher term clears the vote (Figure 2, "All
// Servers"). Deliberately does not reset the election timer, except when a
// leader steps down: its deadline is stale from its own election, and without
// a re-arm it would start an election on the very next tick.
func (r *Raft) stepDown(term uint64) {
	if term > r.term {
		r.term = term
		r.votedFor = ""
	}
	wasLeader := r.role == Leader
	r.role = Follower
	if wasLeader {
		r.resetElectionTimer()
	}
	r.dbg("step down")
}

// tick runs every tickInterval on the node's Clock.
func (r *Raft) tick() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	now := r.clk.Now()
	if r.role == Leader {
		if !now.Before(r.nextHeartbeat) {
			r.broadcastHeartbeats()
		}
		return
	}
	if !now.Before(r.electionDeadline) {
		r.startElection()
	}
}
