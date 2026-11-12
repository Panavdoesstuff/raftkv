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
	nextIndex        map[NodeID]uint64 // leader only
	matchIndex       map[NodeID]uint64 // leader only

	applyCh   chan ApplyMsg
	applyWake chan struct{} // capacity 1: "commitIndex moved"
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
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		applyCh:   make(chan ApplyMsg, 256),
		applyWake: make(chan struct{}, 1),
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

	r.wg.Add(1)
	go r.applier()
	if r.commitIndex > r.lastApplied {
		r.notifyApply()
	}

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

// termAt returns the term of the entry at index i: snapTerm at the snapshot
// boundary, 0 for index 0 or anything outside the log. Caller holds mu.
func (r *Raft) termAt(i uint64) uint64 {
	if i == r.snapIndex {
		return r.snapTerm
	}
	if i < r.snapIndex || i > r.lastIndex() {
		return 0
	}
	return r.entries[i-r.snapIndex-1].Term
}

// entriesFrom returns up to max entries starting at index i (aliasing the
// log; clone before releasing mu). Caller holds mu.
func (r *Raft) entriesFrom(i uint64, max int) []Entry {
	if i <= r.snapIndex || i > r.lastIndex() {
		return nil
	}
	es := r.entries[i-r.snapIndex-1:]
	if len(es) > max {
		es = es[:max]
	}
	return es
}

// truncateFrom deletes every entry with index >= i. Caller holds mu.
// Phase 3: also truncate storage.
func (r *Raft) truncateFrom(i uint64) {
	if i <= r.snapIndex {
		return
	}
	r.entries = r.entries[:i-r.snapIndex-1]
}

// firstIndexOfTerm is the lowest index in the log with the given term (0 if none).
func (r *Raft) firstIndexOfTerm(term uint64) uint64 {
	for i, e := range r.entries {
		if e.Term == term {
			return r.snapIndex + 1 + uint64(i)
		}
	}
	return 0
}

// lastIndexOfTerm is the highest index in the log with the given term (0 if none).
func (r *Raft) lastIndexOfTerm(term uint64) uint64 {
	for i := len(r.entries) - 1; i >= 0; i-- {
		if r.entries[i].Term == term {
			return r.snapIndex + 1 + uint64(i)
		}
	}
	return 0
}

func (r *Raft) notifyApply() {
	select {
	case r.applyWake <- struct{}{}:
	default:
	}
}

// ApplyCh delivers every committed entry, in order, exactly once, including
// no-ops (consumers skip EntryNoop). It is closed when the node stops.
func (r *Raft) ApplyCh() <-chan ApplyMsg { return r.applyCh }

// applier is the only goroutine that advances lastApplied. It copies a batch
// under mu, then applies and sends with no lock held (BUILDMAP §3).
func (r *Raft) applier() {
	defer r.wg.Done()
	defer close(r.applyCh)
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.applyWake:
		}
		for {
			r.mu.Lock()
			if r.lastApplied >= r.commitIndex {
				r.mu.Unlock()
				break
			}
			batch := CloneEntries(r.entriesFrom(r.lastApplied+1, int(r.commitIndex-r.lastApplied)))
			r.mu.Unlock()
			for _, e := range batch {
				var res []byte
				if e.Type == EntryNormal && r.cfg.SM != nil {
					res = r.cfg.SM.Apply(e)
				}
				select {
				case r.applyCh <- ApplyMsg{Entry: e, Result: res}:
				case <-r.ctx.Done():
					return
				}
				r.mu.Lock()
				r.lastApplied = e.Index
				r.mu.Unlock()
			}
		}
	}
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
