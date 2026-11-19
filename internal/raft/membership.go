package raft

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

// Membership changes (Raft thesis ch. 4, single-server variant).
//
// The cluster configuration lives in the log as EntryConfig entries, each
// carrying the whole new config. Every node uses the LATEST config in its log,
// committed or not (thesis §4.1): it takes effect when appended, and reverts
// if the entry is truncated. Configs older than the snapshot survive only in
// Snapshot.Config.
//
// Voters count toward every quorum (commit, elections, ReadIndex confirmation,
// leases). Learners are replicated to but never counted.

var (
	ErrConfigPending = errors.New("raft: a configuration change is already in progress")
	ErrCatchUpFailed = errors.New("raft: new server did not catch up within the allowed rounds")
	ErrLastVoter     = errors.New("raft: cannot remove the last voter")
)

// maxCatchUpRounds bounds learner catch-up (thesis §4.2.1). A round has one
// election timeout to bring the learner up to the log end as of the round's
// start.
const maxCatchUpRounds = 10

// ClusterConfig is a membership: voters and learners, sorted. Addrs carries
// each member's dial address for transports that need one (Phase 8).
type ClusterConfig struct {
	Voters   []NodeID
	Learners []NodeID
	Addrs    map[NodeID]string
}

func (c ClusterConfig) IsVoter(id NodeID) bool   { return hasID(c.Voters, id) }
func (c ClusterConfig) IsLearner(id NodeID) bool { return hasID(c.Learners, id) }
func (c ClusterConfig) IsMember(id NodeID) bool  { return c.IsVoter(id) || c.IsLearner(id) }

func hasID(ids []NodeID, id NodeID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

func without(ids []NodeID, id NodeID) []NodeID {
	var out []NodeID
	for _, x := range ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

func (c ClusterConfig) clone() ClusterConfig {
	out := ClusterConfig{
		Voters:   append([]NodeID(nil), c.Voters...),
		Learners: append([]NodeID(nil), c.Learners...),
	}
	if len(c.Addrs) > 0 {
		out.Addrs = make(map[NodeID]string, len(c.Addrs))
		for k, v := range c.Addrs {
			out.Addrs[k] = v
		}
	}
	return out
}

func (c ClusterConfig) normalized() ClusterConfig {
	sort.Slice(c.Voters, func(i, j int) bool { return c.Voters[i] < c.Voters[j] })
	sort.Slice(c.Learners, func(i, j int) bool { return c.Learners[i] < c.Learners[j] })
	return c
}

func EncodeConfig(c ClusterConfig) []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // strings and slices cannot fail
	}
	return b
}

func DecodeConfig(b []byte) (ClusterConfig, error) {
	var c ClusterConfig
	err := json.Unmarshal(b, &c)
	return c, err
}

// Config returns the node's current (latest-in-log) configuration.
func (r *Raft) Config() ClusterConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mem.clone()
}

// setConfig makes c the node's configuration, as set by the entry at idx (0
// for the initial or snapshot config), and refreshes everything derived from
// it. Caller holds mu.
func (r *Raft) setConfig(c ClusterConfig, idx uint64) {
	r.mem, r.cfgIndex = c, idx
	if aw, ok := r.tr.(AddrAware); ok && len(c.Addrs) > 0 {
		aw.SetAddrs(c.Addrs)
	}
	r.selfVoter = c.IsVoter(r.id)
	r.voterPeers, r.replPeers = nil, nil
	for _, p := range c.Voters {
		if p != r.id {
			r.voterPeers = append(r.voterPeers, p)
			r.replPeers = append(r.replPeers, p)
		}
	}
	for _, p := range c.Learners {
		if p != r.id {
			r.replPeers = append(r.replPeers, p)
		}
	}
	if r.role == Leader {
		// A peer new to the config is probed from the log end and backs up
		// using its conflict hints (or gets a snapshot).
		for _, p := range r.replPeers {
			if _, ok := r.nextIndex[p]; !ok {
				r.nextIndex[p] = r.lastIndex() + 1
				r.matchIndex[p] = 0
			}
		}
	}
	r.dbg("config", "voters", c.Voters, "learners", c.Learners, "index", idx)
}

// refreshConfig recomputes the config from the log: the latest config entry
// in it, else the config at the snapshot boundary. Call after anything that
// removes entries (truncation, snapshot install). Caller holds mu.
func (r *Raft) refreshConfig() {
	for i := len(r.entries) - 1; i >= 0; i-- {
		if e := r.entries[i]; e.Type == EntryConfig {
			c, err := DecodeConfig(e.Data)
			if err != nil {
				r.halt(err)
				return
			}
			r.setConfig(c, e.Index)
			return
		}
	}
	r.setConfig(r.baseCfg.clone(), 0)
}

// noteConfigEntries adopts the last config entry among es, which were just
// appended to the log. Caller holds mu.
func (r *Raft) noteConfigEntries(es []Entry) {
	for i := len(es) - 1; i >= 0; i-- {
		if es[i].Type == EntryConfig {
			c, err := DecodeConfig(es[i].Data)
			if err != nil {
				r.halt(err)
				return
			}
			r.setConfig(c, es[i].Index)
			return
		}
	}
}

// configAt is the configuration in force at log index idx (idx >= snapIndex):
// the last config entry at or before it, else the snapshot-boundary config.
// Caller holds mu.
func (r *Raft) configAt(idx uint64) ClusterConfig {
	for i := len(r.entries) - 1; i >= 0; i-- {
		if e := r.entries[i]; e.Type == EntryConfig && e.Index <= idx {
			if c, err := DecodeConfig(e.Data); err == nil {
				return c
			}
		}
	}
	return r.baseCfg.clone()
}

// --- quorum arithmetic: every quorum in the package goes through these ---

// majority is the quorum size of the current voter set.
func (r *Raft) majority() int { return len(r.mem.Voters)/2 + 1 }

// quorumCount counts the current voters satisfying has. This node counts
// itself only if it is a voter: a leader that removed itself keeps leading
// but is not in its own majorities (thesis §4.2.2). Learners never count.
func (r *Raft) quorumCount(has func(NodeID) bool) int {
	n := 0
	if r.selfVoter {
		n++
	}
	for _, p := range r.voterPeers {
		if has(p) {
			n++
		}
	}
	return n
}

// maybeStepDownRemoved: a leader that is no longer a voter steps down once the
// config that removed it has committed. Caller holds mu.
func (r *Raft) maybeStepDownRemoved() {
	if r.role == Leader && !r.selfVoter && r.cfgIndex != 0 && r.cfgIndex <= r.commitIndex {
		r.dbg("removed from the cluster; stepping down")
		r.stepDown(r.term)
	}
}

// proposeConfigLocked appends next as a config entry. Caller holds mu.
func (r *Raft) proposeConfigLocked(next ClusterConfig) (uint64, error) {
	if r.stopped {
		return 0, ErrStopped
	}
	if r.role != Leader {
		return 0, ErrNotLeader
	}
	// Until we commit an entry of our own term we cannot know which earlier
	// config entries are committed; a change now could race with one that a
	// previous leader committed (the 2015 single-server bug, thesis §4.1).
	if r.termAt(r.commitIndex) != r.term {
		return 0, ErrNotReady
	}
	// One change at a time: two overlapping single-server changes can produce
	// disjoint majorities.
	if r.cfgIndex > r.commitIndex {
		return 0, ErrConfigPending
	}
	next = next.normalized()
	idx := r.appendLocal(Entry{Term: r.term, Type: EntryConfig, Data: EncodeConfig(next)})
	if r.stopped {
		return 0, ErrStopped
	}
	r.setConfig(next, idx)
	r.advanceCommit()
	r.broadcastHeartbeats()
	return idx, nil
}

// AddServer adds id as a voter: first as a learner, caught up in rounds, then
// promoted (thesis §4.2.1). It returns once the promoting entry commits. It is
// idempotent: retrying after a timeout resumes where the last attempt stopped.
//
// A learner is replicated to but not counted, so a slow or dead new server
// cannot stall commits or elections while it catches up.
func (r *Raft) AddServer(ctx context.Context, id NodeID, addr string) error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return ErrStopped
	}
	if r.role != Leader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	term := r.term
	if !r.mem.IsMember(id) {
		next := r.mem.clone()
		next.Learners = append(next.Learners, id)
		if next.Addrs == nil {
			next.Addrs = map[NodeID]string{}
		}
		next.Addrs[id] = addr
		idx, err := r.proposeConfigLocked(next)
		r.mu.Unlock()
		if err != nil {
			return err
		}
		if err := r.waitCommitted(ctx, idx, term); err != nil {
			return err
		}
		r.mu.Lock()
	}
	// The latest config entry (ours, or one from before) must commit before
	// we may append another.
	pending := r.cfgIndex
	voter := r.mem.IsVoter(id)
	r.mu.Unlock()
	if err := r.waitCommitted(ctx, pending, term); err != nil {
		return err
	}
	if voter {
		return nil
	}

	// Catch-up rounds. Each round targets the log end as of its start and has
	// one election timeout to get there; the first round that makes it means
	// the learner is close enough that promoting it will not stall the cluster.
	caught := false
	for round := 0; round < maxCatchUpRounds && !caught; round++ {
		r.mu.Lock()
		if r.stopped || r.role != Leader || r.term != term {
			r.mu.Unlock()
			return ErrNotLeader
		}
		target := r.lastIndex()
		r.mu.Unlock()
		ok, err := r.waitUntil(ctx, electionTimeoutMax, func() (bool, error) {
			if r.role != Leader || r.term != term {
				return false, ErrNotLeader
			}
			return r.matchIndex[id] >= target, nil
		})
		if err != nil {
			return err
		}
		caught = ok
	}
	if !caught {
		return ErrCatchUpFailed
	}

	r.mu.Lock()
	if r.stopped || r.role != Leader || r.term != term {
		r.mu.Unlock()
		return ErrNotLeader
	}
	if !r.mem.IsLearner(id) {
		// Removed (or already promoted) by someone else while we caught up.
		voter := r.mem.IsVoter(id)
		pending := r.cfgIndex
		r.mu.Unlock()
		if !voter {
			return ErrCatchUpFailed
		}
		return r.waitCommitted(ctx, pending, term)
	}
	next := r.mem.clone()
	next.Learners = without(next.Learners, id)
	next.Voters = append(next.Voters, id)
	idx, err := r.proposeConfigLocked(next)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return r.waitCommitted(ctx, idx, term)
}

// RemoveServer removes id (voter or learner). If id is this leader, it keeps
// leading, without counting itself, until the entry commits, then steps down
// (thesis §4.2.2). It returns once the entry commits.
func (r *Raft) RemoveServer(ctx context.Context, id NodeID) error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return ErrStopped
	}
	if r.role != Leader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	term := r.term
	if !r.mem.IsMember(id) {
		pending := r.cfgIndex
		r.mu.Unlock()
		return r.waitCommitted(ctx, pending, term) // already gone; make sure that is committed
	}
	next := r.mem.clone()
	next.Voters = without(next.Voters, id)
	next.Learners = without(next.Learners, id)
	delete(next.Addrs, id)
	if len(next.Voters) == 0 {
		r.mu.Unlock()
		return ErrLastVoter
	}
	idx, err := r.proposeConfigLocked(next)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return r.waitCommitted(ctx, idx, term)
}

// waitCommitted blocks until the entry at idx, proposed in term, is committed.
// Leadership loss (the term moved) means the outcome is unknown: ErrNotLeader.
// A leader that removed itself stays in term, so it still sees success.
func (r *Raft) waitCommitted(ctx context.Context, idx, term uint64) error {
	if idx == 0 {
		return nil
	}
	_, err := r.waitUntil(ctx, 0, func() (bool, error) {
		if r.term != term {
			return false, ErrNotLeader
		}
		if r.commitIndex >= idx {
			return true, nil
		}
		if r.role != Leader {
			return false, ErrNotLeader
		}
		return false, nil
	})
	return err
}

// waitUntil polls cond (called with mu held) every tick on the node's Clock
// until it reports true, errors, ctx ends, or (if timeout > 0) the timeout
// passes, in which case it returns false with no error.
func (r *Raft) waitUntil(ctx context.Context, timeout time.Duration, cond func() (bool, error)) (bool, error) {
	tk := r.clk.NewTicker(tickInterval)
	defer tk.Stop()
	var expire <-chan time.Time
	if timeout > 0 {
		t := r.clk.NewTimer(timeout)
		defer t.Stop()
		expire = t.C()
	}
	for {
		r.mu.Lock()
		stopped := r.stopped
		ok, err := false, error(nil)
		if !stopped {
			ok, err = cond()
		}
		r.mu.Unlock()
		if stopped {
			return false, ErrStopped
		}
		if err != nil || ok {
			return ok, err
		}
		select {
		case <-tk.C():
		case <-expire:
			return false, nil
		case <-ctx.Done():
			return false, ctx.Err()
		case <-r.ctx.Done():
			return false, ErrStopped
		}
	}
}
