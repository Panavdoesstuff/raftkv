package raft

import (
	"context"
	"time"
)

// maxBatch caps the entries in one AppendEntries.
const maxBatch = 256

// broadcastHeartbeats sends every peer an AppendEntries carrying whatever it
// is missing (empty when it is up to date), and re-arms the heartbeat timer.
// Caller holds mu. A new RPC goes out even if an earlier one is still in
// flight; replies are therefore handled idempotently (see replicateTo).
func (r *Raft) broadcastHeartbeats() {
	r.sendSeq++
	r.nextHeartbeat = r.clk.Now().Add(heartbeatInterval)
	for _, p := range r.replPeers {
		r.sendAppend(p)
	}
}

// sendAppend builds and launches one AppendEntries for p. Caller holds mu.
func (r *Raft) sendAppend(p NodeID) {
	next := r.nextIndex[p]
	if next <= r.snapIndex {
		// The entries p needs are gone (compacted): ship the snapshot instead.
		r.sendSnapshot(p)
		return
	}
	seq, sentAt := r.sendSeq, r.clk.Now() // tags for read confirmation and leases
	a := &AppendEntriesArgs{
		Term: r.term, LeaderID: r.id,
		PrevLogIndex: next - 1, PrevLogTerm: r.termAt(next - 1),
		Entries:      CloneEntries(r.entriesFrom(next, maxBatch)),
		LeaderCommit: r.commitIndex,
	}
	r.wg.Add(1)
	go r.replicateTo(p, a, seq, sentAt)
}

func (r *Raft) replicateTo(p NodeID, a *AppendEntriesArgs, seq uint64, sentAt time.Time) {
	defer r.wg.Done()
	reply, err := r.tr.Peer(p).AppendEntries(r.ctx, a) // no lock held
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	if reply.Term > r.term {
		r.stepDown(reply.Term)
		return
	}
	// The world may have moved on while we were unlocked, and replies can
	// arrive late or out of order: act only if we still lead the same term.
	if r.role != Leader || r.term != a.Term {
		return
	}
	// Success or not, a reply in our term means the follower accepted us as
	// leader: that confirms leadership for reads and extends the lease.
	r.recordAck(p, seq, sentAt)
	if reply.Success {
		// matchIndex only moves forward, so a stale reply is harmless.
		if m := a.PrevLogIndex + uint64(len(a.Entries)); m > r.matchIndex[p] {
			r.matchIndex[p] = m
		}
		if r.matchIndex[p]+1 > r.nextIndex[p] {
			r.nextIndex[p] = r.matchIndex[p] + 1
		}
		r.advanceCommit()
		if r.nextIndex[p] <= r.lastIndex() { // more to send: don't wait for the next heartbeat
			r.sendAppend(p)
		}
		return
	}
	// Rejected: back up using the follower's hint (Raft paper §5.3, the
	// optimisation at the end), never below what we know it already holds.
	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		if i := r.lastIndexOfTerm(reply.ConflictTerm); i != 0 {
			next = i + 1 // we have that term: resume after our last entry in it
		}
	}
	if next < 1 {
		next = 1
	}
	if next <= r.matchIndex[p] {
		next = r.matchIndex[p] + 1
	}
	if a.PrevLogIndex+1 != r.nextIndex[p] {
		return // reply to a probe we have already moved past
	}
	r.nextIndex[p] = next
	r.sendAppend(p)
}

// initLeaderState resets per-follower replication state on taking office
// (Figure 2, "Volatile state on leaders"). Caller holds mu.
func (r *Raft) initLeaderState() {
	r.nextIndex = map[NodeID]uint64{}
	r.matchIndex = map[NodeID]uint64{}
	r.snapSending = map[NodeID]bool{}
	r.sendSeq = 0
	r.ackSeq = map[NodeID]uint64{}
	r.ackSent = map[NodeID]time.Time{}
	r.readWaiters = nil
	for _, p := range r.replPeers {
		r.nextIndex[p] = r.lastIndex() + 1
	}
}

// advanceCommit moves commitIndex to the highest N replicated on a majority
// with log[N].term == currentTerm. Entries from earlier terms are never
// committed by counting replicas; they commit indirectly when a current-term
// entry after them does (Raft paper §5.4.2, Figure 8). Caller holds mu.
func (r *Raft) advanceCommit() {
	for n := r.lastIndex(); n > r.commitIndex; n-- {
		if r.termAt(n) != r.term {
			break // terms are non-decreasing: nothing lower is from this term either
		}
		// Only current voters count; a leader that is not a voter (it removed
		// itself) is not in its own majority.
		if r.quorumCount(func(p NodeID) bool { return r.matchIndex[p] >= n }) >= r.majority() {
			r.commitIndex = n
			r.notifyApply()
			r.maybeStepDownRemoved()
			return
		}
	}
}

// Propose appends cmd to the log if this node is the leader. A true result
// means the entry was accepted at (index, term), not that it will commit.
func (r *Raft) Propose(cmd []byte) (index, term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || r.role != Leader {
		return 0, 0, false
	}
	index = r.appendLocal(Entry{Term: r.term, Type: EntryNormal, Data: append([]byte(nil), cmd...)})
	if r.stopped { // storage failed: the entry is not durable
		return 0, 0, false
	}
	r.advanceCommit() // single-node cluster
	r.broadcastHeartbeats()
	return index, r.term, true
}

// appendLocal appends e to our log, durably, and returns its index. Durable
// before replication matters because advanceCommit counts the leader's own
// copy toward the majority. If the write fails the node halts (r.stopped).
// Caller holds mu.
func (r *Raft) appendLocal(e Entry) uint64 {
	e.Index = r.lastIndex() + 1
	r.entries = append(r.entries, e)
	r.logBytes += entrySize(e)
	r.persistAppend([]Entry{e})
	return e.Index
}

// AppendEntries is the receiver side of Figure 2.
func (r *Raft) AppendEntries(_ context.Context, a *AppendEntriesArgs) (*AppendEntriesReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	if a.Term < r.term {
		return &AppendEntriesReply{Term: r.term}, nil
	}
	if a.Term > r.term {
		r.stepDown(a.Term)
	} else if r.role == Candidate {
		r.role = Follower // someone else won this term
	} else if r.role == Leader {
		// Two leaders in one term would break Election Safety; never accept.
		r.dbg("AppendEntries from another leader in our term", "from", a.LeaderID)
		return &AppendEntriesReply{Term: r.term}, nil
	}
	// A newer term must be durable before we reply or touch the log.
	if err := r.persistHardState(); err != nil {
		return nil, err
	}
	r.leaderID = a.LeaderID
	r.heardLeader = r.clk.Now()
	r.preActive = false
	r.resetElectionTimer() // valid reset: AppendEntries from the current leader

	// Consistency check: we must hold an entry at PrevLogIndex with PrevLogTerm.
	if a.PrevLogIndex > r.lastIndex() {
		return &AppendEntriesReply{Term: r.term, ConflictIndex: r.lastIndex() + 1}, nil
	}
	if a.PrevLogIndex >= r.snapIndex && r.termAt(a.PrevLogIndex) != a.PrevLogTerm {
		ct := r.termAt(a.PrevLogIndex)
		return &AppendEntriesReply{Term: r.term, ConflictTerm: ct, ConflictIndex: r.firstIndexOfTerm(ct)}, nil
	}

	// Append, truncating only from the first entry that conflicts. Entries
	// that already match are left alone: a delayed or duplicated RPC carrying
	// a prefix of our log must not erase newer entries (Figure 2, rule 3).
	for i, e := range a.Entries {
		idx := a.PrevLogIndex + 1 + uint64(i)
		if idx <= r.snapIndex {
			continue // already covered by our snapshot
		}
		if idx <= r.lastIndex() {
			if r.termAt(idx) == e.Term {
				continue
			}
			if err := r.truncateFrom(idx); err != nil {
				return nil, err
			}
		}
		added := CloneEntries(a.Entries[i:])
		r.entries = append(r.entries, added...)
		for _, e := range added {
			r.logBytes += entrySize(e)
		}
		if err := r.persistAppend(added); err != nil { // durable before we ack
			return nil, err
		}
		r.noteConfigEntries(added) // a config takes effect when appended
		break
	}

	// Commit up to the last entry this RPC vouches for, never past it: we may
	// hold extra, unverified entries beyond that. commitIndex never moves back.
	if last := a.PrevLogIndex + uint64(len(a.Entries)); a.LeaderCommit > r.commitIndex {
		c := a.LeaderCommit
		if last < c {
			c = last
		}
		if c > r.commitIndex {
			r.commitIndex = c
			r.notifyApply()
		}
	}
	return &AppendEntriesReply{Term: r.term, Success: true}, nil
}

// sendSnapshot launches an InstallSnapshot to p unless one is already in
// flight (heartbeats would otherwise pile up copies of a large snapshot).
// Caller holds mu.
func (r *Raft) sendSnapshot(p NodeID) {
	if r.snapSending[p] {
		return
	}
	r.snapSending[p] = true
	seq, sentAt := r.sendSeq, r.clk.Now()
	a := &InstallSnapshotArgs{
		Term: r.term, LeaderID: r.id,
		LastIncludedIndex: r.snapIndex, LastIncludedTerm: r.snapTerm,
		Config: EncodeConfig(r.baseCfg),
		Data:   r.snapData, // immutable once set; transports copy what they need
	}
	r.wg.Add(1)
	go r.installTo(p, a, seq, sentAt)
}

func (r *Raft) installTo(p NodeID, a *InstallSnapshotArgs, seq uint64, sentAt time.Time) {
	defer r.wg.Done()
	reply, err := r.tr.Peer(p).InstallSnapshot(r.ctx, a) // no lock held
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.snapSending, p)
	if err != nil || r.stopped {
		return
	}
	if reply.Term > r.term {
		r.stepDown(reply.Term)
		return
	}
	if r.role != Leader || r.term != a.Term {
		return
	}
	r.recordAck(p, seq, sentAt)
	// The follower now holds everything through LastIncludedIndex (it either
	// installed the snapshot or already had that much committed).
	if a.LastIncludedIndex > r.matchIndex[p] {
		r.matchIndex[p] = a.LastIncludedIndex
	}
	if r.matchIndex[p]+1 > r.nextIndex[p] {
		r.nextIndex[p] = r.matchIndex[p] + 1
	}
	r.advanceCommit()
	if r.nextIndex[p] <= r.lastIndex() {
		r.sendAppend(p)
	}
}

// InstallSnapshot is the receiver side (Figure 13 of the paper, sent whole
// rather than in chunks). Rules:
//   - older term: reject. Otherwise it is from the current leader, so reset
//     the election timer like AppendEntries does.
//   - a snapshot at or below our commitIndex tells us nothing new: ignore it
//     (installing it would roll the state machine back).
//   - if we hold an entry at LastIncludedIndex with LastIncludedTerm, the log
//     after it is consistent with the leader's: keep it. Otherwise discard the
//     whole log.
//   - persist before replying, then let the applier restore the state machine.
//     lastApplied only moves when the applier has done so, which keeps the
//     apply stream ordered and exactly-once across the jump.
func (r *Raft) InstallSnapshot(_ context.Context, a *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	if a.Term < r.term {
		return &InstallSnapshotReply{Term: r.term}, nil
	}
	if a.Term > r.term {
		r.stepDown(a.Term)
	} else if r.role == Candidate {
		r.role = Follower
	} else if r.role == Leader {
		r.dbg("InstallSnapshot from another leader in our term", "from", a.LeaderID)
		return &InstallSnapshotReply{Term: r.term}, nil
	}
	if err := r.persistHardState(); err != nil {
		return nil, err
	}
	r.leaderID = a.LeaderID
	r.heardLeader = r.clk.Now()
	r.preActive = false
	r.resetElectionTimer()

	idx := a.LastIncludedIndex
	if idx <= r.commitIndex {
		return &InstallSnapshotReply{Term: r.term}, nil
	}
	data := append([]byte(nil), a.Data...)
	if !(idx <= r.lastIndex() && r.termAt(idx) == a.LastIncludedTerm) {
		// Our log does not contain the snapshot's last entry: whatever follows
		// idx is on a diverging branch. Drop it on disk first, while the
		// entries through idx are still there, so a crash before the snapshot
		// is saved leaves a valid (shorter) log rather than a gap or a log
		// whose suffix does not belong to its prefix.
		if err := r.truncateFrom(idx + 1); err != nil {
			return nil, err
		}
	}
	if err := r.cfg.Storage.SaveSnapshot(Snapshot{LastIncludedIndex: idx, LastIncludedTerm: a.LastIncludedTerm, Data: data, Config: a.Config}); err != nil {
		return nil, r.halt(err)
	}
	cfg := r.baseCfg
	if len(a.Config) > 0 {
		var err error
		if cfg, err = DecodeConfig(a.Config); err != nil {
			return nil, r.halt(err)
		}
	}
	r.compactTo(idx, a.LastIncludedTerm, data, cfg)
	r.refreshConfig() // the log suffix we kept may hold newer config entries
	r.commitIndex = idx
	r.pendingSnap = true
	r.notifyApply()
	r.dbg("snapshot installed", "index", idx)
	return &InstallSnapshotReply{Term: r.term}, nil
}
