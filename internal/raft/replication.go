package raft

import "context"

// maxBatch caps the entries in one AppendEntries.
const maxBatch = 256

// broadcastHeartbeats sends every peer an AppendEntries carrying whatever it
// is missing (empty when it is up to date), and re-arms the heartbeat timer.
// Caller holds mu. A new RPC goes out even if an earlier one is still in
// flight; replies are therefore handled idempotently (see replicateTo).
func (r *Raft) broadcastHeartbeats() {
	r.nextHeartbeat = r.clk.Now().Add(heartbeatInterval)
	for _, p := range r.peers {
		r.sendAppend(p)
	}
}

// sendAppend builds and launches one AppendEntries for p. Caller holds mu.
func (r *Raft) sendAppend(p NodeID) {
	next := r.nextIndex[p]
	a := &AppendEntriesArgs{
		Term: r.term, LeaderID: r.id,
		PrevLogIndex: next - 1, PrevLogTerm: r.termAt(next - 1),
		Entries:      CloneEntries(r.entriesFrom(next, maxBatch)),
		LeaderCommit: r.commitIndex,
	}
	r.wg.Add(1)
	go r.replicateTo(p, a)
}

func (r *Raft) replicateTo(p NodeID, a *AppendEntriesArgs) {
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
	for _, p := range r.peers {
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
		count := 1 // ourselves
		for _, p := range r.peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count >= r.majority() {
			r.commitIndex = n
			r.notifyApply()
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
		if err := r.persistAppend(added); err != nil { // durable before we ack
			return nil, err
		}
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

// InstallSnapshot arrives in Phase 5.
func (r *Raft) InstallSnapshot(_ context.Context, a *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	return &InstallSnapshotReply{Term: r.term}, nil
}
