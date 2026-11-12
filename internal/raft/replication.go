package raft

import "context"

// broadcastHeartbeats sends an empty AppendEntries to every peer that doesn't
// already have one outstanding. Caller holds mu. Phase 2 turns these into real
// replication.
func (r *Raft) broadcastHeartbeats() {
	r.nextHeartbeat = r.clk.Now().Add(heartbeatInterval)
	for _, p := range r.peers {
		if r.inflight[p] {
			continue
		}
		r.inflight[p] = true
		a := &AppendEntriesArgs{
			Term: r.term, LeaderID: r.id,
			PrevLogIndex: r.lastIndex(), PrevLogTerm: r.lastTerm(),
			LeaderCommit: r.commitIndex,
		}
		r.wg.Add(1)
		go r.heartbeatTo(p, a)
	}
}

func (r *Raft) heartbeatTo(p NodeID, a *AppendEntriesArgs) {
	defer r.wg.Done()
	reply, err := r.tr.Peer(p).AppendEntries(r.ctx, a) // no lock held
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inflight[p] = false
	if err != nil || r.stopped {
		return
	}
	if reply.Term > r.term {
		r.stepDown(reply.Term)
	}
}

// AppendEntries (Phase 1: heartbeat handling only). The log consistency check,
// truncation and commit index update arrive in Phase 2.
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
	r.leaderID = a.LeaderID
	r.resetElectionTimer() // valid reset: AppendEntries from the current leader
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
