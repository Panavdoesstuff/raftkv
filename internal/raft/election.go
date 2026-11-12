package raft

import "context"

// startElection: become candidate, vote for ourselves, ask everyone else.
// Caller holds mu.
func (r *Raft) startElection() {
	r.role = Candidate
	r.term++
	r.votedFor = r.id
	r.leaderID = ""
	r.votes = 1
	r.resetElectionTimer() // valid reset: starting an election
	r.dbg("start election")
	// Phase 3: persist (term, votedFor) before any RequestVote leaves.

	if r.votes >= r.majority() { // single-node cluster
		r.becomeLeader()
		return
	}
	args := &RequestVoteArgs{
		Term: r.term, CandidateID: r.id,
		LastLogIndex: r.lastIndex(), LastLogTerm: r.lastTerm(),
	}
	for _, p := range r.peers {
		a := *args
		r.wg.Add(1)
		go r.requestVoteFrom(p, &a)
	}
}

func (r *Raft) requestVoteFrom(p NodeID, a *RequestVoteArgs) {
	defer r.wg.Done()
	reply, err := r.tr.Peer(p).RequestVote(r.ctx, a) // no lock held
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
	// Re-check after the unlocked RPC: we may have lost the election or moved on.
	if r.role != Candidate || r.term != a.Term || !reply.VoteGranted {
		return
	}
	r.votes++
	if r.votes >= r.majority() {
		r.becomeLeader()
	}
}

// becomeLeader: caller holds mu.
func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leaderID = r.id
	r.dbg("became leader")
	// Phase 2: initialise nextIndex/matchIndex and append the no-op entry.
	r.broadcastHeartbeats()
}

// RequestVote implements the receiver side of Figure 2.
func (r *Raft) RequestVote(_ context.Context, a *RequestVoteArgs) (*RequestVoteReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	if a.Term < r.term {
		return &RequestVoteReply{Term: r.term}, nil
	}
	if a.Term > r.term {
		r.stepDown(a.Term)
	}
	// Grant only if we haven't voted for someone else this term AND the
	// candidate's log is at least as up to date as ours (§5.4.1): higher last
	// term wins; on equal last terms the longer log wins.
	canVote := r.votedFor == "" || r.votedFor == a.CandidateID
	upToDate := a.LastLogTerm > r.lastTerm() ||
		(a.LastLogTerm == r.lastTerm() && a.LastLogIndex >= r.lastIndex())
	if !canVote || !upToDate {
		r.dbg("vote refused", "candidate", a.CandidateID, "canVote", canVote, "upToDate", upToDate)
		return &RequestVoteReply{Term: r.term}, nil
	}
	r.votedFor = a.CandidateID
	// Phase 3: persist (term, votedFor) before replying.
	r.resetElectionTimer() // valid reset: granting a vote
	r.dbg("vote granted", "candidate", a.CandidateID)
	return &RequestVoteReply{Term: r.term, VoteGranted: true}, nil
}
