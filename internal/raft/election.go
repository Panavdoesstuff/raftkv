package raft

import "context"

// Elections run in two phases (thesis §9.6, PreVote). Before raising its term a
// follower sends PreVote requests for term+1 and only starts a real election if
// a quorum says it would grant a vote. PreVote changes no state on either side,
// so a node that cannot win (partitioned, removed from the cluster, or with a
// stale log) never inflates its term and never forces a healthy leader to step
// down when it reappears.

// startPreVote asks every voter whether it would vote for us in term+1. Caller
// holds mu.
func (r *Raft) startPreVote() {
	r.resetElectionTimer() // valid reset: starting an election
	r.dbg("start prevote")
	r.preEpoch++
	r.preActive = true
	r.preGrants = map[NodeID]bool{}
	if r.quorumCount(func(NodeID) bool { return false }) >= r.majority() { // single-voter cluster
		r.preActive = false
		r.startElection()
		return
	}
	args := &RequestVoteArgs{
		Term: r.term + 1, CandidateID: r.id,
		LastLogIndex: r.lastIndex(), LastLogTerm: r.lastTerm(),
		PreVote: true,
	}
	for _, p := range r.voterPeers {
		a := *args
		r.wg.Add(1)
		go r.preVoteFrom(p, &a, r.preEpoch)
	}
}

func (r *Raft) preVoteFrom(p NodeID, a *RequestVoteArgs, epoch uint64) {
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
		r.stepDown(reply.Term) // someone is ahead of us: adopt their term, stay a follower
		if r.persistHardState() != nil {
			return
		}
		return
	}
	// Re-check after the unlocked RPC: the round may be over (a leader
	// appeared, or we started the real election).
	if !r.preActive || r.preEpoch != epoch || r.term+1 != a.Term || r.role == Leader || !reply.VoteGranted {
		return
	}
	r.preGrants[p] = true
	if r.quorumCount(func(p NodeID) bool { return r.preGrants[p] }) >= r.majority() {
		r.preActive = false
		r.startElection()
	}
}

// startElection: become candidate, vote for ourselves, ask the other voters.
// Caller holds mu.
func (r *Raft) startElection() {
	r.role = Candidate
	r.term++
	r.votedFor = r.id
	r.leaderID = ""
	r.voteGranted = map[NodeID]bool{}
	r.resetElectionTimer() // valid reset: starting an election
	r.dbg("start election")
	// Our own vote must be durable before any RequestVote leaves, or a restart
	// could vote again in this term for someone else.
	if r.persistHardState() != nil {
		return
	}

	if r.quorumCount(func(NodeID) bool { return false }) >= r.majority() { // single-voter cluster
		r.becomeLeader()
		return
	}
	args := &RequestVoteArgs{
		Term: r.term, CandidateID: r.id,
		LastLogIndex: r.lastIndex(), LastLogTerm: r.lastTerm(),
	}
	for _, p := range r.voterPeers {
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
	r.voteGranted[p] = true
	if r.quorumCount(func(p NodeID) bool { return r.voteGranted[p] }) >= r.majority() {
		r.becomeLeader()
	}
}

// becomeLeader: caller holds mu.
func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leaderID = r.id
	r.dbg("became leader")
	r.initLeaderState()
	// The no-op lets us commit entries from earlier terms (Figure 8) and
	// learn the commit point without waiting for a client write.
	r.appendLocal(Entry{Term: r.term, Type: EntryNoop})
	if r.stopped { // storage failed
		return
	}
	r.advanceCommit() // single-node cluster
	r.broadcastHeartbeats()
}

// recentLeader reports whether we have reason to believe a leader is alive:
// either we lead with a recent majority of acks, or we heard from one within
// the minimum election timeout. Vote and PreVote requests are ignored then
// (thesis §4.2.3), so a node that was removed, or lost contact with the
// leader, cannot depose a healthy leader. A leader that has lost its majority
// is not "recent", so it does not block a rival it can no longer outvote.
// Caller holds mu.
func (r *Raft) recentLeader() bool {
	now := r.clk.Now()
	if r.role == Leader {
		ack, ok := r.lastMajorityAck()
		return ok && now.Before(ack.Add(electionTimeoutMin))
	}
	return !r.heardLeader.IsZero() && now.Before(r.heardLeader.Add(electionTimeoutMin))
}

// RequestVote implements the receiver side of Figure 2, plus PreVote.
func (r *Raft) RequestVote(_ context.Context, a *RequestVoteArgs) (*RequestVoteReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	var reply *RequestVoteReply
	if a.PreVote {
		reply = r.handlePreVote(a) // changes no state, so nothing to persist
		return reply, nil
	}
	reply = r.handleRequestVote(a)
	// Persist first: the reply (a vote, or news of our term) is a promise that
	// must survive a crash. This also covers a refusal that raised our term.
	if err := r.persistHardState(); err != nil {
		return nil, err
	}
	return reply, nil
}

// handlePreVote answers "would you vote for me in a.Term?" without changing
// anything: no term bump, no votedFor, no timer reset.
func (r *Raft) handlePreVote(a *RequestVoteArgs) *RequestVoteReply {
	deny := &RequestVoteReply{Term: r.term}
	if a.Term <= r.term || r.recentLeader() {
		return deny
	}
	if !r.logUpToDate(a) {
		return deny
	}
	return &RequestVoteReply{Term: r.term, VoteGranted: true}
}

// logUpToDate is the §5.4.1 check: higher last term wins; on equal last terms
// the longer log wins.
func (r *Raft) logUpToDate(a *RequestVoteArgs) bool {
	return a.LastLogTerm > r.lastTerm() ||
		(a.LastLogTerm == r.lastTerm() && a.LastLogIndex >= r.lastIndex())
}

func (r *Raft) handleRequestVote(a *RequestVoteArgs) *RequestVoteReply {
	if a.Term < r.term {
		return &RequestVoteReply{Term: r.term}
	}
	if a.Term > r.term && r.recentLeader() {
		// Ignore entirely: do not even adopt the higher term (thesis §4.2.3).
		r.dbg("vote request ignored: leader is alive", "candidate", a.CandidateID)
		return &RequestVoteReply{Term: r.term}
	}
	if a.Term > r.term {
		r.stepDown(a.Term)
	}
	// Grant only if we haven't voted for someone else this term AND the
	// candidate's log is at least as up to date as ours.
	canVote := r.votedFor == "" || r.votedFor == a.CandidateID
	upToDate := r.logUpToDate(a)
	if !canVote || !upToDate {
		r.dbg("vote refused", "candidate", a.CandidateID, "canVote", canVote, "upToDate", upToDate)
		return &RequestVoteReply{Term: r.term}
	}
	r.votedFor = a.CandidateID
	r.resetElectionTimer() // valid reset: granting a vote
	r.dbg("vote granted", "candidate", a.CandidateID)
	return &RequestVoteReply{Term: r.term, VoteGranted: true}
}
