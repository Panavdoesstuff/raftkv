package raft

// Test-only hooks for scripted scenarios (Figure 8) that need a leader in an
// exact state without running an election.

// ForceLeader makes r the leader of its current term without appending a
// no-op or sending anything, as if it had just won an election.
func (r *Raft) ForceLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.role = Leader
	r.leaderID = r.id
	r.initLeaderState()
}

// SetMatch records that peer p is known to hold the leader's log through idx.
func (r *Raft) SetMatch(p NodeID, idx uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.matchIndex[p] = idx
}

// AdvanceCommit runs the leader's commit rule.
func (r *Raft) AdvanceCommit() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advanceCommit()
}
