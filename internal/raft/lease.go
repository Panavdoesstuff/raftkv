package raft

import (
	"context"
	"sort"
	"time"
)

// leaseMargin is subtracted from the lease to leave room for clock drift
// between this node and its followers. It buys safety only against drift
// smaller than margin/electionTimeoutMin (here about 13%); beyond that, lease
// reads can be stale (TestLeaseReadSkew shows it).
const leaseMargin = 20 * time.Millisecond

// LeaseRead serves a read without a heartbeat round, if the leader's lease is
// valid: now < lastMajorityAck + electionTimeoutMin - leaseMargin, all
// measured on this node's own Clock.
//
// Why that is safe when clocks run at the same speed: a follower that acked
// our AppendEntries sent at T will not vote for a rival before T +
// electionTimeoutMin on its clock, so no other leader can exist before then.
// lastMajorityAck is the time the round was SENT, not when acks arrived: the
// follower's promise starts no earlier than the send, and a slow reply would
// otherwise stretch the lease past what the followers promised.
//
// It returns ErrLeaseExpired when the lease has lapsed (the caller falls back
// to ReadIndex) and, like ReadIndex, waits for lastApplied >= the returned
// commit index.
func (r *Raft) LeaseRead(ctx context.Context) (uint64, error) {
	r.mu.Lock()
	idx, err := r.readableIndex()
	if err == nil && !r.leaseValid() {
		err = ErrLeaseExpired
	}
	r.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return idx, r.waitApplied(ctx, idx)
}

// leaseValid reports whether the lease holds right now. Caller holds mu.
func (r *Raft) leaseValid() bool {
	ack, ok := r.lastMajorityAck()
	if !ok {
		return false
	}
	return r.clk.Now().Before(ack.Add(electionTimeoutMin - leaseMargin))
}

// lastMajorityAck is the latest instant T such that a majority (counting
// ourselves) acked an AppendEntries sent at or after T. Caller holds mu.
func (r *Raft) lastMajorityAck() (time.Time, bool) {
	need := r.majority() - 1 // acks needed from peers
	if need == 0 {
		return r.clk.Now(), true
	}
	ts := make([]time.Time, 0, len(r.peers))
	for _, p := range r.peers {
		ts = append(ts, r.ackSent[p])
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i].After(ts[j]) })
	t := ts[need-1]
	return t, !t.IsZero()
}
