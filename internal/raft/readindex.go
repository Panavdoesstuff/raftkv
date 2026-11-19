package raft

import (
	"context"
	"errors"
	"time"
)

// Read errors. ErrNotLeader and ErrLeaseExpired mean "ask someone else (or
// retry)"; ErrNotReady means this node leads but has not yet committed an
// entry of its own term, so it cannot know the commit point.
var (
	ErrNotLeader    = errors.New("raft: not the leader")
	ErrNotReady     = errors.New("raft: leader has not committed an entry in its term yet")
	ErrLeaseExpired = errors.New("raft: leader lease expired")
)

// readWaiter is one ReadIndex call between "readIndex recorded" and "a
// majority confirmed we were still leader".
type readWaiter struct {
	// need is the heartbeat sequence number the round must have: only
	// AppendEntries sent by a broadcast numbered >= need were sent after this
	// read arrived, so only their acks prove we led at some point after it.
	need      uint64
	confirmed chan struct{} // closed on confirmation or failure
	err       error         // set before confirmed is closed; nil on success
}

// ReadIndex returns an index such that a read of the state machine that has
// applied at least that index is linearizable (thesis §6.4). Steps:
//
//  1. The leader must have committed an entry of its own term (the no-op), or
//     it cannot know the true commit point: ErrNotReady.
//  2. readIndex = commitIndex.
//  3. Confirm leadership: a heartbeat round sent after this call, acked by a
//     majority. Without it a deposed leader that has not noticed yet would
//     serve stale data. Concurrent reads share rounds (see needSeq below).
//  4. Wait until lastApplied >= readIndex. The caller then reads locally.
//
// It returns ErrNotLeader if this node is not (or stops being) the leader
// before confirmation. Time never enters: the round is counted by sequence
// number, and the wait is bounded only by ctx.
func (r *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	r.mu.Lock()
	idx, w, err := r.startRead()
	r.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if w != nil {
		select {
		case <-w.confirmed:
			if w.err != nil {
				return 0, w.err
			}
		case <-ctx.Done():
			r.mu.Lock()
			r.dropRead(w)
			r.mu.Unlock()
			return 0, ctx.Err()
		case <-r.ctx.Done():
			return 0, ErrStopped
		}
	}
	return idx, r.waitApplied(ctx, idx)
}

// startRead runs steps 1 and 2 and registers the read for step 3. A nil
// waiter means leadership is already confirmed (a single-node cluster).
// Caller holds mu.
func (r *Raft) startRead() (uint64, *readWaiter, error) {
	idx, err := r.readableIndex()
	if err != nil {
		return 0, nil, err
	}
	if r.quorumCount(func(NodeID) bool { return false }) >= r.majority() { // we alone are a quorum
		return idx, nil, nil
	}
	w := &readWaiter{need: r.sendSeq + 1, confirmed: make(chan struct{})}
	// A round already out for an earlier read was sent before this read
	// arrived, so it cannot confirm this one. If one is outstanding, wait for
	// the next broadcast (triggered when it completes, or by the heartbeat
	// timer) so that all reads arriving meanwhile share it; otherwise start
	// one now.
	busy := r.readRoundOutstanding()
	r.readWaiters = append(r.readWaiters, w)
	if !busy {
		r.broadcastHeartbeats() // sendSeq becomes w.need
	}
	return idx, w, nil
}

// readableIndex checks leadership and the current-term commit (steps 1-2) and
// returns commitIndex. Caller holds mu.
func (r *Raft) readableIndex() (uint64, error) {
	if r.stopped {
		return 0, ErrStopped
	}
	if r.role != Leader {
		return 0, ErrNotLeader
	}
	// commitIndex may still be an inherited value from before our term: the
	// entries after it that a previous leader committed are invisible to us
	// until our no-op commits (Figure 8 / DESIGN Q6).
	if r.termAt(r.commitIndex) != r.term {
		return 0, ErrNotReady
	}
	return r.commitIndex, nil
}

// readRoundOutstanding reports whether a round that a pending read depends on
// has been sent and not yet completed. Caller holds mu.
func (r *Raft) readRoundOutstanding() bool {
	for _, w := range r.readWaiters {
		if w.need <= r.sendSeq {
			return true
		}
	}
	return false
}

// checkReads confirms every read whose round has a majority of acks, then
// starts the next round if reads queued behind the one that just finished.
// Caller holds mu.
func (r *Raft) checkReads() {
	if len(r.readWaiters) == 0 {
		return
	}
	kept := r.readWaiters[:0]
	for _, w := range r.readWaiters {
		if r.quorumCount(func(p NodeID) bool { return r.ackSeq[p] >= w.need }) >= r.majority() {
			close(w.confirmed)
		} else {
			kept = append(kept, w)
		}
	}
	r.readWaiters = kept
	if len(kept) > 0 && !r.readRoundOutstanding() {
		r.broadcastHeartbeats()
	}
}

// failReads wakes every unconfirmed read with err. Caller holds mu.
func (r *Raft) failReads(err error) {
	for _, w := range r.readWaiters {
		w.err = err
		close(w.confirmed)
	}
	r.readWaiters = nil
}

// dropRead removes a cancelled read. Caller holds mu.
func (r *Raft) dropRead(w *readWaiter) {
	for i, x := range r.readWaiters {
		if x == w {
			r.readWaiters = append(r.readWaiters[:i], r.readWaiters[i+1:]...)
			return
		}
	}
}

// recordAck notes that p answered an AppendEntries (or InstallSnapshot) of our
// current term that carried heartbeat sequence seq and was sent at sentAt on
// our Clock. Any reply in our term proves p recognised us as leader at some
// instant after the send. Caller holds mu.
func (r *Raft) recordAck(p NodeID, seq uint64, sentAt time.Time) {
	if seq > r.ackSeq[p] {
		r.ackSeq[p] = seq
	}
	if sentAt.After(r.ackSent[p]) {
		r.ackSent[p] = sentAt
	}
	r.checkReads()
}

// waitApplied blocks until lastApplied >= idx (step 4).
func (r *Raft) waitApplied(ctx context.Context, idx uint64) error {
	for {
		r.mu.Lock()
		if r.lastApplied >= idx {
			r.mu.Unlock()
			return nil
		}
		ch := r.appliedCh
		r.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-r.ctx.Done():
			return ErrStopped
		}
	}
}

// signalApplied wakes waitApplied callers. Closing a channel never blocks, so
// this is safe under mu. Caller holds mu.
func (r *Raft) signalApplied() {
	close(r.appliedCh)
	r.appliedCh = make(chan struct{})
}
