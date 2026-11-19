package chaos

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/transport"
)

// ---- membership ----

const membershipTimeout = 8 * time.Second

func (e *Env) memberClient() *kv.Client {
	return kv.NewClient("chaos-admin", e.members, e.T.Caller(), e.T.Clock())
}

func (e *Env) withTimeout(d time.Duration, f func(ctx context.Context) error) error {
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	t := e.T.Clock().NewTimer(d)
	defer t.Stop()
	go func() {
		select {
		case <-t.C():
			cancel()
		case <-ctx.Done():
		}
	}()
	return f(ctx)
}

// nextNodeID names the next joiner: one past the highest numeric suffix seen.
func (e *Env) nextNodeID() raft.NodeID {
	if e.nextNode == 0 {
		for _, id := range e.T.Nodes() {
			if n, err := strconv.Atoi(strings.TrimPrefix(string(id), "n")); err == nil && n >= e.nextNode {
				e.nextNode = n + 1
			}
		}
	}
	id := raft.NodeID("n" + strconv.Itoa(e.nextNode))
	e.nextNode++
	return id
}

func (e *Env) setMembers(m []raft.NodeID) {
	e.members = m
	if e.onServers != nil {
		e.onServers(m)
	}
}

// membershipStep adds a fresh server (when the cluster has fewer than 5 and
// it is the add turn) or removes the oldest one. The member list is the
// script's own bookkeeping, so the schedule does not depend on whether a call
// succeeded.
func membershipStep(e *Env, add bool) {
	if add && len(e.members) < 5 {
		id := e.nextNodeID()
		addr, err := e.T.AddNode(id)
		if err != nil {
			e.Log("add-server", string(id), "", err)
			return
		}
		e.Log("add-server", string(id), "", nil)
		c := e.memberClient()
		err = e.withTimeout(membershipTimeout, func(ctx context.Context) error { return c.AddServer(ctx, id, addr) })
		e.Info("add-server-result", string(id), err)
		if err == nil {
			e.setMembers(append(append([]raft.NodeID(nil), e.members...), id))
		}
		return
	}
	if len(e.members) > 3 {
		victim := e.members[0]
		e.Log("remove-server", string(victim), "", nil)
		c := e.memberClient()
		err := e.withTimeout(membershipTimeout, func(ctx context.Context) error { return c.RemoveServer(ctx, victim) })
		e.Info("remove-server-result", string(victim), err)
		if err == nil {
			e.setMembers(append([]raft.NodeID(nil), e.members[1:]...))
		}
	}
}

// membershipChaos: every 20 s cycle, split the cluster at t+1, change the
// membership at t+2 (add on even cycles, remove on odd), heal at t+11.
func membershipChaos(e *Env) {
	for cycle, base := 0, time.Duration(0); ; cycle, base = cycle+1, base+20*time.Second {
		if !e.WaitUntil(base + time.Second) {
			return
		}
		maj, min := transport.SplitMajority(e.members, e.Rng)
		e.Log("partition", partitionDetail(maj, min), "", e.Cut(transport.Partition(maj, min)))
		if !e.WaitUntil(base + 2*time.Second) {
			return
		}
		membershipStep(e, cycle%2 == 0)
		if !e.WaitUntil(base + 11*time.Second) {
			return
		}
		e.Log("heal", "", "", e.Heal())
		if !e.WaitUntil(base + 14*time.Second) {
			return
		}
		membershipStep(e, cycle%2 == 1) // and again, healed
	}
}
