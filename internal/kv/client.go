package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
)

// Caller sends one Command RPC to one server. An error means the outcome is
// unknown (request or reply lost, server down).
type Caller interface {
	Command(ctx context.Context, to raft.NodeID, a *kvpb.CommandArgs) (*kvpb.CommandReply, error)
}

const (
	attemptTimeout = 3 * time.Second
	backoffMin     = 10 * time.Millisecond
	backoffMax     = 250 * time.Millisecond
)

// Client has one outstanding request at a time. Every request carries
// (clientID, seq); retries reuse the same seq, so the servers apply it once.
// A Client must not be used from several goroutines at once.
type Client struct {
	id      string
	servers []raft.NodeID
	caller  Caller
	clk     clock.Clock

	seq    uint64
	leader int // index into servers: the one we try first

	// pending is a server list set by SetServers from another goroutine; the
	// client adopts it before its next attempt.
	pmu     sync.Mutex
	pending []raft.NodeID
}

func NewClient(id string, servers []raft.NodeID, c Caller, clk clock.Clock) *Client {
	return &Client{id: id, servers: append([]raft.NodeID(nil), servers...), caller: c, clk: clk}
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	r, err := c.do(ctx, kvpb.Op_GET, key, "")
	return r, err
}
func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, kvpb.Op_PUT, key, value)
	return err
}
func (c *Client) Append(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, kvpb.Op_APPEND, key, value)
	return err
}

// SetServers replaces the list of servers to try. Safe to call while the
// client is in use; it takes effect before the next attempt.
func (c *Client) SetServers(servers []raft.NodeID) {
	c.pmu.Lock()
	c.pending = append([]raft.NodeID(nil), servers...)
	c.pmu.Unlock()
}

func (c *Client) refresh() {
	c.pmu.Lock()
	p := c.pending
	c.pending = nil
	c.pmu.Unlock()
	if len(p) > 0 {
		c.servers, c.leader = p, 0
	}
}

// do retries until the request is applied or ctx ends. It never gives up on
// its own: a timed-out attempt may have succeeded, so the only safe answer is
// to ask again with the same seq.
func (c *Client) do(ctx context.Context, op kvpb.Op, key, value string) (string, error) {
	c.seq++
	args := &kvpb.CommandArgs{Op: op, Key: key, Value: value, ClientId: c.id, Seq: c.seq}
	backoff := backoffMin
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		c.refresh()
		rep := c.attempt(ctx, args)
		if rep != nil {
			switch rep.Err {
			case kvpb.Err_OK, kvpb.Err_ERR_NO_KEY:
				return rep.Value, nil // NO_KEY: the key doesn't exist, value is ""
			case kvpb.Err_ERR_WRONG_LEADER:
				if h := c.indexOf(rep.LeaderHint); h >= 0 && h != c.leader {
					c.leader = h // follow the hint immediately
					continue
				}
				c.leader = (c.leader + 1) % len(c.servers)
			case kvpb.Err_ERR_TIMEOUT:
				// Same server may still be leader, just slow; retry it.
			}
		} else {
			c.leader = (c.leader + 1) % len(c.servers)
		}
		if err := c.sleep(ctx, backoff); err != nil {
			return "", err
		}
		if backoff *= 2; backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

// attempt makes one RPC bounded by attemptTimeout on the client's Clock.
func (c *Client) attempt(ctx context.Context, args *kvpb.CommandArgs) *kvpb.CommandReply {
	actx, cancel := context.WithCancel(ctx)
	defer cancel()
	t := c.clk.NewTimer(attemptTimeout)
	defer t.Stop()
	go func() {
		select {
		case <-t.C():
			cancel()
		case <-actx.Done():
		}
	}()
	rep, err := c.caller.Command(actx, c.servers[c.leader], args)
	if err != nil {
		return nil
	}
	return rep
}

func (c *Client) indexOf(id string) int {
	for i, s := range c.servers {
		if string(s) == id {
			return i
		}
	}
	return -1
}

func (c *Client) sleep(ctx context.Context, d time.Duration) error {
	t := c.clk.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MembershipCaller is the optional second half of a Caller: the membership
// RPCs. Client.AddServer and RemoveServer need the Caller to implement it.
type MembershipCaller interface {
	AddServer(ctx context.Context, to raft.NodeID, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error)
	RemoveServer(ctx context.Context, to raft.NodeID, a *kvpb.MembershipArgs) (*kvpb.MembershipReply, error)
}

// ErrMembershipFailed is a membership change the cluster refused for good
// (the new server could not catch up, the last voter cannot be removed, ...).
var ErrMembershipFailed = errors.New("kv: membership change failed")

// membershipAttemptTimeout bounds one AddServer attempt: catch-up takes up to
// ten election timeouts, then a commit. A timed-out attempt is retried, and
// the retry resumes (the server side is idempotent).
const membershipAttemptTimeout = 15 * time.Second

// AddServer adds id (reachable at addr) to the cluster as a voter. It retries
// until the change commits or ctx ends. On success the client also starts
// using id as a server.
func (c *Client) AddServer(ctx context.Context, id raft.NodeID, addr string) error {
	err := c.membership(ctx, func(mc MembershipCaller, actx context.Context, to raft.NodeID) (*kvpb.MembershipReply, error) {
		return mc.AddServer(actx, to, &kvpb.MembershipArgs{Id: string(id), Addr: addr})
	})
	if err == nil && c.indexOf(string(id)) < 0 {
		c.servers = append(c.servers, id)
	}
	return err
}

// RemoveServer removes id from the cluster. It retries until the change
// commits or ctx ends, and stops using id as a server.
func (c *Client) RemoveServer(ctx context.Context, id raft.NodeID) error {
	err := c.membership(ctx, func(mc MembershipCaller, actx context.Context, to raft.NodeID) (*kvpb.MembershipReply, error) {
		return mc.RemoveServer(actx, to, &kvpb.MembershipArgs{Id: string(id)})
	})
	if err == nil {
		if i := c.indexOf(string(id)); i >= 0 && len(c.servers) > 1 {
			c.servers = append(c.servers[:i:i], c.servers[i+1:]...)
			c.leader = 0
		}
	}
	return err
}

func (c *Client) membership(ctx context.Context, call func(MembershipCaller, context.Context, raft.NodeID) (*kvpb.MembershipReply, error)) error {
	mc, ok := c.caller.(MembershipCaller)
	if !ok {
		return errors.New("kv: caller does not support membership changes")
	}
	backoff := backoffMin
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.refresh()
		actx, cancel := context.WithCancel(ctx)
		t := c.clk.NewTimer(membershipAttemptTimeout)
		go func() {
			select {
			case <-t.C():
				cancel()
			case <-actx.Done():
			}
		}()
		rep, err := call(mc, actx, c.servers[c.leader])
		t.Stop()
		cancel()
		if err != nil {
			rep = nil
		}
		if rep == nil {
			c.leader = (c.leader + 1) % len(c.servers)
		} else {
			switch rep.Err {
			case kvpb.Err_OK:
				return nil
			case kvpb.Err_ERR_FAILED:
				return fmt.Errorf("%w: %s", ErrMembershipFailed, rep.Message)
			case kvpb.Err_ERR_WRONG_LEADER:
				if h := c.indexOf(rep.LeaderHint); h >= 0 && h != c.leader {
					c.leader = h
					continue
				}
				c.leader = (c.leader + 1) % len(c.servers)
			default: // CONFIG_BUSY, TIMEOUT: same leader, try again shortly
			}
		}
		if err := c.sleep(ctx, backoff); err != nil {
			return err
		}
		if backoff *= 2; backoff > backoffMax {
			backoff = backoffMax
		}
	}
}
