package kv

import (
	"context"
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
