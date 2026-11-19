// Package history records what clients did, in the shape the Phase 10
// linearizability checker needs: one Op per client operation with its call and
// return times. The on-disk format is JSON Lines (one Op per line, sorted by
// call time), so a checker, a visualiser or a human with jq can read it with
// no code from this repo. Phase 10 adds the Porcupine model next to it.
package history

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
)

// Operation names.
const (
	Get    = "get"
	Put    = "put"
	Append = "append"
)

// Op is one client operation: (clientID, op, key, value, output, callTime,
// returnTime).
//
// Call and Return are nanoseconds on the run's cluster clock since the start
// of the run (a FakeClock on memnet, the wall clock on docker), so they are
// comparable across clients.
//
// Unknown marks an op whose outcome the client never learned (it timed out, or
// the run ended while it was in flight): it may or may not have taken effect,
// at any point after Call. Its Return is the moment the client gave up; a
// checker should treat it as running until the end of the history and accept
// any output (BUILDMAP Phase 10). Output is empty for put, append and unknown
// ops. For a get, Output is the value read ("" if the key does not exist).
type Op struct {
	Client  string `json:"client"`
	Op      string `json:"op"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
	Output  string `json:"output,omitempty"`
	Call    int64  `json:"call"`
	Return  int64  `json:"return"`
	Unknown bool   `json:"unknown,omitempty"`
}

// Recorder collects Ops from many client goroutines.
type Recorder struct {
	clk   clock.Clock
	start time.Time

	mu  sync.Mutex
	ops []Op
}

// NewRecorder returns a recorder whose times are measured from start on clk.
func NewRecorder(clk clock.Clock, start time.Time) *Recorder {
	return &Recorder{clk: clk, start: start}
}

// Now is the time on the history's axis.
func (r *Recorder) Now() int64 { return int64(r.clk.Now().Sub(r.start)) }

// Add records one finished (or abandoned) op.
func (r *Recorder) Add(op Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// Ops returns a copy of everything recorded, sorted by call time (ties by
// client and return time, so the order is stable).
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	out := append([]Op(nil), r.ops...)
	r.mu.Unlock()
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Call != out[j].Call {
			return out[i].Call < out[j].Call
		}
		if out[i].Client != out[j].Client {
			return out[i].Client < out[j].Client
		}
		return out[i].Return < out[j].Return
	})
	return out
}

// KV is the client surface a recorded client wraps (kv.Client has it).
type KV interface {
	Get(ctx context.Context, key string) (string, error)
	Put(ctx context.Context, key, value string) error
	Append(ctx context.Context, key, value string) error
}

// Client records every operation made through it. Like kv.Client it must be
// used from one goroutine at a time.
type Client struct {
	id      string
	kv      KV
	rec     *Recorder
	timeout time.Duration
}

// Wrap returns a recording view of c named id. An op that has not completed
// after timeout of cluster time is abandoned and recorded as Unknown.
func (r *Recorder) Wrap(id string, c KV, timeout time.Duration) *Client {
	return &Client{id: id, kv: c, rec: r, timeout: timeout}
}

// Do performs and records one op. It returns the value read (for a get),
// whether the outcome is known, and ctx's error if the whole run was cancelled.
func (c *Client) Do(ctx context.Context, op, key, value string) (out string, known bool) {
	o := Op{Client: c.id, Op: op, Key: key, Value: value, Call: c.rec.Now()}
	octx, cancel := context.WithCancel(ctx)
	t := c.rec.clk.NewTimer(c.timeout)
	go func() {
		select {
		case <-t.C():
			cancel()
		case <-octx.Done():
		}
	}()
	var err error
	switch op {
	case Get:
		out, err = c.kv.Get(octx, key)
	case Put:
		err = c.kv.Put(octx, key, value)
	case Append:
		err = c.kv.Append(octx, key, value)
	default:
		err = fmt.Errorf("history: unknown op %q", op)
	}
	t.Stop()
	cancel()
	o.Return = c.rec.Now()
	if err != nil {
		o.Unknown = true
		out = ""
	} else {
		o.Output = out
	}
	c.rec.Add(o)
	return out, err == nil
}

// Write stores ops as JSON Lines in path.
func Write(path string, ops []Op) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, o := range ops {
		if err := enc.Encode(o); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Read loads a history written by Write.
func Read(path string) ([]Op, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ops []Op
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var o Op
		if err := json.Unmarshal(sc.Bytes(), &o); err != nil {
			return nil, fmt.Errorf("history: bad line %d: %w", len(ops)+1, err)
		}
		ops = append(ops, o)
	}
	return ops, sc.Err()
}

// Verdict is a checker's answer about a history.
type Verdict string

const (
	// Unchecked: no checker ran (Phase 9; Phase 10 replaces it).
	Unchecked Verdict = "unchecked"
	Ok        Verdict = "ok"
	Violation Verdict = "violation"
	// Unknown: the checker gave up (timeout) without an answer.
	Unknown Verdict = "unknown"
)

// Checker decides whether a history is linearizable. Phase 10 provides the
// Porcupine implementation; chaos.Run takes any Checker.
type Checker interface {
	Check(ops []Op) (Verdict, error)
}

// NoCheck is the Phase 9 checker: it accepts nothing and says so.
type NoCheck struct{}

func (NoCheck) Check([]Op) (Verdict, error) { return Unchecked, nil }
