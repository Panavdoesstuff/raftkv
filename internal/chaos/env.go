package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/transport"
)

// Event is one line of the fault timeline. AtMs, Action and Args are decided
// by the scenario from its seeded RNG alone, so a rerun with the same seed
// produces the same sequence (the determinism check compares exactly these).
// Detail (what "leader" resolved to, what an RPC answered) and ActualMs (when
// it really happened) depend on the live cluster and are not reproducible.
// Info events (results of membership calls) are not part of the schedule.
type Event struct {
	AtMs     int64  `json:"at_ms"`
	Action   string `json:"action"`
	Args     string `json:"args,omitempty"`
	Detail   string `json:"detail,omitempty"`
	ActualMs int64  `json:"actual_ms"`
	Info     bool   `json:"info,omitempty"`
	Err      string `json:"err,omitempty"`
}

// Key is the reproducible part of an event.
func (e Event) Key() string { return fmt.Sprintf("%d %s %s", e.AtMs, e.Action, e.Args) }

// Schedule returns the reproducible keys of the non-info events.
func Schedule(evs []Event) []string {
	var out []string
	for _, e := range evs {
		if !e.Info {
			out = append(out, e.Key())
		}
	}
	return out
}

// WriteTimeline stores events as JSON Lines.
func WriteTimeline(path string, evs []Event) error {
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, e := range evs {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(sb.String()), 0o644)
}

// Env is what a scenario script sees. The script moves a virtual cursor
// forward with Wait; events are stamped with the cursor, not the wall time, so
// the schedule does not depend on how long an RPC took. If an action overruns
// (a membership call that takes seconds), later waits return at once until the
// script catches up.
type Env struct {
	T        Target
	Rng      *rand.Rand
	ReadMode kv.ReadMode
	Duration time.Duration

	start  time.Time
	cursor time.Duration
	ctx    context.Context
	// onServers is told the member list after a membership change.
	onServers func([]raft.NodeID)

	// Script state (only the scenario goroutine touches these).
	members     []raft.NodeID // the cluster membership the script believes in
	crashedNode raft.NodeID
	pausedNode  raft.NodeID
	nextNode    int

	mu      sync.Mutex
	events  []Event
	everCut map[raft.NodeID]bool
}

// Now is the cursor.
func (e *Env) Now() time.Duration { return e.cursor }

// Wait advances the cursor by d and sleeps (on the cluster clock) until that
// time. It returns false when the run is over; the caller should then stop.
func (e *Env) Wait(d time.Duration) bool { return e.WaitUntil(e.cursor + d) }

// WaitUntil is Wait to an absolute cursor value.
func (e *Env) WaitUntil(at time.Duration) bool {
	if at >= e.Duration {
		e.cursor = e.Duration
	} else {
		e.cursor = at
	}
	e.sleepTo(e.cursor)
	return e.cursor < e.Duration && e.ctx.Err() == nil
}

func (e *Env) sleepTo(at time.Duration) {
	clk := e.T.Clock()
	d := e.start.Add(at).Sub(clk.Now())
	if d <= 0 {
		return
	}
	t := clk.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
	case <-e.ctx.Done():
	}
}

// Log records a scheduled event at the cursor.
func (e *Env) Log(action, args, detail string, err error) {
	ev := Event{AtMs: e.cursor.Milliseconds(), Action: action, Args: args, Detail: detail,
		ActualMs: e.T.Clock().Now().Sub(e.start).Milliseconds()}
	if err != nil {
		ev.Err = err.Error()
	}
	e.mu.Lock()
	e.events = append(e.events, ev)
	e.mu.Unlock()
}

// Info records a non-scheduled observation (a result).
func (e *Env) Info(action, detail string, err error) {
	ev := Event{AtMs: e.cursor.Milliseconds(), Action: action, Detail: detail, Info: true,
		ActualMs: e.T.Clock().Now().Sub(e.start).Milliseconds()}
	if err != nil {
		ev.Err = err.Error()
	}
	e.mu.Lock()
	e.events = append(e.events, ev)
	e.mu.Unlock()
}

// Events returns the timeline so far.
func (e *Env) Events() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Event(nil), e.events...)
}

func (e *Env) IDs() []raft.NodeID { return e.T.Nodes() }

func ids(ns []raft.NodeID) string {
	var s []string
	for _, n := range ns {
		s = append(s, string(n))
	}
	return strings.Join(s, ",")
}

// Cut installs a partition (rules from the transport helpers) on top of
// whatever is installed, remembering which nodes it touched so Heal can undo it.
func (e *Env) Cut(rules []transport.Rule) error {
	e.mu.Lock()
	if e.everCut == nil {
		e.everCut = map[raft.NodeID]bool{}
	}
	for _, r := range rules {
		e.everCut[r.From], e.everCut[r.To] = true, true
	}
	e.mu.Unlock()
	return e.T.SetRules(false, rules)
}

// Heal removes every partition rule. Wildcard (flaky) rules stay.
func (e *Env) Heal() error {
	e.mu.Lock()
	var touched []raft.NodeID
	for id := range e.everCut {
		touched = append(touched, id)
	}
	e.mu.Unlock()
	return e.T.SetRules(false, transport.Heal(touched))
}

// Leader resolves "the leader" for a fault, for Detail.
func (e *Env) Leader() (raft.NodeID, bool) { return e.T.Leader() }

// Pick returns a random element index source: a uniform node among ids.
func (e *Env) Pick(from []raft.NodeID) raft.NodeID { return from[e.Rng.Intn(len(from))] }

// Between returns a uniform duration in [lo, hi).
func (e *Env) Between(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(e.Rng.Int63n(int64(hi-lo)))
}

// ReadTimeline loads a timeline written by WriteTimeline.
func ReadTimeline(path string) ([]Event, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []Event
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
