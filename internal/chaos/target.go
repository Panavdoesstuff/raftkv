// Package chaos runs fault-injection scenarios against a RaftKV cluster and
// records what clients saw. A Target is the cluster under test (in-process on
// memnet, or Docker containers); a Scenario is a script of faults; Run ties
// them together with a client load and writes a run directory.
package chaos

import (
	"errors"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/transport"
)

// ErrUnsupported is returned by a Target for a fault it cannot inject.
var ErrUnsupported = errors.New("chaos: not supported by this target")

// Target is the cluster under test. All times are on Clock(): a FakeClock that
// the target itself advances on memnet, the wall clock on docker.
type Target interface {
	Name() string
	Clock() clock.Clock
	// Nodes lists the nodes the cluster has (including ones added later).
	Nodes() []raft.NodeID
	// Caller is how clients reach the servers.
	Caller() kv.Caller
	// Leader returns the live leader with the highest term, if any.
	Leader() (raft.NodeID, bool)

	// Network faults. Rules are directed (From -> To); an all-zero rule
	// deletes the one for that pair. clearAll first removes every rule.
	SetRules(clearAll bool, rules []transport.Rule) error
	// SetClock sets a node's clock: setOffset makes its skew exactly offset
	// (absolute), rate > 0 sets the drift rate.
	SetClock(id raft.NodeID, setOffset bool, offset time.Duration, rate float64) error

	// Process faults.
	Crash(id raft.NodeID) error   // kill the process, keep its disk
	Restart(id raft.NodeID) error // start it again on the old disk
	Pause(id raft.NodeID) error   // freeze it in place, memory intact
	Resume(id raft.NodeID) error

	// AddNode provisions a brand-new, empty node (a joiner) and returns the
	// address AddServer should be given. The node is not a member until the
	// cluster adds it.
	AddNode(id raft.NodeID) (addr string, err error)

	// RecoverAll undoes every fault: no rules, nodes restarted and resumed,
	// clocks back to real time.
	RecoverAll() error
	Close()
}
