// Package clock abstracts time so Raft code never touches the wall clock.
// All time in internal/raft must come from a Clock (BUILDMAP §3).
package clock

import "time"

// Timer is a one-shot timer.
type Timer interface {
	C() <-chan time.Time
	// Stop prevents the timer from firing. Reports whether it was active.
	Stop() bool
	// Reset re-arms the timer to fire after d. Reports whether it was active.
	Reset(d time.Duration) bool
}

// Ticker delivers ticks at a fixed interval.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Clock is the source of time for a node.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
}
