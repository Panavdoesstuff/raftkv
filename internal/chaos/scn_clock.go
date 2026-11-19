package chaos

import (
	"fmt"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/transport"
)

// ---- clocks ----

// skewAll gives every node a drift rate in [0.7, 1.3) and, one time in four, a
// skew of up to +-500 ms. All draws are made for every node.
func skewAll(e *Env) {
	for _, id := range e.IDs() {
		rate := 0.7 + 0.6*e.Rng.Float64()
		off := time.Duration(e.Rng.Intn(1001)-500) * time.Millisecond
		jump := e.Rng.Intn(4) == 0
		args := fmt.Sprintf("%s rate=%.3f", id, rate)
		if jump {
			args += fmt.Sprintf(" offset=%dms", off.Milliseconds())
		}
		e.Log("clock", args, "", e.T.SetClock(id, jump, off, rate))
	}
}

func resetClocks(e *Env) {
	for _, id := range e.IDs() {
		e.T.SetClock(id, true, 0, 1)
	}
	e.Log("clock-reset", "all", "", nil)
}

func skewReadIndex(e *Env) {
	for e.Wait(3 * time.Second) {
		skewAll(e)
	}
}

// skewLease: every 8 s cut the leader off and slow its clock to 5% at the same
// instant (the Phase 6 mechanism: a lease of 150 ms lasts 3 s of real time),
// hold 3.5 s, then restore both.
func skewLease(e *Env) {
	for base := time.Duration(0); ; base += 8 * time.Second {
		if !e.WaitUntil(base + time.Second) {
			return
		}
		l, ok := e.Leader()
		if ok {
			e.Log("skew-isolate", "leader rate=0.05", string(l), firstErr(
				e.T.SetClock(l, false, 0, 0.05), e.Cut(transport.Isolate(l, e.IDs()))))
		} else {
			e.Log("skew-isolate", "leader rate=0.05", "no leader", nil)
		}
		if !e.WaitUntil(base + 4500*time.Millisecond) {
			restoreLease(e, l, ok)
			return
		}
		restoreLease(e, l, ok)
	}
}

func restoreLease(e *Env, l raft.NodeID, ok bool) {
	e.Log("heal", "", "", e.Heal())
	if ok {
		e.T.SetClock(l, true, 0, 1)
	}
	e.Log("clock-reset", "leader", string(l), nil)
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
