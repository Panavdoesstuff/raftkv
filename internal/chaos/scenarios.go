package chaos

import "github.com/panavdoesstuff/raftkv/internal/kv"

// Scenario is a fault script. Run loops until Env.Wait reports the run is
// over. Every decision that shapes the schedule must come from e.Rng and the
// scenario's own state, never from the live cluster (see Event).
type Scenario struct {
	Name     string
	Doc      string
	ReadMode kv.ReadMode // the mode the scenario is about (flag overrides)
	Run      func(e *Env)
}

// Scenarios lists them in BUILDMAP order.
func Scenarios() []Scenario { return scenarios }

// Lookup finds a scenario by name.
func Lookup(name string) (Scenario, bool) {
	for _, s := range scenarios {
		if s.Name == name {
			return s, true
		}
	}
	return Scenario{}, false
}

// StaleReadsExpected is true for the scenario whose point is to produce them.
func StaleReadsExpected(s Scenario, mode kv.ReadMode) bool {
	return s.Name == "clock-skew-lease" || (mode == kv.ReadLease && s.Name == "everything")
}

var scenarios = []Scenario{
	{"isolate-leader", "leader cut off every 5 s", kv.ReadIndex, isolateLeader},
	{"random-partitions", "random splits every 2-5 s", kv.ReadIndex, randomPartitions},
	{"bridge", "one node sees both halves", kv.ReadIndex, bridge},
	{"flaky-net", "20% drops, 0-200 ms delay, duplicates, reordering", kv.ReadIndex, flakyNet},
	{"crash-restart", "random kill/restart every 3 s", kv.ReadIndex, crashRestart},
	{"pause", "pause the leader for longer than an election timeout", kv.ReadIndex, pauseLeader},
	{"membership-chaos", "add/remove servers during partitions", kv.ReadIndex, membershipChaos},
	{"clock-skew-readindex", "drift 0.7x-1.3x on all nodes, ReadIndex", kv.ReadIndex, skewReadIndex},
	{"clock-skew-lease", "slow drift on an isolated leader, lease reads (violations expected)", kv.ReadLease, skewLease},
	{"everything", "all of the above at random", kv.ReadIndex, everything},
}
