package chaos

import (
	"time"

	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/transport"
)

// ---- everything ----

// everything runs one random fault from the other scenarios in each 3-5 s
// slot, undoing it before the next. Clock skew joins the mix only when reads
// are ReadIndex or log: drift is exactly what breaks lease reads.
func everything(e *Env) {
	kinds := []string{"isolate", "partition", "bridge", "flaky", "crash", "pause", "membership", "skew"}
	for {
		if !e.Wait(0) {
			return
		}
		slot := e.Between(3*time.Second, 5*time.Second)
		end := e.cursor + slot
		kind := kinds[e.Rng.Intn(len(kinds))]
		if kind == "skew" && e.ReadMode == kv.ReadLease {
			kind = "partition"
		}
		e.Log("slot", kind, "", nil)
		switch kind {
		case "isolate":
			isolateCurrentLeader(e)
			e.WaitUntil(end)
			e.Log("heal", "", "", e.Heal())
		case "partition":
			maj, min := transport.SplitMajority(e.IDs(), e.Rng)
			e.Log("partition", partitionDetail(maj, min), "", e.Cut(transport.Partition(maj, min)))
			e.WaitUntil(end)
			e.Log("heal", "", "", e.Heal())
		case "bridge":
			bridgeOnce(e)
			e.WaitUntil(end)
			e.Log("heal", "", "", e.Heal())
		case "flaky":
			e.Log("flaky", "on", "", e.T.SetRules(false, []transport.Rule{flakyRule}))
			e.WaitUntil(end)
			e.Log("flaky-off", "", "", e.T.SetRules(false, []transport.Rule{{}}))
		case "crash":
			crashOnce(e)
			e.WaitUntil(e.cursor + slot/2)
			restartCrashed(e)
			e.WaitUntil(end)
		case "pause":
			hold := e.Between(450*time.Millisecond, 900*time.Millisecond)
			pauseOnce(e, hold)
			e.Wait(hold)
			resumePaused(e)
			e.WaitUntil(end)
		case "membership":
			membershipStep(e, e.Rng.Intn(2) == 0)
			e.WaitUntil(end)
		case "skew":
			skewAll(e)
			e.WaitUntil(end)
			resetClocks(e)
		}
		if e.cursor >= e.Duration {
			return
		}
	}
}
