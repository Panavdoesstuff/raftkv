package chaos

import (
	"fmt"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/transport"
)

func partitionDetail(maj, min []raft.NodeID) string {
	return fmt.Sprintf("maj=[%s] min=[%s]", ids(maj), ids(min))
}

// isolateLeader cuts the leader off for 3 s out of every 5.
func isolateLeader(e *Env) {
	for e.Wait(2 * time.Second) {
		isolateCurrentLeader(e)
		if !e.Wait(3 * time.Second) {
			return
		}
		e.Log("heal", "", "", e.Heal())
	}
}

func isolateCurrentLeader(e *Env) {
	l, ok := e.Leader()
	detail := string(l)
	if !ok {
		detail = "no leader"
		e.Log("isolate", "leader", detail, nil)
		return
	}
	e.Log("isolate", "leader", detail, e.Cut(transport.Isolate(l, e.IDs())))
}

func randomPartitions(e *Env) {
	for e.Wait(e.Between(2*time.Second, 5*time.Second)) {
		partitionOnce(e)
	}
}

// partitionOnce heals, then (3 times out of 4) installs a random split.
func partitionOnce(e *Env) {
	roll := e.Rng.Intn(4)
	maj, min := transport.SplitMajority(e.IDs(), e.Rng)
	e.Heal()
	if roll == 0 {
		e.Log("heal", "", "", nil)
		return
	}
	e.Log("partition", partitionDetail(maj, min), "", e.Cut(transport.Partition(maj, min)))
}

func bridge(e *Env) {
	for e.Wait(e.Between(1*time.Second, 2*time.Second)) {
		bridgeOnce(e)
		if !e.Wait(e.Between(3*time.Second, 5*time.Second)) {
			return
		}
		e.Log("heal", "", "", e.Heal())
	}
}

func bridgeOnce(e *Env) {
	all := e.IDs()
	b := e.Pick(all)
	var rest []raft.NodeID
	for _, id := range all {
		if id != b {
			rest = append(rest, id)
		}
	}
	e.Rng.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
	a, c := rest[:len(rest)/2], rest[len(rest)/2:]
	e.Log("bridge", fmt.Sprintf("via=%s a=[%s] b=[%s]", b, ids(a), ids(c)), "", e.Cut(transport.Bridge(b, a, c)))
}

var flakyRule = transport.Rule{Drop: 0.2, DelayMin: 0, DelayMax: 200 * time.Millisecond, Dup: 0.1, Reorder: true}

func flakyNet(e *Env) {
	e.Log("flaky", "drop=20% delay=0-200ms dup=10% reorder", "", e.T.SetRules(false, []transport.Rule{flakyRule}))
	e.Wait(e.Duration)
	e.Log("flaky-off", "", "", e.T.SetRules(false, []transport.Rule{{}}))
}
