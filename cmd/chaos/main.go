// Command chaos runs fault-injection scenarios against a RaftKV cluster
// (in-process on memnet, or Docker containers) under a mixed get/put/append
// load, and writes one run directory per run:
//
//	<out>/<scenario>-<seed>/history.jsonl   every client op (see internal/history)
//	                        timeline.jsonl  the injected faults
//	                        meta.json       seed, counts, ops/s, sanity, verdict
//	                        verdict         "verdict: unchecked" (Phase 10 checks)
//
// Re-running with the same -scenario and -seed reproduces the fault schedule.
// With -runs N it picks N random scenario/seed pairs and prints a summary.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/chaos"
	"github.com/panavdoesstuff/raftkv/internal/kv"
)

func main() {
	var (
		scenario = flag.String("scenario", "random", "scenario name, or random (see -list)")
		seed     = flag.Int64("seed", 0, "seed (0: pick one from the clock; it is printed)")
		duration = flag.Duration("duration", 30*time.Second, "run length in cluster time")
		clients  = flag.Int("clients", 4, "concurrent clients")
		nodes    = flag.Int("nodes", 3, "cluster size")
		readMode = flag.String("read-mode", "", "log|readindex|lease (default: the scenario's)")
		target   = flag.String("target", "memnet", "memnet|docker")
		runs     = flag.Int("runs", 1, "number of runs; with more than one, scenarios and seeds are random")
		out      = flag.String("out", "chaos-runs", "directory for run directories")
		compose  = flag.String("compose-file", "deploy/docker-compose.3.yml", "docker target: compose file")
		list     = flag.Bool("list", false, "list scenarios and exit")
	)
	flag.Parse()
	if *list {
		for _, s := range chaos.Scenarios() {
			fmt.Printf("%-22s %s\n", s.Name, s.Doc)
		}
		return
	}
	var mode *kv.ReadMode
	if *readMode != "" {
		m, err := kv.ParseReadMode(*readMode)
		if err != nil {
			fmt.Fprintln(os.Stderr, "chaos:", err)
			os.Exit(2)
		}
		mode = &m
	}
	base := *seed
	if base == 0 {
		base = time.Now().UnixNano() % 1_000_000_000
	}
	pick := rand.New(rand.NewSource(base))
	names := []string{}
	for _, s := range chaos.Scenarios() {
		names = append(names, s.Name)
	}

	var results []chaos.Result
	failed := 0
	for i := 0; i < *runs; i++ {
		name, s := *scenario, base
		if *runs > 1 {
			s = base*1000 + int64(i)
		}
		if name == "random" {
			name = names[pick.Intn(len(names))]
		}
		res, err := chaos.Run(chaos.Config{
			Scenario: name, Seed: s, Duration: *duration, Clients: *clients, Nodes: *nodes,
			ReadMode: mode, Target: *target, OutDir: *out,
			Docker: chaos.DockerOptions{ComposeFile: *compose},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "chaos: %s seed=%d: %v\n", name, s, err)
			res.Err = err.Error()
		}
		results = append(results, res)
		status := "ok"
		if res.Failed() {
			status = "FAIL"
			failed++
		}
		fmt.Printf("%-4s %-22s seed=%-12d %7.1f ops/s unknown=%-3d sanity=%s verdict=%s %s\n",
			status, res.Scenario, res.Seed, res.OpsPerSec, res.Unknown, res.Sanity, res.Verdict, res.Err)
	}
	fmt.Println()
	summary(results)
	fmt.Printf("chaos: %d runs, %d failed\n", len(results), failed)
	if failed > 0 {
		os.Exit(1)
	}
}

func summary(rs []chaos.Result) {
	type agg struct {
		runs, fail, unknown, sanity int
		ops, min, max               float64
	}
	by := map[string]*agg{}
	for _, r := range rs {
		a := by[r.Scenario]
		if a == nil {
			a = &agg{min: 1e18}
			by[r.Scenario] = a
		}
		a.runs++
		if r.Failed() {
			a.fail++
		}
		a.unknown += r.Unknown
		a.sanity += r.SanityViolations
		a.ops += r.OpsPerSec
		if r.OpsPerSec < a.min {
			a.min = r.OpsPerSec
		}
		if r.OpsPerSec > a.max {
			a.max = r.OpsPerSec
		}
	}
	var names []string
	for n := range by {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Printf("%-22s %5s %5s %9s %9s %9s %8s %7s\n", "scenario", "runs", "fail", "ops/s avg", "min", "max", "unknown", "sanity")
	fmt.Println(strings.Repeat("-", 84))
	for _, n := range names {
		a := by[n]
		fmt.Printf("%-22s %5d %5d %9.1f %9.1f %9.1f %8d %7d\n", n, a.runs, a.fail, a.ops/float64(a.runs), a.min, a.max, a.unknown, a.sanity)
	}
	fmt.Println()
}
