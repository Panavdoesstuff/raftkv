package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/history"
	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
)

// Config is one chaos run.
type Config struct {
	Scenario string
	Seed     int64
	Duration time.Duration // cluster time
	Clients  int
	Nodes    int
	ReadMode *kv.ReadMode // nil: the scenario's own
	Target   string       // "memnet" or "docker"
	OutDir   string       // run directory parent; "" = chaos-runs
	Checker  history.Checker
	// NewTarget overrides target construction (tests).
	NewTarget func(c Config, mode kv.ReadMode) (Target, error)
	Docker    DockerOptions
}

// Result summarises a run. The run directory has history.jsonl, timeline.jsonl
// and meta.json (the same fields as Result, including the seed and verdict).
type Result struct {
	Scenario   string  `json:"scenario"`
	Seed       int64   `json:"seed"`
	Target     string  `json:"target"`
	ReadMode   string  `json:"read_mode"`
	DurationMs int64   `json:"duration_ms"`
	Clients    int     `json:"clients"`
	Nodes      int     `json:"nodes"`
	Ops        int     `json:"ops"`      // recorded, including the final reads
	Completed  int     `json:"complete"` // outcome known
	Unknown    int     `json:"unknown"`  // timed out
	OpsPerSec  float64 `json:"ops_per_sec"`
	Verdict    string  `json:"verdict"` // linearizability: "unchecked" until Phase 10
	Sanity     string  `json:"sanity"`  // the cheap check in sanity.go
	// SanityViolations counts sanity findings (stale reads are expected for
	// the lease scenario; see StaleExpected).
	SanityViolations int    `json:"sanity_violations"`
	StaleExpected    bool   `json:"stale_expected"`
	Events           int    `json:"events"`
	Dir              string `json:"dir"`
	// Err is set when the run itself failed: a harness safety violation, the
	// cluster not serving after recovery, no client progress, a panic.
	Err string `json:"err,omitempty"`
}

// Failed reports whether the run found something wrong.
func (r Result) Failed() bool {
	return r.Err != "" || (r.SanityViolations > 0 && !r.StaleExpected)
}

const (
	opTimeout       = 6 * time.Second
	recoverTimeout  = 30 * time.Second
	leaderWait      = 15 * time.Second
	finalClientName = "final"
)

var (
	appendKeys = []string{"a0", "a1", "a2"}
	putKeys    = []string{"p0", "p1"}
)

func isAppendKey(k string) bool { return strings.HasPrefix(k, "a") }

func seedFor(base int64, name string) int64 {
	h := fnv.New64a()
	h.Write([]byte(name))
	return base ^ int64(h.Sum64()&0x7fffffffffffffff)
}

// Run executes one scenario and writes its run directory.
func Run(cfg Config) (res Result, err error) {
	sc, ok := Lookup(cfg.Scenario)
	if !ok {
		return Result{}, fmt.Errorf("unknown scenario %q", cfg.Scenario)
	}
	if cfg.Clients <= 0 {
		cfg.Clients = 4
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 30 * time.Second
	}
	if cfg.Nodes <= 0 {
		cfg.Nodes = 3
	}
	if cfg.OutDir == "" {
		cfg.OutDir = "chaos-runs"
	}
	if cfg.Checker == nil {
		cfg.Checker = history.NoCheck{}
	}
	mode := sc.ReadMode
	if cfg.ReadMode != nil {
		mode = *cfg.ReadMode
	}
	res = Result{Scenario: sc.Name, Seed: cfg.Seed, Target: cfg.Target, ReadMode: mode.String(),
		DurationMs: cfg.Duration.Milliseconds(), Clients: cfg.Clients, Nodes: cfg.Nodes,
		Verdict: string(history.Unchecked), StaleExpected: StaleReadsExpected(sc, mode)}
	res.Dir = filepath.Join(cfg.OutDir, fmt.Sprintf("%s-%d", sc.Name, cfg.Seed))
	if err := os.MkdirAll(res.Dir, 0o755); err != nil {
		return res, err
	}
	defer func() {
		if r := recover(); r != nil {
			res.Err = fmt.Sprintf("panic: %v", r)
			err = nil
		}
		writeMeta(res)
	}()

	mk := cfg.NewTarget
	if mk == nil {
		mk = defaultTarget
	}
	tg, terr := mk(cfg, mode)
	if terr != nil {
		res.Err = "start target: " + terr.Error()
		return res, nil
	}
	defer tg.Close()
	clk := tg.Clock()

	// Wait for a first leader so the load does not start into an election.
	{
		deadline := clk.NewTimer(leaderWait)
		poll := clk.NewTicker(50 * time.Millisecond)
		got := false
		for !got {
			if _, ok := tg.Leader(); ok {
				got = true
				break
			}
			select {
			case <-poll.C():
			case <-deadline.C():
				res.Err = "no leader elected before the run started"
				poll.Stop()
				return res, nil
			}
		}
		poll.Stop()
		deadline.Stop()
	}

	start := clk.Now()
	rec := history.NewRecorder(clk, start)
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()

	// Clients.
	var clientsMu sync.Mutex
	var clients []*kv.Client
	var wg sync.WaitGroup
	for i := 0; i < cfg.Clients; i++ {
		name := fmt.Sprintf("c%d", i)
		kc := kv.NewClient(name, tg.Nodes(), tg.Caller(), clk)
		clientsMu.Lock()
		clients = append(clients, kc)
		clientsMu.Unlock()
		hc := rec.Wrap(name, kc, opTimeout)
		rng := rand.New(rand.NewSource(cfg.Seed*7919 + int64(i)))
		wg.Add(1)
		go func() {
			defer wg.Done()
			runClient(runCtx, hc, name, rng, clk)
		}()
	}

	// Scenario.
	env := &Env{
		T: tg, Rng: rand.New(rand.NewSource(seedFor(cfg.Seed, sc.Name))), ReadMode: mode,
		Duration: cfg.Duration, start: start, ctx: runCtx, members: tg.Nodes(),
	}
	env.onServers = func(m []raft.NodeID) {
		clientsMu.Lock()
		defer clientsMu.Unlock()
		for _, c := range clients {
			c.SetServers(m)
		}
	}
	sc.Run(env)
	env.WaitUntil(cfg.Duration) // the run lasts the full duration whatever the script did
	stopRun()
	wg.Wait()

	// Recovery: undo every fault, then demand the cluster serve again.
	env.cursor = cfg.Duration
	env.Log("recover", "all", "", tg.RecoverAll())
	fin := kv.NewClient(finalClientName, tg.Nodes(), tg.Caller(), clk)
	fc := rec.Wrap(finalClientName, fin, recoverTimeout)
	if _, known := fc.Do(context.Background(), history.Put, "sentinel", "ok"); !known {
		res.Err = fmt.Sprintf("cluster did not serve a write within %v of cluster time after all faults were removed", recoverTimeout)
	}
	if res.Err == "" {
		for _, k := range append(append([]string(nil), appendKeys...), putKeys...) {
			if _, known := fc.Do(context.Background(), history.Get, k, ""); !known {
				res.Err = fmt.Sprintf("final read of %s did not complete", k)
				break
			}
		}
	}

	ops := rec.Ops()
	for _, o := range ops {
		res.Ops++
		if o.Unknown {
			res.Unknown++
		} else {
			res.Completed++
		}
	}
	// Throughput counts the load phase only.
	loadDone := 0
	for _, o := range ops {
		if o.Client != finalClientName && !o.Unknown {
			loadDone++
		}
	}
	res.OpsPerSec = float64(loadDone) / cfg.Duration.Seconds()
	if loadDone == 0 && res.Err == "" {
		res.Err = "no client operation completed (no progress)"
	}
	s := CheckSanity(ops, isAppendKey, finalClientName)
	res.Sanity, res.SanityViolations = s.String(), s.Violations()
	if mt, ok := tg.(interface{ Harness() []string }); ok {
		if errs := mt.Harness(); len(errs) > 0 && res.Err == "" {
			res.Err = "harness: " + errs[0]
		}
	}
	if v, cerr := cfg.Checker.Check(ops); cerr == nil {
		res.Verdict = string(v)
	} else {
		res.Verdict = string(history.Unknown)
	}
	evs := env.Events()
	res.Events = len(evs)
	if werr := history.Write(filepath.Join(res.Dir, "history.jsonl"), ops); werr != nil {
		return res, werr
	}
	if werr := WriteTimeline(filepath.Join(res.Dir, "timeline.jsonl"), evs); werr != nil {
		return res, werr
	}
	if len(s.StaleReads)+len(s.Duplicates)+len(s.Lost)+len(s.Phantoms) > 0 {
		var sb strings.Builder
		for _, group := range [][]string{s.Duplicates, s.Phantoms, s.Lost, s.StaleReads} {
			for i, line := range group {
				if i >= 20 {
					break
				}
				sb.WriteString(line + "\n")
			}
		}
		os.WriteFile(filepath.Join(res.Dir, "sanity.txt"), []byte(sb.String()), 0o644)
	}
	return res, nil
}

func writeMeta(res Result) {
	b, _ := json.MarshalIndent(res, "", "  ")
	os.WriteFile(filepath.Join(res.Dir, "meta.json"), append(b, '\n'), 0o644)
	os.WriteFile(filepath.Join(res.Dir, "verdict"), []byte("verdict: "+res.Verdict+"\n"), 0o644)
}

// runClient is the load: a closed loop of random gets, puts and appends.
func runClient(ctx context.Context, hc *history.Client, name string, rng *rand.Rand, clk clock.Clock) {
	n := 0
	for ctx.Err() == nil {
		// Think time of 5-15 ms of cluster time keeps the history a size a
		// checker can handle (and the run from being CPU-bound).
		t := clk.NewTimer(5*time.Millisecond + time.Duration(rng.Intn(10))*time.Millisecond)
		select {
		case <-t.C():
		case <-ctx.Done():
			t.Stop()
			return
		}
		n++
		switch r := rng.Intn(10); {
		case r < 4:
			k := keyOf(rng)
			hc.Do(ctx, history.Get, k, "")
		case r < 6:
			hc.Do(ctx, history.Put, putKeys[rng.Intn(len(putKeys))], fmt.Sprintf("%s-%d", name, n))
		default:
			hc.Do(ctx, history.Append, appendKeys[rng.Intn(len(appendKeys))], fmt.Sprintf("%s.%d;", name, n))
		}
	}
}

func keyOf(rng *rand.Rand) string {
	if rng.Intn(5) < 3 {
		return appendKeys[rng.Intn(len(appendKeys))]
	}
	return putKeys[rng.Intn(len(putKeys))]
}

func defaultTarget(c Config, mode kv.ReadMode) (Target, error) {
	switch c.Target {
	case "memnet", "":
		return NewMemTarget(MemOptions{Nodes: c.Nodes, Seed: c.Seed, ReadMode: mode})
	case "docker":
		return NewDockerTarget(c.Docker, c.Nodes, mode)
	}
	return nil, fmt.Errorf("unknown target %q", c.Target)
}
