package chaos

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/panavdoesstuff/raftkv/internal/history"
)

func runOnce(t *testing.T, name string, seed int64, d time.Duration, dir string) Result {
	t.Helper()
	res, err := Run(Config{Scenario: name, Seed: seed, Duration: d, Clients: 3, Target: "memnet", OutDir: dir})
	if err != nil {
		t.Fatalf("%s seed=%d: %v", name, seed, err)
	}
	if res.Err != "" {
		t.Errorf("%s seed=%d: run failed: %s", name, seed, res.Err)
	}
	if res.Failed() && res.Err == "" {
		t.Errorf("%s seed=%d: sanity violations: %s", name, seed, res.Sanity)
	}
	return res
}

// Every scenario runs, makes progress, and writes history, timeline, seed and
// an unchecked verdict.
func TestEveryScenarioRunsAndWritesOutputs(t *testing.T) {
	dir := t.TempDir()
	for _, sc := range Scenarios() {
		sc := sc
		t.Run(sc.Name, func(t *testing.T) {
			res := runOnce(t, sc.Name, 5, 10*time.Second, dir)
			if res.OpsPerSec <= 0 {
				t.Errorf("no progress (seed=%d)", res.Seed)
			}
			for _, f := range []string{"history.jsonl", "timeline.jsonl", "meta.json", "verdict"} {
				if _, err := os.Stat(filepath.Join(res.Dir, f)); err != nil {
					t.Errorf("missing %s: %v", f, err)
				}
			}
			b, _ := os.ReadFile(filepath.Join(res.Dir, "verdict"))
			if string(b) != "verdict: unchecked\n" {
				t.Errorf("verdict file = %q", b)
			}
			ops, err := history.Read(filepath.Join(res.Dir, "history.jsonl"))
			if err != nil || len(ops) != res.Ops {
				t.Errorf("history: %d ops (want %d), err=%v", len(ops), res.Ops, err)
			}
			if evs, err := ReadTimeline(filepath.Join(res.Dir, "timeline.jsonl")); err != nil || len(evs) == 0 {
				t.Errorf("timeline: %d events, err=%v", len(evs), err)
			}
			t.Logf("%s: %.0f ops/s, %d unknown, sanity %s", sc.Name, res.OpsPerSec, res.Unknown, res.Sanity)
		})
	}
}

// The fault schedule is a function of the seed alone.
func TestSameSeedSameFaultTimeline(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"random-partitions", "crash-restart", "clock-skew-readindex", "everything", "membership-chaos"} {
		var sched [3][]string
		for i, seed := range []int64{21, 21, 22} {
			res := runOnce(t, name, seed, 25*time.Second, filepath.Join(dir, string(rune('a'+i))))
			evs, err := ReadTimeline(filepath.Join(res.Dir, "timeline.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			sched[i] = Schedule(evs)
		}
		if len(sched[0]) < 3 {
			t.Errorf("%s: only %d scheduled events", name, len(sched[0]))
		}
		if !reflect.DeepEqual(sched[0], sched[1]) {
			t.Errorf("%s: same seed gave different schedules:\n%v\n%v", name, sched[0], sched[1])
		}
		if reflect.DeepEqual(sched[0], sched[2]) {
			t.Errorf("%s: different seeds gave the same schedule", name)
		}
	}
}
