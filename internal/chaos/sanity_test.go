package chaos

import (
	"strings"
	"testing"

	"github.com/panavdoesstuff/raftkv/internal/history"
)

func op(client, kind, key, val, out string, call, ret int64, unknown bool) history.Op {
	return history.Op{Client: client, Op: kind, Key: key, Value: val, Output: out, Call: call, Return: ret, Unknown: unknown}
}

var anyKey = func(string) bool { return true }

func TestSanityCleanHistory(t *testing.T) {
	ops := []history.Op{
		op("c0", "append", "a0", "c0.1;", "", 1, 2, false),
		op("c1", "append", "a0", "c1.1;", "", 3, 4, false),
		op("c0", "get", "a0", "", "c0.1;c1.1;", 5, 6, false),
		op("c0", "get", "a0", "", "c0.1;c1.1;", 7, 8, false),
		op("c2", "append", "a0", "c2.1;", "", 9, 20, true), // unknown, may or may not apply
		op("final", "get", "a0", "", "c0.1;c1.1;", 30, 31, false),
	}
	if s := CheckSanity(ops, anyKey, "final"); !s.OK() {
		t.Fatalf("clean history flagged: %s", s)
	}
}

func TestSanityCatchesEachProblem(t *testing.T) {
	cases := []struct {
		name string
		ops  []history.Op
		want string
	}{
		{"duplicate append", []history.Op{
			op("c0", "append", "a0", "c0.1;", "", 1, 2, false),
			op("final", "get", "a0", "", "c0.1;c0.1;", 5, 6, false),
		}, "duplicates"},
		{"lost acknowledged append", []history.Op{
			op("c0", "append", "a0", "c0.1;", "", 1, 2, false),
			op("final", "get", "a0", "", "", 5, 6, false),
		}, "lost"},
		{"phantom token", []history.Op{
			op("c0", "get", "a0", "", "zz.9;", 1, 2, false),
		}, "phantoms"},
		{"stale read after an acknowledged append", []history.Op{
			op("c0", "append", "a0", "c0.1;", "", 1, 2, false),
			op("c1", "get", "a0", "", "", 3, 4, false),
		}, "stale"},
		{"stale read after a newer read", []history.Op{
			op("c0", "append", "a0", "c0.1;", "", 1, 20, false),
			op("c1", "get", "a0", "", "c0.1;", 3, 4, false),
			op("c2", "get", "a0", "", "", 5, 6, false),
		}, "stale"},
	}
	for _, c := range cases {
		s := CheckSanity(c.ops, anyKey, "final")
		if s.OK() || !strings.Contains(s.String(), c.want) {
			t.Errorf("%s: sanity = %q, want %q", c.name, s.String(), c.want)
		}
	}
}

// Concurrent operations may be seen in either order.
func TestSanityConcurrentOpsAreNotStale(t *testing.T) {
	ops := []history.Op{
		op("c0", "append", "a0", "c0.1;", "", 1, 10, false),
		op("c1", "get", "a0", "", "", 5, 6, false), // overlaps the append: may miss it
		op("c2", "get", "a0", "", "c0.1;", 5, 8, false),
	}
	if s := CheckSanity(ops, anyKey, "final"); !s.OK() {
		t.Fatalf("concurrent ops flagged: %s", s)
	}
}
