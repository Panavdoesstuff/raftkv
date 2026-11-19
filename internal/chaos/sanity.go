package chaos

import (
	"fmt"
	"sort"
	"strings"

	"github.com/panavdoesstuff/raftkv/internal/history"
)

// Sanity is a cheap check that does NOT replace the Phase 10 linearizability
// checker. It exploits append-only keys: the load appends unique tokens to the
// keys named in AppendKeys, so a correct store keeps every acknowledged token
// exactly once, and the value of such a key only ever grows. That makes three
// necessary conditions of linearizability easy to test on a history:
//
//   - exactly once: no token appears twice in any value read (a duplicate
//     append), and no read contains a token nobody appended
//   - durability: after the run the final read of each key holds every token
//     whose append was acknowledged
//   - real-time order: if read r1 returned before read r2 was called, or an
//     acknowledged append returned before a read was called, the later read
//     contains everything the earlier operation showed. A stale read breaks
//     this (it is how the lease demo shows up before Phase 10).
type Sanity struct {
	Reads, Appends  int
	Duplicates      []string // tokens read more than once in one value
	Phantoms        []string // tokens read but never appended
	Lost            []string // acknowledged appends missing from the final read
	StaleReads      []string // real-time order violations
	FinalReadsFound int
}

// OK reports whether nothing was violated.
func (s Sanity) OK() bool {
	return len(s.Duplicates)+len(s.Phantoms)+len(s.Lost)+len(s.StaleReads) == 0
}

// Violations is the count of all findings.
func (s Sanity) Violations() int {
	return len(s.Duplicates) + len(s.Phantoms) + len(s.Lost) + len(s.StaleReads)
}

func (s Sanity) String() string {
	if s.OK() {
		return "ok"
	}
	var parts []string
	add := func(name string, l []string) {
		if len(l) > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d (e.g. %s)", name, len(l), l[0]))
		}
	}
	add("duplicates", s.Duplicates)
	add("phantoms", s.Phantoms)
	add("lost", s.Lost)
	add("stale", s.StaleReads)
	return strings.Join(parts, " ")
}

// splitTokens splits an append-key value into its tokens (each ends with ';').
func splitTokens(v string) []string {
	var out []string
	for _, t := range strings.Split(v, ";") {
		if t != "" {
			out = append(out, t+";")
		}
	}
	return out
}

// maxPairs bounds the quadratic real-time-order check per key.
const maxPairs = 4_000_000

// CheckSanity runs the checks over ops. isAppendKey says which keys are
// append-only. finalClient names the client that did the post-run reads.
func CheckSanity(ops []history.Op, isAppendKey func(string) bool, finalClient string) Sanity {
	var s Sanity
	type rd struct {
		call, ret int64
		val       string
		set       map[string]bool
	}
	appended := map[string]bool{}      // every token any client tried to append
	acked := map[string][]history.Op{} // key -> acknowledged appends
	reads := map[string][]rd{}
	final := map[string]string{}
	for _, o := range ops {
		if !isAppendKey(o.Key) {
			continue
		}
		switch o.Op {
		case history.Append:
			appended[o.Value] = true
			s.Appends++
			if !o.Unknown {
				acked[o.Key] = append(acked[o.Key], o)
			}
		case history.Get:
			if o.Unknown {
				continue
			}
			s.Reads++
			r := rd{call: o.Call, ret: o.Return, val: o.Output, set: map[string]bool{}}
			seen := map[string]bool{}
			for _, t := range splitTokens(o.Output) {
				if seen[t] {
					s.Duplicates = append(s.Duplicates, fmt.Sprintf("%s read by %s has %q twice", o.Key, o.Client, t))
				}
				seen[t] = true
				r.set[t] = true
			}
			reads[o.Key] = append(reads[o.Key], r)
			if o.Client == finalClient {
				final[o.Key] = o.Output
				s.FinalReadsFound++
			}
		}
	}
	for key, rs := range reads {
		for _, r := range rs {
			for t := range r.set {
				if !appended[t] {
					s.Phantoms = append(s.Phantoms, fmt.Sprintf("%s holds %q that no client appended", key, t))
				}
			}
		}
	}
	for key, aa := range acked {
		fv, ok := final[key]
		if !ok {
			continue
		}
		have := map[string]bool{}
		for _, t := range splitTokens(fv) {
			have[t] = true
		}
		for _, a := range aa {
			if !have[a.Value] {
				s.Lost = append(s.Lost, fmt.Sprintf("%s: acknowledged append %q by %s missing at the end", key, a.Value, a.Client))
			}
		}
	}
	// Real-time order. Sort reads by call time; for each read r2, everything
	// shown by reads that returned before r2 was called, and every append
	// acknowledged before, must be in r2.
	for key, rs := range reads {
		sort.Slice(rs, func(i, j int) bool { return rs[i].call < rs[j].call })
		aa := acked[key]
		if len(rs)*len(rs) > maxPairs {
			continue
		}
		for j, r2 := range rs {
			for i, r1 := range rs {
				if i == j || r1.ret >= r2.call {
					continue
				}
				if len(r2.set) >= len(r1.set) && r1.val != "" && !strings.HasPrefix(r2.val, r1.val) {
					// Same or more tokens but not an extension: diverged.
					s.StaleReads = append(s.StaleReads, fmt.Sprintf("%s: read at %dms is not an extension of an earlier read", key, r2.ret/1e6))
					continue
				}
				if len(r2.set) >= len(r1.set) {
					continue
				}
				for t := range r1.set {
					if !r2.set[t] {
						s.StaleReads = append(s.StaleReads, fmt.Sprintf("%s: read returned at %dms lacks %q seen by a read that returned at %dms (before it was called)",
							key, r2.ret/1e6, t, r1.ret/1e6))
						break
					}
				}
			}
			for _, a := range aa {
				if a.Return < r2.call && !r2.set[a.Value] {
					s.StaleReads = append(s.StaleReads, fmt.Sprintf("%s: read called at %dms lacks %q whose append by %s was acknowledged at %dms",
						key, r2.call/1e6, a.Value, a.Client, a.Return/1e6))
				}
			}
		}
	}
	sort.Strings(s.StaleReads)
	return s
}
