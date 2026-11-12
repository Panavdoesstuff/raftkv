# RaftKV — Build Map

A Raft-replicated key-value store in Go. It runs as real processes over gRPC, supports snapshots and membership changes, and is proven correct with a fault injector (network partitions, node kills, clock skew) plus the Porcupine linearizability checker.

- **Module:** `github.com/panavdoesstuff/raftkv`
- **Time:** about 3 weeks at 3–4 h/day (about 10–12 days full-time)
- **How to use with Claude Code:** one phase per session. Start each session with:
  > Read `docs/BUILDMAP.md`. Do Phase N. Tests first.

**Rule for the whole build:** don't start the next phase until every exit check of the current one passes.

---

## 0. Definition of done

The project is finished when all of these are true:

- [ ] Leader election, log replication, persistence, snapshots
- [ ] KV store with exactly-once semantics for retried client requests
- [ ] Linearizable reads via ReadIndex, plus lease reads behind a flag
- [ ] Single-server membership changes with learner catch-up and PreVote
- [ ] Real cluster: 3–5 processes over gRPC in Docker Compose
- [ ] Fault injector: partitions, drops, delays, reordering, duplicates, kills, pauses, clock skew
- [ ] Porcupine check on every chaos run; 0 violations across 1,000+ runs
- [ ] Demo: lease reads **fail** the checker under clock skew, ReadIndex **passes**
- [ ] Benchmarks with graphs: throughput, p50/p99, failover, read-path comparison, 3 vs 5 nodes
- [ ] README with architecture, correctness method, results, and bugs found

---

## 1. Architecture

```
            ┌──────────── client (retries, leader hints, clientID+seq) ───────────┐
            ▼                                                                     │
   ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐           │
   │     Node 1      │     │     Node 2      │     │     Node 3      │           │
   │ ┌─────────────┐ │     │                 │     │                 │           │
   │ │  KV server  │ │     │      ...        │     │      ...        │           │
   │ │ dedup table │ │     │                 │     │                 │           │
   │ └──────┬──────┘ │     └────────┬────────┘     └────────┬────────┘           │
   │ ┌──────▼──────┐ │              │                       │                    │
   │ │    Raft     │◄├──────────────┴───────────────────────┘                    │
   │ └──┬───┬───┬──┘ │      RequestVote / AppendEntries / InstallSnapshot         │
   │    │   │   │    │                                                            │
   │ Storage Clock Transport ◄── Faulty wrapper ◄── Chaos controller (admin RPC) ─┘
   └─────────────────┘
```

Every outside dependency sits behind an interface. This is what makes the project testable:

| Interface | Implementations | Why |
|---|---|---|
| `Transport` | `memnet` (in-process), `grpc` (real), `faulty` (wraps either) | Same Raft code in fast tests and on a real cluster |
| `Clock` | `RealClock`, `FakeClock` (tests), `SkewedClock` (offset + drift) | Deterministic timing in tests; clock-skew injection |
| `Storage` | `FileStorage` (WAL + fsync), `MemStorage` (tests, survives simulated crash) | Crash tests without touching disk |
| `StateMachine` | `kv.Store` | Raft doesn't know it's a KV store |

### Key Go signatures (Phase 0 creates these)

```go
type NodeID string

type Peer interface {
    RequestVote(ctx context.Context, a *RequestVoteArgs) (*RequestVoteReply, error)
    AppendEntries(ctx context.Context, a *AppendEntriesArgs) (*AppendEntriesReply, error)
    InstallSnapshot(ctx context.Context, a *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

type Transport interface {
    Peer(id NodeID) Peer
    Serve(id NodeID, h Peer) error // register this node's handlers
}

type Clock interface {
    Now() time.Time
    NewTimer(d time.Duration) Timer
    NewTicker(d time.Duration) Ticker
}

type Storage interface {
    SaveHardState(term uint64, votedFor NodeID) error           // must fsync
    Append(entries []Entry) error                                // must fsync
    TruncateFrom(index uint64) error
    SaveSnapshot(s Snapshot) error                               // atomic: temp file + rename
    Load() (HardState, []Entry, *Snapshot, error)
}

type StateMachine interface {
    Apply(e Entry) []byte
    Snapshot() ([]byte, error)
    Restore(data []byte) error
}

// Raft's public API
func (r *Raft) Propose(cmd []byte) (index, term uint64, isLeader bool)
func (r *Raft) ReadIndex(ctx context.Context) (uint64, error)
func (r *Raft) AddServer(ctx context.Context, id NodeID, addr string) error
func (r *Raft) RemoveServer(ctx context.Context, id NodeID) error
func (r *Raft) ApplyCh() <-chan ApplyMsg
```

---

## 2. Directory layout

```
cmd/
  server/         one node (flags: id, peers, data-dir, ports, read-mode)
  client/         CLI: get / put / append / add-server / remove-server
  chaos/          scenario runner (memnet or docker), writes histories + verdicts
  bench/          load generator, writes CSVs
internal/
  raft/           raft.go election.go replication.go commit.go snapshot.go
                  readindex.go lease.go membership.go prevote.go persist.go
  kv/             store.go dedup.go server.go client.go
  transport/      transport.go memnet.go grpc.go faulty.go
  clock/          clock.go real.go fake.go skewed.go
  storage/        file.go mem.go
  admin/          admin RPC: set network rules, set clock skew, status
  history/        recorder + porcupine model + visualisation output
proto/            raft.proto kv.proto admin.proto
test/             cluster harness for memnet tests (start/stop/crash/partition nodes)
deploy/           docker-compose.3.yml docker-compose.5.yml Dockerfile
bench/results/    CSVs + PNG graphs
docs/             BUILDMAP.md, design notes, bug log
Makefile
```

### Makefile targets (Phase 0)

| Target | Does |
|---|---|
| `make test` | `go test ./... -race` |
| `make stress PKG=... RUN=... N=500` | Repeat one test N times with `-race`, stop on first failure |
| `make chaos N=1000` | Chaos scenarios on memnet with random seeds |
| `make chaos-docker N=20` | Chaos on the real Docker cluster |
| `make cluster-up` / `cluster-down` | Docker Compose, 3 nodes (`NODES=5` for 5) |
| `make bench` | Benchmarks to `bench/results/` |
| `make proto` | Regenerate protobuf code |

---

## 3. Conventions

- **Never hold the Raft mutex during an RPC, disk write or channel send.** Copy what you need, unlock, call, re-lock, then **re-check term and role** before using the reply.
- **All time comes from `Clock`.** No `time.Now()` or `time.Sleep` inside `internal/raft`.
- **All networking goes through `Transport`.**
- **Logging:** `log/slog` with `node`, `term`, `role` on every line. Debug is off by default; turn it on with `RAFT_DEBUG=1`.
- **Reproducible tests:** every memnet test and chaos run takes a seed and prints it on failure. Re-running with that seed reproduces the run.
- **Dependencies:** only gRPC, protobuf and Porcupine. Ask before adding anything else.
- **Bug log:** every real bug found goes into `docs/BUGS.md`, with its symptom, cause, fix and the test that catches it. These become your interview stories.
- **Index arithmetic:** every log-index calculation goes through one helper that handles the snapshot offset.
- **Persist first:** persist (and fsync) before replying to any RPC that changed persistent state.
- **Keep output short:** while iterating, run single tests with `-run`; on failure show only the last ~50 lines and the seed, never full debug logs. For chaos failures, read the saved files in `chaos-failures/`.

---

## 4. Phases

Each phase has:
- **Build:** what to implement
- **Mutation check 🔨:** insert this bug on purpose, confirm a test catches it, then revert
- **Exit:** every box must be ticked

---

### Phase 0 — Foundations (Day 1)

**Read before any code:**
- The Raft paper (Ongaro & Ousterhout, 2014), sections 5–8. **Figure 2 is the spec.**
- Ongaro's PhD thesis, chapter 4 (membership changes) and section 6.4 (reads). You'll need them in Phases 6–7.
- "Students' Guide to Raft" (Jon Gjengset), a list of common bugs.
- The visualisation at raft.github.io.

**Build:**
- [x] `go.mod`, the proto files, generated code, Makefile
- [x] The `Transport`, `Clock`, `Storage` and `StateMachine` interfaces
- [x] `memnet`: in-process network with a seeded RNG
- [x] `FakeClock` (advanced manually in tests) and `RealClock`
- [x] `MemStorage`: survives a simulated crash, so a crashed node keeps its "disk" but loses its memory
- [x] `test/` cluster harness: `StartCluster(n, seed)`, `Crash(id)`, `Restart(id)`, `Partition(groups...)`, `Heal()`, `CheckOneLeader()`, `CheckLogsAgree()`
- [x] GitHub Actions: `make test` on every push

**Exit:**
- [x] `make test` passes (no Raft logic yet, just the harness)
- [ ] You can draw Figure 2's state and RPCs from memory

---

### Phase 1 — Leader election (Days 2–3)

**Build:**
- [ ] Follower / Candidate / Leader states, terms, `votedFor`
- [ ] Randomised election timeout (150–300 ms) and heartbeats (50 ms), both from `Clock`
- [ ] `RequestVote` with the up-to-date-log check
- [ ] Step down when any request or reply has a higher term
- [ ] Reset the election timer **only** when granting a vote, receiving AppendEntries from the current leader, or starting an election

**Mutation check 🔨:**
- Remove the up-to-date-log check. A node with a stale log can win, and a later test loses committed data.
- Use a fixed timeout instead of a random one. Split votes repeat forever.

**Tests:**
- [ ] `TestInitialElection`: exactly one leader, all nodes agree on the term
- [ ] `TestReElection`: leader disconnected → new leader; old leader rejoins → it steps down
- [ ] `TestNoLeaderWithoutMajority`
- [ ] `TestManyElections`: random disconnects for 10 s; at most one leader per term at all times

**Exit:**
- [ ] `make stress RUN=Election N=500` passes


---

### Phase 2 — Log replication and commit (Days 3–5)

**Build:**
- [ ] `AppendEntries` with the `prevLogIndex`/`prevLogTerm` consistency check
- [ ] Truncate a follower's log **only** from the first conflicting entry. Never truncate entries that match
- [ ] Leader state: `nextIndex[]`, `matchIndex[]`
- [ ] Fast backtracking: replies carry `conflictTerm` and `conflictIndex`
- [ ] Commit rule: advance `commitIndex` to N only if a majority has `matchIndex >= N` **and** `log[N].term == currentTerm` (Figure 8)
- [ ] The new leader appends a **no-op** entry on election
- [ ] An apply loop that delivers committed entries to `ApplyCh` in order, exactly once

**Mutation check 🔨:**
- Commit an old-term entry by counting replicas. Reproduce Figure 8 in a test.
- Truncate on any `AppendEntries`, even when entries match. A delayed, reordered RPC erases committed entries.

**Tests:**
- [ ] `TestBasicAgree`
- [ ] `TestFailAgree`: a follower disconnects and reconnects, then catches up
- [ ] `TestFailNoAgree`: no commit without a majority
- [ ] `TestRejoin`: an old leader with uncommitted entries rejoins, and those entries are overwritten
- [ ] `TestBackup`: a follower 50+ entries behind with a conflicting log catches up quickly
- [ ] `TestFigure8`: a scripted scenario from the paper
- [ ] `TestReorderedAppends`: memnet delivers out of order; no committed entry is ever lost

**Exit:**
- [ ] All tests pass 500× with `-race`


---

### Phase 3 — Persistence (Day 6)

**Build:**
- [ ] Persist `currentTerm`, `votedFor` and log changes **before** replying to any RPC that changed them
- [ ] `FileStorage`: append-only WAL, `fsync` on every write, CRC per record, truncate a torn tail on load
- [ ] Restart path: load the state, rebuild in-memory state, start as follower

**Mutation check 🔨:**
- Don't persist `votedFor`. After a restart a node votes twice in one term, and two leaders appear in the same term.

**Tests:**
- [ ] `TestPersistBasic`: restart all nodes, committed entries survive
- [ ] `TestPersistChurn`: random crash/restart during replication for 30 s; logs agree
- [ ] `TestTornWrite`: cut the WAL off in the middle of a record; the node loads cleanly
- [ ] `TestNoDoubleVote`: crash right after voting; the node doesn't vote again in that term

**Exit:**
- [ ] All tests pass 500×
- [ ] You can list what's persisted, when, and what breaks without each


---

### Phase 4 — KV store and client (Days 7–8)

**Build:**
- [ ] `Get`, `Put` and `Append` all go through the log (later phases speed up reads)
- [ ] Every client has a `clientID` and a monotonically increasing `seq`
- [ ] Dedup table `clientID → (lastSeq, lastResult)`. A retry returns the cached result instead of applying again
- [ ] The server waits for its log index to apply. If a different entry lands there, it returns `ErrWrongLeader`
- [ ] Client: leader hints, rotating through servers, retries with backoff, timeouts

**Mutation check 🔨:**
- Remove dedup, then drop replies. An `Append` gets applied twice.

**Tests:**
- [ ] `TestKVBasic`
- [ ] `TestKVConcurrentClients`: 10 clients, unreliable network, leader churn
- [ ] `TestKVAppendExactlyOnce`: every appended token appears exactly once
- [ ] `TestKVPartitionedLeader`: a client on the minority side never gets a stale success

**Exit:**
- [ ] All tests pass 200×


---

### Phase 5 — Snapshots (Days 9–10)

**Build:**
- [ ] The KV server snapshots its **store, dedup table and current config** once the log passes `maxLogBytes`
- [ ] Raft discards entries covered by the snapshot and keeps `lastIncludedIndex` and `lastIncludedTerm`
- [ ] All index arithmetic is offset by `lastIncludedIndex`, through one helper. No raw slice indexing
- [ ] `InstallSnapshot` for followers that are behind the leader's snapshot
- [ ] Snapshots are written atomically (temp file, fsync, rename)

**Mutation check 🔨:**
- Leave the dedup table out of the snapshot. After a restart, retried appends are duplicated.

**Tests:**
- [ ] `TestSnapshotBoundedLog`: log size stays under the limit under continuous load
- [ ] `TestSnapshotInstall`: a node offline for 10k entries catches up
- [ ] `TestSnapshotRestart`: restart from snapshot plus log tail
- [ ] `TestSnapshotChurn`: crashes + partitions + snapshots for 60 s
- [ ] All earlier tests still pass

**Exit:**
- [ ] All tests pass 200×


---

### Phase 6 — Linearizable reads: ReadIndex and leases (Days 11–12)

**Build:**
- [ ] **ReadIndex** (thesis §6.4):
  1. The leader must have committed an entry in its current term (the no-op)
  2. Record `readIndex = commitIndex`
  3. Confirm leadership with one heartbeat round acked by a majority
  4. Wait until `lastApplied >= readIndex`, then serve the read locally
- [ ] Batch concurrent reads so they share one heartbeat round
- [ ] **Lease reads** (only with `--read-mode=lease`): the leader serves reads locally if `now < lastMajorityAck + electionTimeoutMin - margin`, measured on **its own `Clock`**
- [ ] Server flag `--read-mode=log|readindex|lease`

**Mutation check 🔨:**
- Skip step 3. A partitioned old leader serves stale reads. (The Phase 10 checker must catch this.)

**Tests:**
- [ ] `TestReadIndexBasic`
- [ ] `TestReadIndexPartitionedLeader`: an isolated old leader cannot serve reads
- [ ] `TestLeaseReadSkew`: with a slow-drift `SkewedClock` on the leader, a lease read **can** return stale data. This test should **show** the violation

**Exit:**
- [ ] ReadIndex tests pass 200×
- [ ] The lease skew test reliably produces a stale read


---

### Phase 7 — Membership changes and PreVote (Days 13–15)

**Build:**
- [ ] Config entries in the log. Each node uses the **latest config in its log, committed or not**
- [ ] **AddServer:**
  1. Add as a **learner** (non-voting) and replicate
  2. Catch up in rounds; each round must finish within an election timeout, max 10 rounds, else fail
  3. Append the config entry that makes it a voter
- [ ] **RemoveServer:** append a config without the server. If the leader removes itself, it keeps leading until that entry commits (without counting itself in majorities), then steps down
- [ ] Only **one change at a time**: reject while an uncommitted config entry exists
- [ ] The leader must commit an entry in its **current term** before accepting a change (fixes the known single-server-change bug Ongaro found in 2015)
- [ ] **PreVote**: a candidate first checks it could win before raising its term. This stops removed or partitioned nodes from disrupting the cluster
- [ ] Ignore `RequestVote` within the minimum election timeout of hearing from a current leader
- [ ] Client and CLI: `add-server`, `remove-server`

**Mutation check 🔨:**
- Allow two concurrent config changes. Construct a case with two leaders in one term.
- Disable PreVote. A removed node keeps forcing elections, and throughput collapses.

**Tests:**
- [ ] `TestAddServerUnderLoad`: no failed client ops during the add
- [ ] `TestRemoveFollower`, `TestRemoveLeader`
- [ ] `TestGrowShrink`: 3 → 5 → 3 nodes under load and partitions
- [ ] `TestConcurrentChangeRejected`
- [ ] `TestRemovedNodeNoDisruption`: with PreVote, the removed node causes no term increases
- [ ] `TestRejoinAfterPartitionPreVote`: a partitioned node rejoins without bumping the term

**Exit:**
- [ ] All tests pass 200×


---

### Phase 8 — Real cluster over gRPC (Day 16)

**Build:**
- [ ] A `grpc` transport implementing `Transport`, with per-RPC deadlines
- [ ] `cmd/server` with flags and a data directory per node
- [ ] Admin service: status (role, term, commit index, config), network rules, clock skew
- [ ] `Dockerfile`, and `docker-compose.3.yml` / `docker-compose.5.yml` with a volume per node
- [ ] `cmd/client` CLI

**Exit:**
- [ ] `make cluster-up`, then `docker kill` the leader: writes resume after the new election
- [ ] `docker restart` a node: it catches up from disk
- [ ] Add a 4th container with `add-server`, then remove it

---

### Phase 9 — Fault injector (Days 17–18)

**Network (the `faulty` transport, controlled through admin RPC):**
- [ ] Rules per (from, to) pair: block, drop %, delay range, duplicate %, reorder
- [ ] Partition helpers: majority/minority split, isolate the leader, bridge (one node sees both sides), one-way link

**Process:**
- [ ] Kill/restart (`docker kill` / `start`), and pause/resume (`docker pause`, which freezes a node in place)

**Clock (`SkewedClock`, set through admin RPC):**
- [ ] Offset (a jump forward or back) and drift rate (e.g. 0.7× to 1.3×)
- [ ] Applies to election timers, heartbeats and lease checks

**Scenario runner (`cmd/chaos`):**
- [ ] Inputs: scenario, seed, duration, number of clients, read mode, target (memnet or docker)
- [ ] Clients run a mixed get/put/append load throughout
- [ ] Output: history file, verdict, seed, fault timeline

**Scenarios:**

| Scenario | Faults | Expected |
|---|---|---|
| `isolate-leader` | Leader cut off every 5 s | 0 violations |
| `random-partitions` | Random splits every 2–5 s | 0 violations |
| `bridge` | One node sees both halves | 0 violations, progress continues |
| `flaky-net` | 20% drops, 0–200 ms delay, duplicates, reordering | 0 violations |
| `crash-restart` | Random kill/restart every 3 s | 0 violations |
| `pause` | Pause the leader for longer than an election timeout | 0 violations |
| `membership-chaos` | Add/remove servers during partitions | 0 violations |
| `clock-skew-readindex` | Drift 0.7×–1.3× on all nodes, ReadIndex | **0 violations** |
| `clock-skew-lease` | Slow drift on the leader, lease reads | **violations expected** (the demo) |
| `everything` | All of the above at random | 0 violations |

**Exit:**
- [ ] Every scenario runs on memnet and on docker

---

### Phase 10 — Linearizability checking (Days 19–20)

**Build:**
- [ ] Recorder: wrap every client op and record `(clientID, op, key, value, output, callTime, returnTime)`
- [ ] **Timed-out ops are unknown.** Set the return time to the end of the history, and have the model accept any output for them
- [ ] Porcupine KV model, **partitioned by key** so checking stays fast
- [ ] Check timeout per run (e.g. 60 s); report "unknown" separately from "pass"
- [ ] On a violation, save the history, the seed, the fault timeline and Porcupine's HTML visualisation to `chaos-failures/`
- [ ] `make chaos N=1000` runs random scenarios and seeds and prints a pass/fail table

**Mutation check 🔨 (each must be caught):**
- Reads served from local state with no ReadIndex
- No dedup
- Committing old-term entries
- Lease reads under skew (expected)

**Exit:**
- [ ] 0 violations across 1,000+ memnet runs and 50+ docker runs, with every non-lease scenario included
- [ ] Every bug in the 🔨 list is caught
- [ ] The lease-under-skew violation is saved as a visualisation for the README


---

### Phase 11 — Benchmarks (Day 21)

Use closed-loop clients with 128 B values. Each config runs 60 s after a 10 s warm-up, repeated 3×; report the median.

| Benchmark | Variables | Output |
|---|---|---|
| Throughput and latency | 3 vs 5 nodes × 1/8/32/64 clients | ops/s, p50, p99 |
| Read paths | log vs readindex vs lease | read p50/p99, ops/s |
| Failover | Kill the leader, 20 trials | Time to first successful write: median and p99 |
| Membership | Add a server under load | Throughput dip and duration |
| Catch-up | A node 10k / 100k entries behind | Time to catch up (snapshot vs log) |
| Under chaos | `random-partitions` running | ops/s, % of failed ops |

- [ ] Write CSVs to `bench/results/` and plot PNGs
- [ ] Record the machine specs (CPU, RAM, disk) next to the results

---

### Phase 12 — Hardening, README and resume (Days 22–24)

- [ ] Run `make chaos N=5000` overnight. Fix everything, and log each bug in `docs/BUGS.md`
- [ ] Run the whole test suite 1,000× with `-race`
- [ ] Comment every tricky invariant in the code, citing the paper section
- [ ] README:
  1. One paragraph plus the architecture diagram
  2. A 3-command quickstart
  3. **Correctness:** the fault model, the scenarios, how many runs, 0 violations, how the checker works
  4. **The lease vs ReadIndex demo**, with the Porcupine visualisation
  5. Benchmarks with graphs and machine specs
  6. The 3 best bugs from `docs/BUGS.md`
  7. Design decisions and limits (single Raft group, no sharding)

---

## 5. Daily loop with Claude Code

1. New session: "Read `docs/BUILDMAP.md`. Do Phase N."
2. Write the phase's tests first, then the implementation.
3. Run the targeted tests, then the stress command.
4. Run the 🔨 mutation checks: insert each bug, confirm a test fails, revert.
5. Tick the phase's boxes in this file.
6. Commit: `phase N: <summary>`.
7. Push to GitHub right after each phase commit, one phase per push. Never batch several phases into one push.

---

## 6. Interview checklist

You should be able to answer all of these without notes:

1. Why does a node refuse to vote for a candidate whose log is behind its own?
2. Why can't a leader commit an old-term entry by counting replicas? (Figure 8)
3. What must be persisted before replying, and what breaks without each piece?
4. The leader crashes after replicating your write but before replying. What happens?
5. How do retried requests avoid being applied twice?
6. How does ReadIndex work, and why does it need a current-term commit?
7. Exactly how does clock drift break lease reads? (Show your Porcupine visualisation.)
8. Why add learners before voters? Why only one config change at a time?
9. What does PreVote fix?
10. What is linearizability, and how does Porcupine check it?
11. What was the hardest bug you found, and how did you find it?

---

## 7. Resume bullets (fill with real numbers)

**RaftKV: Fault-Tolerant Replicated KV Store** | Go, gRPC, Docker, Porcupine

- Built a Raft-replicated KV store with snapshots, ReadIndex reads and live membership changes across [3–5] gRPC nodes, sustaining [X] ops/s at [Y] ms p99 with [Z] ms leader failover.
- Proved linearizability under partitions, crashes and clock skew with a custom fault injector and the Porcupine checker: 0 violations across [N] chaos runs, catching [K] bugs during development.
- Showed lease reads return stale data under [D]× clock drift while ReadIndex stays correct, with [P]% lower read latency for leases (shown with a Porcupine trace).

---

## 8. Common pitfalls

- Holding the mutex during an RPC, disk write or channel send leads to deadlock
- Using a reply without re-checking term and role (state changed while you waited)
- Resetting the election timer on every RPC instead of only the three valid cases
- Off-by-one errors after snapshots: route every index through one helper
- Applying entries twice or out of order after `InstallSnapshot`
- Treating timed-out client ops as failed (they may have succeeded)
- Counting the leader in majorities after it removed itself from the config
- Reading `time.Now()` directly, which bypasses `Clock` so skew injection does nothing
