# RaftKV — Design Notes

Answers to the design questions in BUILDMAP §6, added at the end of each phase. Each answer names the code or test that enforces it.

## Phase 1 — Leader election

### 1. Why does a node refuse to vote for a candidate whose log is behind its own?

A vote is a promise that the candidate's log can be the future source of truth. If a node with a stale log wins, it can overwrite entries that a majority already holds, including committed ones. The rule (Raft §5.4.1) makes every winner hold every committed entry: committing needs a majority, winning needs a majority, and any two majorities share a node, so some voter holds the committed entry and refuses a candidate that lacks it.

"Up to date" compares the last entry's **term** first (higher wins), and on equal terms the **index** (longer wins). Comparing length alone is wrong: a long log of old-term entries can hold uncommitted junk while a shorter log ends in a newer term.

- Code: `handleRequestVote` in [election.go](../internal/raft/election.go).
- Test: `TestRequestVoteLogUpToDate` in [vote_test.go](../internal/raft/vote_test.go).
- Mutation: removing the check lets a stale node win and a later test loses committed data.

### Other election rules worth remembering

- **One vote per term:** `votedFor` is cleared only when the term increases, so a node never votes twice in a term. `TestVoteOncePerTerm`.
- **Randomised timeouts (150–300 ms)** break split votes. A fixed timeout makes candidates collide forever.
- **The election timer resets in exactly three cases:** granting a vote, accepting AppendEntries from the current leader, and starting an election. Resetting on every RPC lets a deposed or partitioned node suppress elections indefinitely. `TestElectionTimerResetRules`.
- **A leader that steps down re-arms its timer.** Its deadline is stale from its own election, so without the re-arm it would start an election on the next tick.

## Phase 2 — Log replication and commit

### 2. Why can't a leader commit an old-term entry by counting replicas? (Figure 8)

Because a majority copy of an old-term entry can still be overwritten. Scripted in `TestFigure8` ([replication_test.go](../internal/raft/replication_test.go)):

1. S1 leads term 2 and puts index 2 on S2 only, then crashes.
2. S5 wins term 3 with votes from S3 and S4, appends a different entry at index 2, then crashes.
3. S1 returns and wins term 4. It re-replicates its term-2 entry at index 2 to S2 and S3. Now a majority (S1, S2, S3) holds it.
4. If S1 committed it now and then crashed, S5 could win term 5. S5's last log term is 3, which beats the term 2 at the end of the logs on S1, S2 and S3, so those nodes vote for it. S5 overwrites index 2 everywhere. An entry that was "committed" disappears.

The fix: a leader commits only entries **from its own term** by counting replicas (`advanceCommit` in [replication.go](../internal/raft/replication.go)). Once a current-term entry is on a majority, every entry before it is committed indirectly (Log Matching). Once a term-4 entry is on a majority, S5 can no longer win: the voters' logs end in term 4, which beats S5's term 3.

That is also why a new leader appends a **no-op** on election: it gives it a current-term entry to commit, which commits everything inherited from earlier terms without waiting for a client write.

### Truncation rule

A follower truncates only from the first entry that **conflicts** (same index, different term). If an RPC's entries match what the follower already holds, nothing is removed. Otherwise a delayed, duplicated or reordered AppendEntries carrying a log prefix would erase newer, possibly committed entries. `TestReorderedAppends`.

## Phase 3 — Persistence

### 3. What must be persisted before replying, and what breaks without each piece?

| State | Persisted before | Breaks without it |
|---|---|---|
| `currentTerm` | Replying to or sending anything carrying the new term | A restarted node accepts stale leaders and candidates; term numbers can go backwards, so a stale leader appears legitimate. `TestPersistBasic`. |
| `votedFor` | Granting a vote; sending `RequestVote` (the candidate's own vote) | After a restart the node votes again in the same term. Two candidates both get a majority: two leaders in one term (Election Safety). `TestNoDoubleVote`. |
| Log entries | Acking AppendEntries; the leader counting its own copy toward commit | A follower acks, crashes and forgets, so a "committed" entry is on fewer than a majority. A later leader without it overwrites it: committed data lost. `TestFigure8Unreliable`, `TestPersistChurn`. |
| Log truncation | Appending the replacement entries | A restart resurrects entries the leader had overwritten, on the same indexes as the new ones. |

**Not persisted:** `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`, role. A restarted node comes back as a follower with `commitIndex = 0` and re-learns it from the leader; the state machine is rebuilt by replaying the log (or snapshot, from Phase 5).

**`TestPersistBeforeReply`** reads the disk the moment each RPC returns, so a missing persist fails whichever path skipped it.

**Storage errors halt the node** (`halt` in [raft.go](../internal/raft/raft.go)): memory may now be ahead of disk, and carrying on could acknowledge something that is not durable.

### FileStorage and torn writes

The WAL is append-only. Each record is `[len][crc32c][type][payload]` and is fsynced on every write, with a directory fsync when files are created or renamed.

On load:

- **Incomplete or CRC-failing final record:** a torn write, i.e. the process died mid-append. That write was never acknowledged (the fsync had not returned), so dropping it loses nothing. The file is truncated to the last good record before any new append.
- **Bad record with valid data after it:** not a torn write. The disk is damaged, and truncating there would silently discard acknowledged entries, so `OpenFile` returns `ErrCorrupt`.

Tests: `TestFileTornTail` (cut at every byte of the last record), `TestFileCorruptMiddleIsError`, `TestTornWrite`.

### Why disk writes happen under the Raft mutex

This is a deliberate exception to the "no disk write under the lock" rule. State must be durable before the reply, and the leader's own entry counts toward commit only once durable. Doing the write outside the lock would need write ordering plus a durable-index in the commit count. Storage never calls back into Raft, so this can stall but not deadlock. Phase 11 measures the cost and implements group commit if it limits throughput.

## Phase 4 — KV store and client

### 4. The leader crashes after replicating your write but before replying. What happens?

The client sees a timeout or error, which means **unknown outcome**, not failure. The entry may be on a majority, in which case the next leader commits it (its no-op commit covers inherited entries), or it may not be, in which case it is overwritten. The client can't tell which, so it retries the same `(clientID, seq)` against another server. If the write had committed, the dedup table returns the cached result and nothing is applied twice; if not, it is applied now, once. `TestKVConcurrentClients` crashes leaders under load and `TestKVAppendExactlyOnce` drops half the replies.

A server that proposed at index *i* in term *t* also stops waiting once it is no longer leader of *t*, or when an entry with a different term applies at *i*, and answers `ErrWrongLeader` ([server.go](../internal/kv/server.go)). The entry may still commit later at another index; that is fine for the same reason.

### 5. How do retried requests avoid being applied twice?

Each client has an ID and a counter, with one request outstanding at a time. A retry reuses the same `seq`. The dedup table `clientID → (lastSeq, lastResult)` lives **inside the state machine** and is updated in `Apply` ([store.go](../internal/kv/store.go)), so every replica makes the same decision from the same log. A request with `seq <= lastSeq` is not executed; `seq == lastSeq` returns the cached result. Putting dedup in `Apply` rather than in the server matters: a retry that reaches a new leader after failover is checked against a table rebuilt by replaying the same log.

Gets are deduped too, so a retried Get returns the value from its first execution, which is inside the client's call interval and therefore still linearizable.

The table is part of `Snapshot`/`Restore` so a restart from a snapshot (Phase 5) doesn't forget it.

Tests: `TestKVDedupDirect` (same seq twice, no network loss), `TestKVAppendExactlyOnce` (50% lost replies, every token exactly once and in order). Mutation: disabling the `seq` check fails both plus `TestKVConcurrentClients`.

### Partitioned leader

A leader in a minority can accept a proposal but never commits it, so its server never gets an apply and the request ends in `ErrTimeout` or `ErrWrongLeader`; the client never sees success. Reads also go through the log in this phase, so a stale read is impossible by construction. `TestKVPartitionedLeader`.

### Lost leadership: the term check

The server proposes at index *i* in term *t* and waits. If it is partitioned, a new leader may commit a different entry at *i*, and after the heal the old leader applies that entry at *i*. Index alone can't tell the waiter its own request from the replacement, so the waiter compares the applied entry's **term** with the one it proposed in, and answers `ErrWrongLeader` on a mismatch ([server.go](../internal/kv/server.go)). The 50 ms "still leader of my term?" check usually catches this first, but it is a poll, so the term check is what makes it deterministic.

`TestKVLostLeadershipNoFalseSuccess`: the old leader takes two Puts at N and N+1, is partitioned, the majority puts its own no-op at N and its own Put at N+1, then the partition heals. (The new leader's no-op never reaches `Apply`, so only the Put at N+1 exercises the check.) Mutation: removing the term check makes the Put at N+1 return success; 10 or more of 20 runs fail.
