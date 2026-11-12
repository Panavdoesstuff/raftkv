// Package raft holds the Raft core types and the interfaces that isolate it
// from the outside world (network, disk, state machine). Phase 0 defines only
// the contracts; the algorithm arrives in Phases 1+.
package raft

import (
	"context"
	"fmt"
)

type NodeID string

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryNoop
	EntryConfig
)

// Entry is one log entry. Index is its absolute log index (1-based; 0 means
// "before the first entry").
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

func (e Entry) Clone() Entry {
	e.Data = append([]byte(nil), e.Data...)
	return e
}

func CloneEntries(es []Entry) []Entry {
	if es == nil {
		return nil
	}
	out := make([]Entry, len(es))
	for i, e := range es {
		out[i] = e.Clone()
	}
	return out
}

// HardState is what Raft must persist (and fsync) before answering any RPC
// that changed it (Figure 2, "Persistent state").
type HardState struct {
	Term     uint64
	VotedFor NodeID
}

// Snapshot covers the log through LastIncludedIndex.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictTerm  uint64
	ConflictIndex uint64
}

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Clone helpers let in-process transports hand each side its own copy, so a
// node can never observe another node's later mutations.
func (a *AppendEntriesArgs) Clone() *AppendEntriesArgs {
	c := *a
	c.Entries = CloneEntries(a.Entries)
	return &c
}

func (a *InstallSnapshotArgs) Clone() *InstallSnapshotArgs {
	c := *a
	c.Data = append([]byte(nil), a.Data...)
	return &c
}

// Peer is the RPC surface of a node. A Transport returns a Peer for talking to
// a remote node; a node registers its own implementation with Serve.
type Peer interface {
	RequestVote(ctx context.Context, a *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, a *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, a *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Transport is one node's view of the network.
type Transport interface {
	Peer(id NodeID) Peer
	Serve(id NodeID, h Peer) error // register this node's handlers
}

// Storage is a node's durable state. Every mutating method must be durable
// (fsync'd) before it returns.
type Storage interface {
	SaveHardState(term uint64, votedFor NodeID) error
	Append(entries []Entry) error
	// TruncateFrom deletes every entry with Index >= index.
	TruncateFrom(index uint64) error
	// SaveSnapshot atomically stores s and drops log entries it covers.
	SaveSnapshot(s Snapshot) error
	// Load returns the persisted state. Entries are those after the snapshot.
	Load() (HardState, []Entry, *Snapshot, error)
}

// StateMachine is the replicated application (the KV store).
type StateMachine interface {
	Apply(e Entry) []byte
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// ApplyMsg is delivered on ApplyCh for each committed entry, in order, once.
type ApplyMsg struct {
	Entry  Entry
	Result []byte
}

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("role(%d)", uint8(r))
}

// Status is a point-in-time view of a node, used by tests and the admin RPC.
type Status struct {
	ID          NodeID
	Role        Role
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
}
