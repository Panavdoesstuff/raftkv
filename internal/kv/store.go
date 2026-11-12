// Package kv is the replicated key-value state machine, its server and its
// client. Raft doesn't know it is a KV store (BUILDMAP §1).
package kv

import (
	"bytes"
	"encoding/gob"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
)

// Store is the state machine: the map plus the dedup table. Both are mutated
// only by Apply (called in log order by Raft's applier), so every replica
// computes the same thing.
type Store struct {
	mu    sync.Mutex
	data  map[string]string
	dedup map[string]dedupEntry // clientID -> last request and its result

	// onApply, if set, is told about every applied entry (the server uses it
	// to wake the request waiting on that index). Called with mu released.
	onApply func(index, term uint64, reply *kvpb.CommandReply)
}

func NewStore() *Store {
	return &Store{data: map[string]string{}, dedup: map[string]dedupEntry{}}
}

// Encode serializes a command for the log.
func Encode(a *kvpb.CommandArgs) []byte {
	b, err := proto.Marshal(a)
	if err != nil {
		panic(err) // a plain message of strings and ints can't fail
	}
	return b
}

// Apply executes one committed entry. A request whose (clientID, seq) was
// already applied is not executed again: the cached result is returned
// (BUILDMAP §4, Phase 4; DESIGN Q5).
func (s *Store) Apply(e raft.Entry) []byte {
	a := &kvpb.CommandArgs{}
	if err := proto.Unmarshal(e.Data, a); err != nil {
		return nil // not a KV command; can't happen for entries we proposed
	}
	s.mu.Lock()
	rep := s.applyLocked(a)
	hook := s.onApply
	s.mu.Unlock()
	if hook != nil {
		hook(e.Index, e.Term, rep)
	}
	b, _ := proto.Marshal(rep)
	return b
}

func (s *Store) applyLocked(a *kvpb.CommandArgs) *kvpb.CommandReply {
	if d, ok := s.dedup[a.ClientId]; ok && a.Seq <= d.Seq {
		if a.Seq == d.Seq {
			return d.reply()
		}
		// Older than the client's latest request: the client has moved on
		// (one outstanding request per client), so nobody is waiting.
		return &kvpb.CommandReply{Err: kvpb.Err_OK}
	}
	rep := &kvpb.CommandReply{Err: kvpb.Err_OK}
	switch a.Op {
	case kvpb.Op_GET:
		v, ok := s.data[a.Key]
		if !ok {
			rep.Err = kvpb.Err_ERR_NO_KEY
		}
		rep.Value = v
	case kvpb.Op_PUT:
		s.data[a.Key] = a.Value
	case kvpb.Op_APPEND:
		s.data[a.Key] += a.Value
	}
	s.dedup[a.ClientId] = dedupEntry{Seq: a.Seq, Err: rep.Err, Value: rep.Value}
	return rep
}

type dedupEntry struct {
	Seq   uint64
	Err   kvpb.Err
	Value string
}

func (d dedupEntry) reply() *kvpb.CommandReply {
	return &kvpb.CommandReply{Err: d.Err, Value: d.Value}
}

// Peek reads a key without going through the log. Tests only: it is NOT a
// linearizable read.
func (s *Store) Peek(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

type snapshotV struct {
	Data  map[string]string
	Dedup map[string]dedupEntry
}

// Snapshot and Restore cover the store and the dedup table together; leaving
// the dedup table out would make retries after a restart apply twice.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshotV{s.data, s.dedup}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *Store) Restore(b []byte) error {
	var v snapshotV
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&v); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.dedup = v.Data, v.Dedup
	if s.data == nil {
		s.data = map[string]string{}
	}
	if s.dedup == nil {
		s.dedup = map[string]dedupEntry{}
	}
	return nil
}
