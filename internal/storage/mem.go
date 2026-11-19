// Package storage holds Storage implementations.
package storage

import (
	"errors"
	"sync"

	"github.com/panavdoesstuff/raftkv/internal/raft"
)

// MemStorage is an in-memory "disk". A simulated crash discards the node but
// keeps this object, so the restarted node sees exactly what was durable.
type MemStorage struct {
	mu   sync.Mutex
	hs   raft.HardState
	log  []raft.Entry // contiguous, ascending Index
	snap *raft.Snapshot
}

var _ raft.Storage = (*MemStorage)(nil)

func NewMem() *MemStorage { return &MemStorage{} }

func (m *MemStorage) SaveHardState(term uint64, votedFor raft.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = raft.HardState{Term: term, VotedFor: votedFor}
	return nil
}

func (m *MemStorage) Append(entries []raft.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range entries {
		if n := len(m.log); n > 0 && e.Index != m.log[n-1].Index+1 {
			return errors.New("storage: non-contiguous append")
		}
		m.log = append(m.log, e.Clone())
	}
	return nil
}

func (m *MemStorage) TruncateFrom(index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range m.log {
		if e.Index >= index {
			m.log = m.log[:i]
			break
		}
	}
	return nil
}

func (m *MemStorage) SaveSnapshot(s raft.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snap != nil && s.LastIncludedIndex <= m.snap.LastIncludedIndex {
		return nil // older than what we hold
	}
	s.Data = append([]byte(nil), s.Data...)
	s.Config = append([]byte(nil), s.Config...)
	m.snap = &s
	kept := m.log[:0:0]
	for _, e := range m.log {
		if e.Index > s.LastIncludedIndex {
			kept = append(kept, e)
		}
	}
	m.log = kept
	return nil
}

func (m *MemStorage) Load() (raft.HardState, []raft.Entry, *raft.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var snap *raft.Snapshot
	if m.snap != nil {
		c := *m.snap
		c.Data = append([]byte(nil), c.Data...)
		c.Config = append([]byte(nil), c.Config...)
		snap = &c
	}
	return m.hs, raft.CloneEntries(m.log), snap, nil
}
