package storage

import (
	"reflect"
	"testing"

	"github.com/panavdoesstuff/raftkv/internal/raft"
)

func ents(term uint64, from, to uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: []byte{byte(i)}})
	}
	return out
}

func TestMemRoundTrip(t *testing.T) {
	s := NewMem()
	s.SaveHardState(3, "n2")
	s.Append(ents(1, 1, 5))
	hs, log, snap, err := s.Load()
	if err != nil || snap != nil {
		t.Fatalf("err=%v snap=%v", err, snap)
	}
	if hs.Term != 3 || hs.VotedFor != "n2" {
		t.Fatalf("hardstate %+v", hs)
	}
	if !reflect.DeepEqual(log, ents(1, 1, 5)) {
		t.Fatalf("log %+v", log)
	}
}

func TestMemSurvivesCrash(t *testing.T) {
	// "Crash" = drop every reference to the node; the storage object remains.
	s := NewMem()
	s.SaveHardState(7, "n1")
	s.Append(ents(7, 1, 3))
	hs, log, _, _ := s.Load()
	if hs.Term != 7 || len(log) != 3 {
		t.Fatalf("state lost: %+v %d", hs, len(log))
	}
}

func TestMemTruncateFrom(t *testing.T) {
	s := NewMem()
	s.Append(ents(1, 1, 5))
	s.TruncateFrom(3)
	_, log, _, _ := s.Load()
	if len(log) != 2 || log[1].Index != 2 {
		t.Fatalf("log %+v", log)
	}
	// Append after truncation continues from the new tail.
	if err := s.Append(ents(2, 3, 4)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ents(2, 9, 9)); err == nil {
		t.Fatal("non-contiguous append accepted")
	}
}

func TestMemSnapshotDropsCoveredEntries(t *testing.T) {
	s := NewMem()
	s.Append(ents(1, 1, 10))
	s.SaveSnapshot(raft.Snapshot{LastIncludedIndex: 6, LastIncludedTerm: 1, Data: []byte("x")})
	_, log, snap, _ := s.Load()
	if snap == nil || snap.LastIncludedIndex != 6 || string(snap.Data) != "x" {
		t.Fatalf("snap %+v", snap)
	}
	if len(log) != 4 || log[0].Index != 7 {
		t.Fatalf("log %+v", log)
	}
}

func TestMemLoadReturnsCopies(t *testing.T) {
	s := NewMem()
	s.Append(ents(1, 1, 1))
	_, log, _, _ := s.Load()
	log[0].Data[0] = 99
	_, again, _, _ := s.Load()
	if again[0].Data[0] == 99 {
		t.Fatal("Load aliases internal state")
	}
}
