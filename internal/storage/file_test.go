package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/panavdoesstuff/raftkv/internal/raft"
)

func openT(t *testing.T, dir string) *FileStorage {
	t.Helper()
	s, err := OpenFile(dir)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func sameEntries(t *testing.T, got, want []raft.Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d: %+v", len(got), len(want), got)
	}
	for i := range got {
		g, w := got[i], want[i]
		if g.Index != w.Index || g.Term != w.Term || g.Type != w.Type || !bytes.Equal(g.Data, w.Data) {
			t.Fatalf("entry %d: got %+v, want %+v", i, g, w)
		}
	}
}

func walSize(t *testing.T, dir string) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, WALFile))
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func TestFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	if hs, log, snap, err := s.Load(); err != nil || snap != nil || len(log) != 0 || hs != (raft.HardState{}) {
		t.Fatalf("fresh dir: %+v %v %v %v", hs, log, snap, err)
	}
	must(t, s.SaveHardState(3, "n2"))
	must(t, s.Append(ents(1, 1, 5)))
	must(t, s.Close())

	s = openT(t, dir)
	hs, log, snap, err := s.Load()
	if err != nil || snap != nil {
		t.Fatalf("err=%v snap=%v", err, snap)
	}
	if hs.Term != 3 || hs.VotedFor != "n2" {
		t.Fatalf("hardstate %+v", hs)
	}
	sameEntries(t, log, ents(1, 1, 5))
}

func TestFileHardStateLastWins(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	must(t, s.SaveHardState(1, "n0"))
	must(t, s.SaveHardState(2, ""))
	must(t, s.SaveHardState(2, "n1"))
	must(t, s.Close())
	hs, _, _, _ := openT(t, dir).Load()
	if hs.Term != 2 || hs.VotedFor != "n1" {
		t.Fatalf("hardstate %+v", hs)
	}
}

func TestFileTruncateFrom(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	must(t, s.Append(ents(1, 1, 5)))
	must(t, s.TruncateFrom(3))
	must(t, s.Append(ents(2, 3, 4)))
	_, log, _, _ := s.Load()
	want := append(ents(1, 1, 2), ents(2, 3, 4)...)
	sameEntries(t, log, want)
	must(t, s.Close())
	_, log, _, _ = openT(t, dir).Load()
	sameEntries(t, log, want)
}

func TestFileAppendNonContiguous(t *testing.T) {
	s := openT(t, t.TempDir())
	must(t, s.Append(ents(1, 1, 3)))
	if err := s.Append(ents(1, 5, 6)); err == nil {
		t.Fatal("gap in log accepted")
	}
}

// TestFileTornTail cuts the WAL at every byte inside the last record. Open
// must succeed, drop exactly that record, repair the file, and keep working.
func TestFileTornTail(t *testing.T) {
	src := t.TempDir()
	s := openT(t, src)
	must(t, s.SaveHardState(2, "n1"))
	must(t, s.Append(ents(1, 1, 2)))
	before := walSize(t, src)
	must(t, s.Append(ents(2, 3, 3)))
	after := walSize(t, src)
	must(t, s.Close())
	full, err := os.ReadFile(filepath.Join(src, WALFile))
	if err != nil {
		t.Fatal(err)
	}

	for cut := before; cut < after; cut++ {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, WALFile), full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		s, err := OpenFile(dir)
		if err != nil {
			t.Fatalf("cut at %d/%d: Open failed: %v", cut, after, err)
		}
		hs, log, _, _ := s.Load()
		if hs.Term != 2 || hs.VotedFor != "n1" {
			t.Fatalf("cut at %d: hardstate %+v", cut, hs)
		}
		sameEntries(t, log, ents(1, 1, 2))
		if got := walSize(t, dir); got != before {
			t.Fatalf("cut at %d: file not repaired, size %d want %d", cut, got, before)
		}
		// The repaired log must accept the entry again and survive a reopen.
		must(t, s.Append(ents(2, 3, 3)))
		must(t, s.Close())
		_, log, _, _ = openT(t, dir).Load()
		sameEntries(t, log, append(ents(1, 1, 2), ents(2, 3, 3)...))
	}
}

func TestFileTornTailGarbage(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	must(t, s.Append(ents(1, 1, 3)))
	good := walSize(t, dir)
	must(t, s.Close())
	for _, junk := range [][]byte{
		make([]byte, 8),                   // zero-filled tail (preallocated / lost page)
		{0xff, 0xff, 0xff, 0xff, 1, 2, 3}, // absurd length
		{9},                               // partial header
	} {
		f, _ := os.OpenFile(filepath.Join(dir, WALFile), os.O_WRONLY|os.O_APPEND, 0o644)
		f.Write(junk)
		f.Close()
		s := openT(t, dir)
		_, log, _, err := s.Load()
		if err != nil {
			t.Fatal(err)
		}
		sameEntries(t, log, ents(1, 1, 3))
		if got := walSize(t, dir); got != good {
			t.Fatalf("junk %v: size %d want %d", junk, got, good)
		}
		s.Close()
	}
}

// A bad CRC on the final record is a torn write: drop it.
func TestFileCorruptLastRecordDropped(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	must(t, s.Append(ents(1, 1, 2)))
	before := walSize(t, dir)
	must(t, s.Append(ents(1, 3, 3)))
	must(t, s.Close())
	flipByte(t, filepath.Join(dir, WALFile), walSize(t, dir)-1)
	_, log, _, err := openT(t, dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	sameEntries(t, log, ents(1, 1, 2))
	if got := walSize(t, dir); got != before {
		t.Fatalf("size %d want %d", got, before)
	}
}

// A bad CRC with valid data after it is not a torn write; silently truncating
// there would throw away acknowledged entries, so Open must refuse.
func TestFileCorruptMiddleIsError(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	must(t, s.Append(ents(1, 1, 1)))
	first := walSize(t, dir)
	must(t, s.Append(ents(1, 2, 4)))
	must(t, s.Close())
	flipByte(t, filepath.Join(dir, WALFile), first-1) // last byte of record 1
	if s, err := OpenFile(dir); err == nil {
		s.Close()
		t.Fatal("mid-file corruption was accepted")
	}
}

func TestFileSnapshot(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	must(t, s.SaveHardState(4, "n1"))
	must(t, s.Append(ents(1, 1, 5)))
	must(t, s.SaveSnapshot(raft.Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("state")}))
	must(t, s.Append(ents(1, 6, 6)))
	must(t, s.Close())

	hs, log, snap, err := openT(t, dir).Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs.Term != 4 || hs.VotedFor != "n1" {
		t.Fatalf("hardstate lost across compaction: %+v", hs)
	}
	if snap == nil || snap.LastIncludedIndex != 3 || snap.LastIncludedTerm != 1 || string(snap.Data) != "state" {
		t.Fatalf("snapshot %+v", snap)
	}
	sameEntries(t, log, ents(1, 4, 6))
}

// Crash between writing the snapshot file and compacting the WAL: the WAL
// still holds covered entries, which Load must ignore.
func TestFileSnapshotCrashBeforeCompaction(t *testing.T) {
	dir := t.TempDir()
	s := openT(t, dir)
	must(t, s.Append(ents(1, 1, 5)))
	must(t, s.writeSnapshotFile(raft.Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("x")}))
	must(t, s.Close())
	_, log, snap, err := openT(t, dir).Load()
	if err != nil || snap == nil || snap.LastIncludedIndex != 3 {
		t.Fatalf("snap=%+v err=%v", snap, err)
	}
	sameEntries(t, log, ents(1, 4, 5))
}

func TestFileLoadReturnsCopies(t *testing.T) {
	s := openT(t, t.TempDir())
	must(t, s.Append(ents(1, 1, 2)))
	_, log, _, _ := s.Load()
	log[0].Data[0] = 0xEE
	_, log2, _, _ := s.Load()
	if log2[0].Data[0] == 0xEE {
		t.Fatal("Load aliases internal state")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[off] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
