package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/panavdoesstuff/raftkv/internal/raft"
)

// File names inside a FileStorage directory.
const (
	WALFile      = "wal"
	SnapshotFile = "snapshot"
)

// WAL record layout, little endian:
//
//	[len u32][crc u32][type u8][payload ...]
//
// len counts type+payload; crc is CRC-32C over type+payload. Payloads:
//
//	hardstate: term u64, votedFor (rest)
//	entry:     index u64, term u64, entry type u8, data (rest)
//	truncate:  index u64   (drop every entry with Index >= index)
const (
	recHardState byte = 1
	recEntry     byte = 2
	recTruncate  byte = 3

	headerLen    = 8
	maxRecordLen = 1 << 30
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)

	// ErrCorrupt means the WAL or snapshot is damaged somewhere a torn write
	// cannot explain. Recovering would silently drop acknowledged data, so we
	// refuse to start instead.
	ErrCorrupt = errors.New("storage: corrupt data")
)

// FileStorage is a durable raft.Storage: an append-only WAL plus an atomically
// replaced snapshot file. Every mutating call fsyncs before it returns.
//
// Recovery (Open) replays the WAL. A torn tail, meaning an incomplete or
// CRC-failing final record such as a power cut mid-write leaves, is cut off.
// A bad record with valid data after it is reported as ErrCorrupt.
type FileStorage struct {
	mu     sync.Mutex
	dir    string
	f      *os.File // WAL, opened O_APPEND
	broken error    // sticky: after a failed write the file's tail is unknown

	hs   raft.HardState
	log  []raft.Entry // contiguous, ascending Index, all > snap index
	snap *raft.Snapshot
}

var _ raft.Storage = (*FileStorage)(nil)

// OpenFile opens (creating if needed) the storage in dir and recovers it.
func OpenFile(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Leftovers from a crash during an atomic replace are never valid.
	for _, tmp := range []string{WALFile + ".tmp", SnapshotFile + ".tmp"} {
		if err := os.Remove(filepath.Join(dir, tmp)); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	s := &FileStorage{dir: dir}
	snap, err := readSnapshotFile(filepath.Join(dir, SnapshotFile))
	if err != nil {
		return nil, err
	}
	s.snap = snap

	walPath := filepath.Join(dir, WALFile)
	data, err := os.ReadFile(walPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	good, err := s.replay(data)
	if err != nil {
		return nil, err
	}
	// A crash between writing the snapshot and compacting the WAL leaves
	// covered entries behind; they are dead.
	s.dropCovered()

	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != good { // torn tail: cut it off before appending after it
		if err := f.Truncate(good); err != nil {
			f.Close()
			return nil, err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	s.f = f
	return s, nil
}

// replay applies every intact record to the in-memory state and returns the
// offset just past the last one.
func (s *FileStorage) replay(data []byte) (int64, error) {
	off := 0
	for off < len(data) {
		rest := data[off:]
		if len(rest) < headerLen {
			return int64(off), nil // torn header
		}
		n := binary.LittleEndian.Uint32(rest[0:4])
		if n == 0 || n > maxRecordLen || int(n) > len(rest)-headerLen {
			return int64(off), nil // torn: length is garbage or runs past EOF
		}
		body := rest[headerLen : headerLen+int(n)]
		if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(rest[4:8]) {
			if headerLen+int(n) == len(rest) {
				return int64(off), nil // bad final record: torn write
			}
			return 0, fmt.Errorf("%w: bad checksum at WAL offset %d with %d bytes after it",
				ErrCorrupt, off, len(rest)-headerLen-int(n))
		}
		if err := s.apply(body); err != nil {
			return 0, fmt.Errorf("%w: WAL offset %d: %v", ErrCorrupt, off, err)
		}
		off += headerLen + int(n)
	}
	return int64(off), nil
}

func (s *FileStorage) apply(body []byte) error {
	typ, p := body[0], body[1:]
	switch typ {
	case recHardState:
		if len(p) < 8 {
			return errors.New("short hardstate record")
		}
		s.hs = raft.HardState{Term: binary.LittleEndian.Uint64(p), VotedFor: raft.NodeID(p[8:])}
	case recEntry:
		if len(p) < 17 {
			return errors.New("short entry record")
		}
		e := raft.Entry{
			Index: binary.LittleEndian.Uint64(p),
			Term:  binary.LittleEndian.Uint64(p[8:]),
			Type:  raft.EntryType(p[16]),
		}
		if len(p) > 17 {
			e.Data = append([]byte(nil), p[17:]...)
		}
		if n := len(s.log); n > 0 && e.Index != s.log[n-1].Index+1 {
			return fmt.Errorf("entry index %d after %d", e.Index, s.log[n-1].Index)
		}
		s.log = append(s.log, e)
	case recTruncate:
		if len(p) != 8 {
			return errors.New("bad truncate record")
		}
		s.truncateMem(binary.LittleEndian.Uint64(p))
	default:
		return fmt.Errorf("unknown record type %d", typ)
	}
	return nil
}

func (s *FileStorage) truncateMem(index uint64) {
	for i, e := range s.log {
		if e.Index >= index {
			s.log = s.log[:i]
			return
		}
	}
}

func (s *FileStorage) dropCovered() {
	if s.snap == nil {
		return
	}
	kept := s.log[:0:0]
	for _, e := range s.log {
		if e.Index > s.snap.LastIncludedIndex {
			kept = append(kept, e)
		}
	}
	s.log = kept
}

func encodeRecord(typ byte, payload []byte) []byte {
	b := make([]byte, headerLen+1+len(payload))
	b[headerLen] = typ
	copy(b[headerLen+1:], payload)
	binary.LittleEndian.PutUint32(b[0:4], uint32(1+len(payload)))
	binary.LittleEndian.PutUint32(b[4:8], crc32.Checksum(b[headerLen:], crcTable))
	return b
}

func hardStateRecord(hs raft.HardState) []byte {
	p := binary.LittleEndian.AppendUint64(nil, hs.Term)
	return encodeRecord(recHardState, append(p, hs.VotedFor...))
}

func entryRecord(e raft.Entry) []byte {
	p := binary.LittleEndian.AppendUint64(nil, e.Index)
	p = binary.LittleEndian.AppendUint64(p, e.Term)
	p = append(p, byte(e.Type))
	return encodeRecord(recEntry, append(p, e.Data...))
}

// write appends buf to the WAL and fsyncs. On failure the tail is unknown, so
// every later call fails too: the caller must stop and recover by reopening.
func (s *FileStorage) write(buf []byte) error {
	if s.broken != nil {
		return s.broken
	}
	if s.f == nil {
		return os.ErrClosed
	}
	if _, err := s.f.Write(buf); err != nil {
		s.broken = err
		return err
	}
	if err := s.f.Sync(); err != nil {
		s.broken = err
		return err
	}
	return nil
}

func (s *FileStorage) SaveHardState(term uint64, votedFor raft.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hs := raft.HardState{Term: term, VotedFor: votedFor}
	if err := s.write(hardStateRecord(hs)); err != nil {
		return err
	}
	s.hs = hs
	return nil
}

func (s *FileStorage) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := uint64(0)
	if n := len(s.log); n > 0 {
		prev = s.log[n-1].Index
	}
	var buf []byte
	for i, e := range entries {
		if (prev != 0 || i > 0) && e.Index != prev+1 {
			return errors.New("storage: non-contiguous append")
		}
		prev = e.Index
		buf = append(buf, entryRecord(e)...)
	}
	if err := s.write(buf); err != nil { // one write + one fsync for the batch
		return err
	}
	for _, e := range entries {
		s.log = append(s.log, e.Clone())
	}
	return nil
}

func (s *FileStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.write(encodeRecord(recTruncate, binary.LittleEndian.AppendUint64(nil, index))); err != nil {
		return err
	}
	s.truncateMem(index)
	return nil
}

// SaveSnapshot atomically replaces the snapshot, then compacts the WAL. A
// crash between the two steps is safe: Open ignores covered entries.
func (s *FileStorage) SaveSnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return s.broken
	}
	if s.f == nil {
		return os.ErrClosed
	}
	if s.snap != nil && snap.LastIncludedIndex <= s.snap.LastIncludedIndex {
		return nil // older than what we hold
	}
	if err := s.writeSnapshotFile(snap); err != nil {
		return err
	}
	return s.compactWAL()
}

func (s *FileStorage) writeSnapshotFile(snap raft.Snapshot) error {
	snap.Data = append([]byte(nil), snap.Data...)
	body := binary.LittleEndian.AppendUint64(nil, snap.LastIncludedIndex)
	body = binary.LittleEndian.AppendUint64(body, snap.LastIncludedTerm)
	body = append(body, snap.Data...)
	buf := binary.LittleEndian.AppendUint32(nil, crc32.Checksum(body, crcTable))
	buf = append(buf, body...)
	if err := atomicWrite(s.dir, SnapshotFile, buf); err != nil {
		return err
	}
	s.snap = &snap
	s.dropCovered()
	return nil
}

// compactWAL rewrites the WAL as: current hardstate + surviving entries.
func (s *FileStorage) compactWAL() error {
	buf := hardStateRecord(s.hs)
	for _, e := range s.log {
		buf = append(buf, entryRecord(e)...)
	}
	if err := atomicWrite(s.dir, WALFile, buf); err != nil {
		s.broken = err
		return err
	}
	f, err := os.OpenFile(filepath.Join(s.dir, WALFile), os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		s.broken = err
		return err
	}
	s.f.Close()
	s.f = f
	return nil
}

func (s *FileStorage) Load() (raft.HardState, []raft.Entry, *raft.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snap *raft.Snapshot
	if s.snap != nil {
		c := *s.snap
		c.Data = append([]byte(nil), c.Data...)
		snap = &c
	}
	return s.hs, raft.CloneEntries(s.log), snap, nil
}

// Close releases the file. Safe to call more than once.
func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

func readSnapshotFile(path string) (*raft.Snapshot, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Written by temp file + rename, so it is whole or absent. Anything else
	// is damage.
	if len(b) < 20 || crc32.Checksum(b[4:], crcTable) != binary.LittleEndian.Uint32(b) {
		return nil, fmt.Errorf("%w: snapshot file", ErrCorrupt)
	}
	return &raft.Snapshot{
		LastIncludedIndex: binary.LittleEndian.Uint64(b[4:]),
		LastIncludedTerm:  binary.LittleEndian.Uint64(b[12:]),
		Data:              append([]byte(nil), b[20:]...),
	}, nil
}

// atomicWrite replaces dir/name with data: temp file, fsync, rename, fsync dir.
func atomicWrite(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
