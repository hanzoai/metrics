package metrics

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL is a simple append-only write-ahead log of length-prefixed records. It
// gives the in-memory stores durability: every write is appended here and the
// log is replayed on startup. This is intentionally a single flat segment — a
// production deployment adds rotation/compaction behind the same Append/Replay
// API without touching callers.
type WAL struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// OpenWAL opens (creating parent dirs and the file as needed) an append log.
func OpenWAL(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, w: bufio.NewWriter(f)}, nil
}

// Replay invokes fn for every record from the start, then positions the file at
// the end so subsequent Appends extend the log. Call once on startup.
func (w *WAL) Replay(fn func([]byte)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(w.f)
	var hdr [4]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		rec := make([]byte, binary.LittleEndian.Uint32(hdr[:]))
		if _, err := io.ReadFull(r, rec); err != nil {
			return err
		}
		fn(rec)
	}
	_, err := w.f.Seek(0, io.SeekEnd)
	return err
}

// Append writes one length-prefixed record and flushes it to disk.
func (w *WAL) Append(rec []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(rec)))
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.w.Write(rec); err != nil {
		return err
	}
	return w.w.Flush()
}

// Close flushes and closes the log.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.w != nil {
		_ = w.w.Flush()
	}
	return w.f.Close()
}
