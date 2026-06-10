package metrics

import (
	"encoding/json"
	"strings"
	"sync"
)

// LogRecord is one structured log line — the native, prometheus-free, Loki-free
// log signal. Bodies are stored verbatim; labels are the indexed dimensions.
type LogRecord struct {
	TsNs   int64             `json:"t"`
	Level  string            `json:"level,omitempty"`
	Body   string            `json:"body"`
	Labels map[string]string `json:"labels,omitempty"`
}

// LogStore is the native log store: an append-only bounded ring with label +
// time-range + case-insensitive substring query. Durable via the shared WAL.
type LogStore struct {
	mu      sync.RWMutex
	records []LogRecord
	max     int
	wal     *WAL
}

// NewLogStore returns an empty store retaining up to 1Mi records in memory.
func NewLogStore() *LogStore { return &LogStore{max: 1 << 20} }

// EnableDurability opens and replays a WAL at path (e.g. <DataDir>/logs/logs.wal).
func (s *LogStore) EnableDurability(path string) error {
	w, err := OpenWAL(path)
	if err != nil {
		return err
	}
	if err := w.Replay(func(rec []byte) {
		var lr LogRecord
		if json.Unmarshal(rec, &lr) == nil {
			s.appendMem(lr)
		}
	}); err != nil {
		return err
	}
	s.mu.Lock()
	s.wal = w
	s.mu.Unlock()
	return nil
}

// Append stores one log record (and durably logs it when durability is on).
func (s *LogStore) Append(lr LogRecord) {
	s.appendMem(lr)
	if s.wal != nil {
		if rec, err := json.Marshal(lr); err == nil {
			_ = s.wal.Append(rec)
		}
	}
}

func (s *LogStore) appendMem(lr LogRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, lr)
	if len(s.records) > s.max {
		s.records = s.records[len(s.records)-s.max:]
	}
}

// Query returns up to limit records (newest first) matching labels, the
// [startNs,endNs] range, and an optional case-insensitive substring of Body.
func (s *LogStore) Query(matchers map[string]string, startNs, endNs int64, contains string, limit int) []LogRecord {
	if limit <= 0 {
		limit = 100
	}
	contains = strings.ToLower(contains)
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]LogRecord, 0, limit)
	for i := len(s.records) - 1; i >= 0 && len(out) < limit; i-- {
		r := s.records[i]
		if startNs != 0 && r.TsNs < startNs {
			continue
		}
		if endNs != 0 && r.TsNs > endNs {
			continue
		}
		if !labelsSupersetOf(r.Labels, matchers) {
			continue
		}
		if contains != "" && !strings.Contains(strings.ToLower(r.Body), contains) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// Count reports the number of records held.
func (s *LogStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}
