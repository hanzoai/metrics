package metrics

import (
	"encoding/json"
	"sync"
)

// Span is one unit of a distributed trace — the native, prometheus-free,
// Tempo-free trace signal. Times are nanoseconds since the Unix epoch.
type Span struct {
	TraceID string            `json:"traceId"`
	SpanID  string            `json:"spanId"`
	Parent  string            `json:"parentId,omitempty"`
	Name    string            `json:"name"`
	StartNs int64             `json:"startNs"`
	EndNs   int64             `json:"endNs"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

// TraceStore is the native span store: an append-only bounded ring with a
// trace-id index for waterfall lookup and time-range listing. Durable via WAL.
type TraceStore struct {
	mu      sync.RWMutex
	spans   []Span
	byTrace map[string][]int
	max     int
	wal     *WAL
}

// NewTraceStore returns an empty store retaining up to 1Mi spans in memory.
func NewTraceStore() *TraceStore {
	return &TraceStore{byTrace: make(map[string][]int), max: 1 << 20}
}

// EnableDurability opens and replays a WAL at path (e.g. <DataDir>/traces/traces.wal).
func (s *TraceStore) EnableDurability(path string) error {
	w, err := OpenWAL(path)
	if err != nil {
		return err
	}
	if err := w.Replay(func(rec []byte) {
		var sp Span
		if json.Unmarshal(rec, &sp) == nil {
			s.appendMem(sp)
		}
	}); err != nil {
		return err
	}
	s.mu.Lock()
	s.wal = w
	s.mu.Unlock()
	return nil
}

// Append stores one span (and durably logs it when durability is on).
func (s *TraceStore) Append(sp Span) {
	s.appendMem(sp)
	if s.wal != nil {
		if rec, err := json.Marshal(sp); err == nil {
			_ = s.wal.Append(rec)
		}
	}
}

func (s *TraceStore) appendMem(sp Span) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans = append(s.spans, sp)
	s.byTrace[sp.TraceID] = append(s.byTrace[sp.TraceID], len(s.spans)-1)
	if len(s.spans) > s.max {
		// Drop the oldest half and rebuild the trace-id index (infrequent).
		s.spans = append([]Span(nil), s.spans[len(s.spans)-s.max:]...)
		s.byTrace = make(map[string][]int, len(s.spans))
		for i, sp := range s.spans {
			s.byTrace[sp.TraceID] = append(s.byTrace[sp.TraceID], i)
		}
	}
}

// ByTrace returns every span belonging to a trace id (the waterfall).
func (s *TraceStore) ByTrace(traceID string) []Span {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idxs := s.byTrace[traceID]
	out := make([]Span, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, s.spans[i])
	}
	return out
}

// Recent returns up to limit spans (newest first) starting within [startNs,endNs].
func (s *TraceStore) Recent(startNs, endNs int64, limit int) []Span {
	if limit <= 0 {
		limit = 100
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Span, 0, limit)
	for i := len(s.spans) - 1; i >= 0 && len(out) < limit; i-- {
		sp := s.spans[i]
		if startNs != 0 && sp.StartNs < startNs {
			continue
		}
		if endNs != 0 && sp.StartNs > endNs {
			continue
		}
		out = append(out, sp)
	}
	return out
}

// Count reports the number of spans held.
func (s *TraceStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.spans)
}
