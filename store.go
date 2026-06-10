// Package metrics is the native, ZAP-native, prometheus-free time-series store
// for the Hanzo cloud. It replaces the vendored Grafana/Prometheus observability
// backends with a small in-process store that ingests luxfi/metric.MetricBatch
// (the same wire shape the ZAP MsgMetricBatch transport carries) and serves
// range queries under /v1/metrics/*.
//
// The storage API is deliberately tiny (Append + Query + SeriesCount) so a
// durable per-tenant backend (DataDir-backed, columnar) can replace the in-memory
// map without changing mount.go or ingest.go. There is ZERO prometheus here.
package metrics

import (
	"sort"
	"strings"
	"sync"
)

// Sample is a single timestamped value. Ts is nanoseconds since the Unix epoch
// to match luxfi/metric.MetricBatch.TimestampNs.
type Sample struct {
	TsNs  int64   `json:"t"`
	Value float64 `json:"v"`
}

// Series is a named, labeled append-only stream of samples.
type Series struct {
	Name    string            `json:"name"`
	Labels  map[string]string `json:"labels,omitempty"`
	Samples []Sample          `json:"samples"`
}

// Store is the native in-memory time-series store. Each series is bounded to
// maxPerSeries samples (oldest evicted first). Safe for concurrent use.
type Store struct {
	mu           sync.RWMutex
	series       map[string]*Series
	maxPerSeries int
}

// NewStore returns an empty store with a default per-series retention of 64Ki
// samples (a real deployment sets this from config / per-tenant quota).
func NewStore() *Store {
	return &Store{series: make(map[string]*Series), maxPerSeries: 1 << 16}
}

// seriesKey is the canonical identity of a series: name plus label set in a
// stable (sorted) order. NUL separators keep it injective.
func seriesKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return b.String()
}

// Append adds one sample to the series identified by (name, labels), creating
// the series on first write and evicting the oldest sample past retention.
func (s *Store) Append(name string, labels map[string]string, smp Sample) {
	k := seriesKey(name, labels)
	s.mu.Lock()
	defer s.mu.Unlock()
	ser := s.series[k]
	if ser == nil {
		ser = &Series{Name: name, Labels: labels}
		s.series[k] = ser
	}
	ser.Samples = append(ser.Samples, smp)
	if len(ser.Samples) > s.maxPerSeries {
		ser.Samples = ser.Samples[len(ser.Samples)-s.maxPerSeries:]
	}
}

// Query returns copies of every series whose Name equals name (or all, if name
// is "") and whose labels are a superset of matchers, with samples restricted to
// [startNs, endNs] (a zero bound is treated as unbounded).
func (s *Store) Query(name string, matchers map[string]string, startNs, endNs int64) []Series {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Series, 0, 8)
	for _, ser := range s.series {
		if name != "" && ser.Name != name {
			continue
		}
		if !labelsSupersetOf(ser.Labels, matchers) {
			continue
		}
		smps := make([]Sample, 0, len(ser.Samples))
		for _, smp := range ser.Samples {
			if startNs != 0 && smp.TsNs < startNs {
				continue
			}
			if endNs != 0 && smp.TsNs > endNs {
				continue
			}
			smps = append(smps, smp)
		}
		out = append(out, Series{Name: ser.Name, Labels: ser.Labels, Samples: smps})
	}
	return out
}

// labelsSupersetOf reports whether have contains every key/value pair in want.
func labelsSupersetOf(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// SeriesCount reports the number of distinct series held (surfaced on /health).
func (s *Store) SeriesCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.series)
}
