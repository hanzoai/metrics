package metrics

import metric "github.com/luxfi/metric"

// IngestBatch writes every sample in a luxfi/metric.MetricBatch into the store.
// This is the exact wire type the ZAP MsgMetricBatch transport carries, so the
// same code path serves both the HTTP /v1/metrics/batch endpoint and a future
// ZAP receiver. Counter/gauge values land directly; histogram/summary families
// contribute derived <name>_sum and <name>_count series. Returns samples written.
func (s *Store) IngestBatch(b *metric.MetricBatch) int {
	if b == nil {
		return 0
	}
	ts := b.TimestampNs
	n := 0
	for _, fam := range b.Families {
		for _, m := range fam.Metrics {
			if m.Value != nil {
				s.Append(fam.Name, m.Labels, Sample{TsNs: ts, Value: *m.Value})
				n++
			}
			if m.SampleSum != nil {
				s.Append(fam.Name+"_sum", m.Labels, Sample{TsNs: ts, Value: *m.SampleSum})
				n++
			}
			if m.SampleCount != nil {
				s.Append(fam.Name+"_count", m.Labels, Sample{TsNs: ts, Value: float64(*m.SampleCount)})
				n++
			}
		}
	}
	return n
}
