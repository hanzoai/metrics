package metrics

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	luxlog "github.com/luxfi/log"
	metric "github.com/luxfi/metric"
	"github.com/zap-proto/zip"
)

// newTestApp mounts the subsystem on a real zip.App rooted at a fresh temp
// DataDir, so every test exercises routing, binding and JSON through zip — not
// the handler funcs in isolation.
func newTestApp(t *testing.T) *zip.App {
	t.Helper()
	log := luxlog.NewNoOpLogger()
	app := zip.New(zip.Config{AppName: "metrics-test", Logger: log})
	if err := Mount(app, Deps{Logger: log, DataDir: t.TempDir(), Brand: "hanzo"}); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	return app
}

// do issues one request through the app's router and returns the decoded JSON
// body, failing the test on any non-200.
func do(t *testing.T, app *zip.App, method, path, org string, body any) map[string]any {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if org != "" {
		req.Header.Set("X-Org-Id", org)
	}
	resp, err := app.Fiber().Test(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: status %d body %s", method, path, resp.StatusCode, raw)
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
	}
	return out
}

func num(t *testing.T, m map[string]any, k string) float64 {
	t.Helper()
	v, ok := m[k].(float64)
	if !ok {
		t.Fatalf("field %q missing or not a number in %v", k, m)
	}
	return v
}

func TestMetricsWriteQuery(t *testing.T) {
	app := newTestApp(t)
	in := map[string]any{"series": []Series{{
		Name:    "http_requests_total",
		Labels:  map[string]string{"route": "/v1/x"},
		Samples: []Sample{{TsNs: 100, Value: 1}, {TsNs: 200, Value: 2}},
	}}}
	if got := num(t, do(t, app, "POST", "/v1/metrics/write", "", in), "written"); got != 2 {
		t.Fatalf("written = %v, want 2", got)
	}

	res := do(t, app, "GET", "/v1/metrics/query?name=http_requests_total&match=route=/v1/x", "", nil)
	if got := num(t, res, "count"); got != 1 {
		t.Fatalf("count = %v, want 1", got)
	}
	// Range bounds must clip: [150,∞) keeps only the second sample.
	res = do(t, app, "GET", "/v1/metrics/query?name=http_requests_total&start=150", "", nil)
	series := res["series"].([]any)
	smps := series[0].(map[string]any)["samples"].([]any)
	if len(smps) != 1 || smps[0].(map[string]any)["v"].(float64) != 2 {
		t.Fatalf("range query samples = %v, want the single v=2 sample", smps)
	}
	// A non-matching label matcher selects nothing.
	if got := num(t, do(t, app, "GET", "/v1/metrics/query?match=route=/nope", "", nil), "count"); got != 0 {
		t.Fatalf("count for non-matching matcher = %v, want 0", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/metrics/health", "", nil), "series"); got != 1 {
		t.Fatalf("health series = %v, want 1", got)
	}
}

func TestMetricsBatchIngest(t *testing.T) {
	app := newTestApp(t)
	val, sum, cnt := 7.0, 12.5, uint64(3)
	batch := metric.MetricBatch{
		TimestampNs: 1_000,
		Families: []metric.MetricFamilyWire{
			{Name: "cpu_seconds", Type: "gauge", Metrics: []metric.MetricWire{{Value: &val}}},
			{Name: "req_latency", Type: "histogram", Metrics: []metric.MetricWire{{SampleSum: &sum, SampleCount: &cnt}}},
		},
	}
	// One value + one _sum + one _count = 3 samples written.
	if got := num(t, do(t, app, "POST", "/v1/metrics/batch", "", batch), "written"); got != 3 {
		t.Fatalf("written = %v, want 3", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/metrics/query?name=req_latency_count", "", nil), "count"); got != 1 {
		t.Fatalf("derived _count series not queryable: %v", got)
	}
}

func TestLogsAndTraces(t *testing.T) {
	app := newTestApp(t)
	logs := map[string]any{"records": []LogRecord{
		{TsNs: 10, Level: "info", Body: "Started ingest", Labels: map[string]string{"svc": "o11y"}},
		{TsNs: 20, Level: "error", Body: "disk full", Labels: map[string]string{"svc": "o11y"}},
	}}
	if got := num(t, do(t, app, "POST", "/v1/logs/write", "", logs), "written"); got != 2 {
		t.Fatalf("logs written = %v, want 2", got)
	}
	// Substring match is case-insensitive and label-scoped.
	if got := num(t, do(t, app, "GET", "/v1/logs/query?match=svc=o11y&contains=STARTED", "", nil), "count"); got != 1 {
		t.Fatalf("logs contains-query count = %v, want 1", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/logs/health", "", nil), "records"); got != 2 {
		t.Fatalf("logs health records = %v, want 2", got)
	}

	spans := map[string]any{"spans": []Span{
		{TraceID: "t1", SpanID: "s1", Name: "root", StartNs: 1, EndNs: 9},
		{TraceID: "t1", SpanID: "s2", Parent: "s1", Name: "child", StartNs: 2, EndNs: 8},
		{TraceID: "t2", SpanID: "s3", Name: "other", StartNs: 3, EndNs: 7},
	}}
	if got := num(t, do(t, app, "POST", "/v1/traces/write", "", spans), "written"); got != 3 {
		t.Fatalf("spans written = %v, want 3", got)
	}
	if got := len(do(t, app, "GET", "/v1/traces/trace?id=t1", "", nil)["spans"].([]any)); got != 2 {
		t.Fatalf("trace t1 waterfall = %d spans, want 2", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/traces/query?limit=2", "", nil), "count"); got != 2 {
		t.Fatalf("traces query count = %v, want 2 (limit honoured)", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/traces/health", "", nil), "spans"); got != 3 {
		t.Fatalf("traces health spans = %v, want 3", got)
	}
}

// TestTenantIsolation pins the hard guarantee: data written under one X-Org-Id
// is invisible to every other org, and an absent header falls back to the brand.
func TestTenantIsolation(t *testing.T) {
	app := newTestApp(t)
	in := map[string]any{"series": []Series{{Name: "s", Samples: []Sample{{TsNs: 1, Value: 1}}}}}
	do(t, app, "POST", "/v1/metrics/write", "acme", in)

	if got := num(t, do(t, app, "GET", "/v1/metrics/query?name=s", "acme", nil), "count"); got != 1 {
		t.Fatalf("acme sees its own series count = %v, want 1", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/metrics/query?name=s", "other", nil), "count"); got != 0 {
		t.Fatalf("cross-tenant leak: other sees %v series, want 0", got)
	}
	// No header => the deployment brand, a distinct tenant from "acme".
	h := do(t, app, "GET", "/v1/metrics/health", "", nil)
	if h["org"] != "hanzo" {
		t.Fatalf("default org = %v, want the brand \"hanzo\"", h["org"])
	}
	if got := num(t, h, "series"); got != 0 {
		t.Fatalf("brand tenant sees %v series, want 0", got)
	}
}

// TestWALDurability proves the per-org WAL replays: a second Registry over the
// same DataDir recovers what the first wrote.
func TestWALDurability(t *testing.T) {
	dir := t.TempDir()
	r1 := NewRegistry(dir)
	ts := r1.For("acme")
	ts.Metrics.Append("m", map[string]string{"a": "b"}, Sample{TsNs: 5, Value: 42})
	ts.Logs.Append(LogRecord{TsNs: 5, Body: "hello"})
	ts.Traces.Append(Span{TraceID: "t", SpanID: "s", StartNs: 5})

	r2 := NewRegistry(dir)
	got := r2.For("acme")
	if n := got.Metrics.SeriesCount(); n != 1 {
		t.Fatalf("replayed series = %d, want 1", n)
	}
	if q := got.Metrics.Query("m", map[string]string{"a": "b"}, 0, 0); len(q) != 1 || q[0].Samples[0].Value != 42 {
		t.Fatalf("replayed samples = %v, want one v=42", q)
	}
	if n := got.Logs.Count(); n != 1 {
		t.Fatalf("replayed logs = %d, want 1", n)
	}
	if n := len(got.Traces.ByTrace("t")); n != 1 {
		t.Fatalf("replayed spans for trace t = %d, want 1", n)
	}
	// Isolation on disk: a different org has its own empty WAL tree.
	if n := r2.For("zoo").Metrics.SeriesCount(); n != 0 {
		t.Fatalf("org zoo replayed %d series from acme's WAL, want 0", n)
	}
	if _, err := filepath.Glob(filepath.Join(dir, "orgs", "acme", "o11y", "*.wal")); err != nil {
		t.Fatalf("glob per-org wal dir: %v", err)
	}
}
