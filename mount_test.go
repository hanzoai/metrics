package metrics

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
	if err := Mount(app, Deps{Logger: log, DataDir: t.TempDir(), Brand: "hanzo", Org: testOrg}); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	return app
}

// testOrg is the same shape as the decision cloud passes in (principal.Org): an
// org counts only ALONGSIDE a validated principal. Mirroring it here — rather
// than accepting the org header alone — is what makes these tests exercise the
// rule that actually runs in production; a laxer stand-in would let the very
// hole this package just closed pass its own suite.
func testOrg(c *zip.Ctx) (string, bool) {
	if strings.TrimSpace(c.Header("X-User-Id")) == "" {
		return "", false
	}
	org := strings.TrimSpace(c.Header("X-Org-Id"))
	if org == "" {
		return "", false
	}
	return org, true
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
		// The identity boundary mints both, and the org is worth nothing without
		// the user — so a test that sends one and not the other is testing a
		// request the boundary never produces.
		req.Header.Set("X-User-Id", "u-"+org)
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
	if got := num(t, do(t, app, "POST", "/v1/metrics/write", "acme", in), "written"); got != 2 {
		t.Fatalf("written = %v, want 2", got)
	}

	res := do(t, app, "GET", "/v1/metrics/query?name=http_requests_total&match=route=/v1/x", "acme", nil)
	if got := num(t, res, "count"); got != 1 {
		t.Fatalf("count = %v, want 1", got)
	}
	// Range bounds must clip: [150,∞) keeps only the second sample.
	res = do(t, app, "GET", "/v1/metrics/query?name=http_requests_total&start=150", "acme", nil)
	series := res["series"].([]any)
	smps := series[0].(map[string]any)["samples"].([]any)
	if len(smps) != 1 || smps[0].(map[string]any)["v"].(float64) != 2 {
		t.Fatalf("range query samples = %v, want the single v=2 sample", smps)
	}
	// A non-matching label matcher selects nothing.
	if got := num(t, do(t, app, "GET", "/v1/metrics/query?match=route=/nope", "acme", nil), "count"); got != 0 {
		t.Fatalf("count for non-matching matcher = %v, want 0", got)
	}
	// Counts come from the tenant's own query, not from health — health is
	// liveness for an unauthenticated probe and carries no per-org figure.
	if got := num(t, do(t, app, "GET", "/v1/metrics/query?name=http_requests_total", "acme", nil), "count"); got != 1 {
		t.Fatalf("owner series count = %v, want 1", got)
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
	if got := num(t, do(t, app, "POST", "/v1/metrics/batch", "acme", batch), "written"); got != 3 {
		t.Fatalf("written = %v, want 3", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/metrics/query?name=req_latency_count", "acme", nil), "count"); got != 1 {
		t.Fatalf("derived _count series not queryable: %v", got)
	}
}

func TestLogsAndTraces(t *testing.T) {
	app := newTestApp(t)
	logs := map[string]any{"records": []LogRecord{
		{TsNs: 10, Level: "info", Body: "Started ingest", Labels: map[string]string{"svc": "o11y"}},
		{TsNs: 20, Level: "error", Body: "disk full", Labels: map[string]string{"svc": "o11y"}},
	}}
	if got := num(t, do(t, app, "POST", "/v1/logs/write", "acme", logs), "written"); got != 2 {
		t.Fatalf("logs written = %v, want 2", got)
	}
	// Substring match is case-insensitive and label-scoped.
	if got := num(t, do(t, app, "GET", "/v1/logs/query?match=svc=o11y&contains=STARTED", "acme", nil), "count"); got != 1 {
		t.Fatalf("logs contains-query count = %v, want 1", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/logs/query?limit=10", "acme", nil), "count"); got != 2 {
		t.Fatalf("owner log count = %v, want 2", got)
	}

	spans := map[string]any{"spans": []Span{
		{TraceID: "t1", SpanID: "s1", Name: "root", StartNs: 1, EndNs: 9},
		{TraceID: "t1", SpanID: "s2", Parent: "s1", Name: "child", StartNs: 2, EndNs: 8},
		{TraceID: "t2", SpanID: "s3", Name: "other", StartNs: 3, EndNs: 7},
	}}
	if got := num(t, do(t, app, "POST", "/v1/traces/write", "acme", spans), "written"); got != 3 {
		t.Fatalf("spans written = %v, want 3", got)
	}
	if got := len(do(t, app, "GET", "/v1/traces/trace?id=t1", "acme", nil)["spans"].([]any)); got != 2 {
		t.Fatalf("trace t1 waterfall = %d spans, want 2", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/traces/query?limit=2", "acme", nil), "count"); got != 2 {
		t.Fatalf("traces query count = %v, want 2 (limit honoured)", got)
	}
	if got := num(t, do(t, app, "GET", "/v1/traces/query?limit=10", "acme", nil), "count"); got != 3 {
		t.Fatalf("owner span count = %v, want 3", got)
	}
}

// TestTenantIsolation pins the hard guarantee: data written under one org is
// invisible to every other org, and health reports no per-tenant figure at all.
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
	// Health is liveness and NOTHING else. It answers an unauthenticated probe,
	// so anything per-org it reported would be per-org data handed to exactly
	// the caller who proved nothing: it used to return the tenant's series count
	// and the resolved org name.
	h := do(t, app, "GET", "/v1/metrics/health", "", nil)
	for _, leak := range []string{"org", "series", "records", "spans"} {
		if _, ok := h[leak]; ok {
			t.Fatalf("health leaks per-tenant field %q: %v", leak, h)
		}
	}
}

// TestAnonymousCallerCannotNameATenant is the production defect, as a test.
//
// Measured against api.hanzo.ai before the fix, with no credential at any step:
// POST /v1/logs/write carrying an invented X-Org-Id answered {"written":1}, and
// GET /v1/logs/query with that header read the record straight back. The header
// WAS the tenant boundary, so every org's metrics, logs and traces were readable
// and forgeable by anyone who could guess a name.
//
// Every data route is covered, not just the one that was probed: the hole was
// never in a handler, it was in the resolver they all shared, so a fix verified
// on one route says nothing about the other nine.
func TestAnonymousCallerCannotNameATenant(t *testing.T) {
	app := newTestApp(t)

	// Seed as a properly authenticated caller, so the refusals below are about
	// the credential and not about an empty store.
	do(t, app, "POST", "/v1/logs/write", "acme",
		map[string]any{"records": []LogRecord{{TsNs: 1, Body: "secret"}}})
	do(t, app, "POST", "/v1/metrics/write", "acme",
		map[string]any{"series": []Series{{Name: "s", Samples: []Sample{{TsNs: 1, Value: 1}}}}})
	do(t, app, "POST", "/v1/traces/write", "acme",
		map[string]any{"spans": []Span{{TraceID: "t1", SpanID: "s1", Name: "op", StartNs: 1, EndNs: 2}}})

	for _, tc := range []struct {
		method, path string
		body         any
	}{
		{"GET", "/v1/logs/query?limit=10", nil},
		{"GET", "/v1/metrics/query?name=s", nil},
		{"GET", "/v1/traces/query?limit=10", nil},
		{"GET", "/v1/traces/trace?id=t1", nil},
		{"POST", "/v1/logs/write", map[string]any{"records": []LogRecord{{TsNs: 2, Body: "forged"}}}},
		{"POST", "/v1/metrics/write", map[string]any{"series": []Series{{Name: "s", Samples: []Sample{{TsNs: 2, Value: 9}}}}}},
		{"POST", "/v1/traces/write", map[string]any{"spans": []Span{{TraceID: "t2", SpanID: "s2", Name: "op", StartNs: 1, EndNs: 2}}}},
		{"POST", "/v1/metrics/batch", map[string]any{}},
	} {
		// The org header alone — exactly what the live probe sent, and what the
		// old resolver honoured.
		if code := status(t, app, tc.method, tc.path, map[string]string{"X-Org-Id": "acme"}, tc.body); code != http.StatusForbidden {
			t.Errorf("%s %s with a bare X-Org-Id: got %d, want 403 — the header is not a credential",
				tc.method, tc.path, code)
		}
		// Nothing at all.
		if code := status(t, app, tc.method, tc.path, nil, tc.body); code != http.StatusForbidden {
			t.Errorf("%s %s anonymous: got %d, want 403", tc.method, tc.path, code)
		}
	}

	// And the seeded tenant still reads its own data, so the gate refuses
	// callers rather than the feature.
	if got := num(t, do(t, app, "GET", "/v1/logs/query?limit=10", "acme", nil), "count"); got != 1 {
		t.Fatalf("authenticated owner sees %v records, want 1 — the fix must not break the product", got)
	}
}

// TestMountRefusesWithoutATenantDecision pins the fail-closed boot. A nil Org
// could only default to something, and every default is a tenant an anonymous
// caller gets to name — so the store must be unreachable rather than open.
func TestMountRefusesWithoutATenantDecision(t *testing.T) {
	log := luxlog.NewNoOpLogger()
	app := zip.New(zip.Config{AppName: "metrics-test", Logger: log})
	if err := Mount(app, Deps{Logger: log, DataDir: t.TempDir(), Brand: "hanzo"}); err == nil {
		t.Fatal("Mount with no Org decision succeeded — it must refuse, not pick a default tenant")
	}
}

// status issues a request with explicit headers and returns the status code
// only. `do` fatals on any non-200, which is precisely what a refusal test
// needs to observe rather than die on.
func status(t *testing.T, app *zip.App, method, path string, headers map[string]string, body any) int {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := app.Fiber().Test(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
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
