// HIP-0106 native observability subsystem — New/Mount composition-root form.
//
//	import "github.com/hanzoai/metrics"
//	metrics.Mount(app, metrics.Deps{Logger: log, DataDir: dir, Brand: brand, Org: principal.Org})
//
// One subsystem serves all three signals — metrics, logs, traces — under
// /v1/{metrics,logs,traces}/* on the shared zip.App. Storage is native and
// WAL-durable; ingest for metrics is luxfi/metric.MetricBatch (the ZAP
// MsgMetricBatch payload). Every request is scoped to a tenant the AUTHENTICATING
// boundary resolved — Deps.Org, never a header this package reads itself — so the
// same binary serves any tenant with hard data isolation. There is no prometheus,
// no Grafana, no scrape endpoint, no /api/ path.
//
// This package imports ONLY zap-proto/zip + luxfi (no hanzoai/cloud): it depends
// on what it uses — a logger, a data dir, a brand label and the tenant decision —
// which it declares in its own Deps. The composition root (cmd/cloud) constructs those and
// calls Mount explicitly; there is no global registry and no init() side effect.
package metrics

import (
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"

	luxlog "github.com/luxfi/log"
	metric "github.com/luxfi/metric"
	"github.com/zap-proto/zip"
)

// Version is surfaced on the /health routes.
const Version = "0.4.0"

// reg is the process-global per-tenant store registry, initialised in Mount.
var reg *Registry

// org is the tenant decision, supplied by the composition root. See Deps.Org.
var org func(*zip.Ctx) (string, bool)

// brand is the deployment label — a log field, and the ZAP receiver's default
// for an in-cluster peer that names no org. Never an HTTP tenant. See Deps.Brand.
var brand string

// Deps is the NARROW dependency surface this subsystem declares — only what it
// uses. The composition root builds it from Config and passes it to Mount. No
// hanzoai/cloud import, no god-struct: a subsystem depends on what it needs,
// nothing more.
type Deps struct {
	// Logger is the canonical Hanzo logger; Mount derives a scoped child.
	Logger luxlog.Logger
	// DataDir is the per-deployment data root; per-org WALs land under it.
	DataDir string

	// Brand is the deployment's own label. It is NOT a tenant on the HTTP
	// surface — see Org — and it stopped being one there. It remains the
	// default on the ZAP receiver, which binds only when O11Y_ZAP_PORT is set,
	// is not internet-reachable, and admits only peers already on the cluster
	// network; a batch from such a peer that names no org is this deployment's
	// own telemetry. An anonymous HTTP caller is not that, which is the whole
	// difference.
	Brand string

	// Org is THE tenant decision, and it is not ours to make.
	//
	// This subsystem used to read X-Org-Id itself and fall back to the brand.
	// That is a header a client sends, so the tenant boundary was whatever the
	// caller typed: an anonymous request could name any org and read — or write
	// — that org's metrics, logs and traces. Measured against production before
	// the fix: POST /v1/logs/write with an invented X-Org-Id answered
	// {"written":1}, and GET /v1/logs/query with the same header read it back.
	//
	// The rule the rest of the fleet applies is that an org is trustworthy only
	// alongside a VALIDATED principal, and it lives in exactly one place —
	// cloud's principal.OrgOf, whose own doc warns that a second hand-rolled
	// check is drift waiting to happen. This field is how that one rule reaches
	// here without this module importing cloud: the boundary that authenticates
	// hands down the predicate, and every route asks it.
	//
	// It returns ok=false for an unauthenticated or org-less caller, and the
	// route answers 403. There is no brand fallback: a default tenant for
	// callers who proved nothing is the hole itself.
	Org func(*zip.Ctx) (string, bool)
}

// tenant resolves the store for a request, or answers 403 and returns ok=false.
// Every route touching per-org state goes through it, so a new route cannot
// reach a tenant's data by forgetting the check — the store is only reachable
// through the decision.
func tenant(c *zip.Ctx) (*tenantSet, bool) {
	name, ok := org(c)
	if !ok {
		_ = c.JSON(http.StatusForbidden, map[string]string{"error": "X-Org-Id required"})
		return nil, false
	}
	return reg.For(name), true
}

// Mount registers the native observability routes on the shared cloud App.
func Mount(app *zip.App, deps Deps) error {
	// Fail closed, and fail at BOOT. A nil decision could only default to
	// something, and every default here is a tenant an unauthenticated caller
	// gets to name. Refusing to mount is the one outcome that cannot ship a
	// silently open store.
	if deps.Org == nil {
		return errors.New("o11y: Deps.Org is required — the tenant decision belongs to the boundary that authenticates, and there is no safe default")
	}
	log := deps.Logger.New("subsystem", "o11y")
	reg = NewRegistry(deps.DataDir)
	org = deps.Org
	brand = deps.Brand

	// Optional ZAP push ingest — bind a luxfi/zap node accepting MsgMetricBatch
	// when O11Y_ZAP_PORT is set. HTTP /v1/metrics/batch carries the same wire
	// shape, so this is a transport optimisation, not a requirement.
	if p := os.Getenv("O11Y_ZAP_PORT"); p != "" {
		if port, err := strconv.Atoi(p); err == nil {
			if _, err := startZAPReceiver(port, "o11y-metrics-"+brand, log); err != nil {
				log.Warn("zap metric receiver disabled", "err", err)
			}
		}
	}

	// --- Metrics ---
	app.Get("/v1/metrics/health", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{
			"status": "ok", "service": "metrics", "version": Version,
		})
	})
	// Batch ingest — luxfi/metric.MetricBatch (the ZAP MsgMetricBatch wire shape).
	app.Post("/v1/metrics/batch", func(c *zip.Ctx) error {
		var b metric.MetricBatch
		if err := c.Bind(&b); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid metric batch"})
		}
		t, ok := tenant(c)
		if !ok {
			return nil
		}
		return c.JSON(http.StatusOK, map[string]any{"written": t.Metrics.IngestBatch(&b)})
	})
	app.Post("/v1/metrics/write", func(c *zip.Ctx) error {
		var req struct {
			Series []Series `json:"series"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid write request"})
		}
		t, ok := tenant(c)
		if !ok {
			return nil
		}
		st := t.Metrics
		n := 0
		for _, ser := range req.Series {
			for _, smp := range ser.Samples {
				st.Append(ser.Name, ser.Labels, smp)
				n++
			}
		}
		return c.JSON(http.StatusOK, map[string]any{"written": n})
	})
	app.Get("/v1/metrics/query", func(c *zip.Ctx) error {
		start, _ := strconv.ParseInt(c.Query("start"), 10, 64)
		end, _ := strconv.ParseInt(c.Query("end"), 10, 64)
		t, ok := tenant(c)
		if !ok {
			return nil
		}
		res := t.Metrics.Query(c.Query("name"), parseMatchers(c.Query("match")), start, end)
		return c.JSON(http.StatusOK, map[string]any{"count": len(res), "series": res})
	})

	// --- Logs (native, Loki-free) ---
	app.Get("/v1/logs/health", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{"status": "ok", "service": "logs", "version": Version})
	})
	app.Post("/v1/logs/write", func(c *zip.Ctx) error {
		var req struct {
			Records []LogRecord `json:"records"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid logs write"})
		}
		t, ok := tenant(c)
		if !ok {
			return nil
		}
		st := t.Logs
		for _, r := range req.Records {
			st.Append(r)
		}
		return c.JSON(http.StatusOK, map[string]any{"written": len(req.Records)})
	})
	app.Get("/v1/logs/query", func(c *zip.Ctx) error {
		start, _ := strconv.ParseInt(c.Query("start"), 10, 64)
		end, _ := strconv.ParseInt(c.Query("end"), 10, 64)
		limit, _ := strconv.Atoi(c.Query("limit"))
		t, ok := tenant(c)
		if !ok {
			return nil
		}
		res := t.Logs.Query(parseMatchers(c.Query("match")), start, end, c.Query("contains"), limit)
		return c.JSON(http.StatusOK, map[string]any{"count": len(res), "records": res})
	})

	// --- Traces (native, Tempo-free) ---
	app.Get("/v1/traces/health", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{"status": "ok", "service": "traces", "version": Version})
	})
	app.Post("/v1/traces/write", func(c *zip.Ctx) error {
		var req struct {
			Spans []Span `json:"spans"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid traces write"})
		}
		t, ok := tenant(c)
		if !ok {
			return nil
		}
		st := t.Traces
		for _, sp := range req.Spans {
			st.Append(sp)
		}
		return c.JSON(http.StatusOK, map[string]any{"written": len(req.Spans)})
	})
	app.Get("/v1/traces/trace", func(c *zip.Ctx) error {
		t, ok := tenant(c)
		if !ok {
			return nil
		}
		return c.JSON(http.StatusOK, map[string]any{"spans": t.Traces.ByTrace(c.Query("id"))})
	})
	app.Get("/v1/traces/query", func(c *zip.Ctx) error {
		start, _ := strconv.ParseInt(c.Query("start"), 10, 64)
		end, _ := strconv.ParseInt(c.Query("end"), 10, 64)
		limit, _ := strconv.Atoi(c.Query("limit"))
		t, ok := tenant(c)
		if !ok {
			return nil
		}
		res := t.Traces.Recent(start, end, limit)
		return c.JSON(http.StatusOK, map[string]any{"count": len(res), "spans": res})
	})

	log.Info("mounted native ZAP observability store (metrics+logs+traces)",
		"version", Version, "durable", deps.DataDir != "", "brand", brand,
		"routes", "/v1/{metrics,logs,traces}/*", "tenancy", "validated principal")
	return nil
}

// parseMatchers turns "k=v,k2=v2" into a label matcher map.
func parseMatchers(s string) map[string]string {
	m := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		if k, v, ok := strings.Cut(pair, "="); ok && k != "" {
			m[k] = v
		}
	}
	return m
}
