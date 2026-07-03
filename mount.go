// HIP-0106 native observability subsystem — New/Mount composition-root form.
//
//	import "github.com/hanzoai/metrics"
//	metrics.Mount(app, metrics.Deps{Logger: log, DataDir: dir, Brand: brand})
//
// One subsystem serves all three signals — metrics, logs, traces — under
// /v1/{metrics,logs,traces}/* on the shared zip.App. Storage is native and
// WAL-durable; ingest for metrics is luxfi/metric.MetricBatch (the ZAP
// MsgMetricBatch payload). Every request is scoped to a tenant via the
// gateway-minted X-Org-Id header (falling back to the deployment brand), so the
// same binary serves any tenant with hard data isolation. There is no
// prometheus, no Grafana, no scrape endpoint, no /api/ path.
//
// This package imports ONLY zap-proto/zip + luxfi (no hanzoai/cloud): it depends
// on the 3 things it uses — a logger, a data dir, and the brand — which it
// declares in its own Deps. The composition root (cmd/cloud) constructs those and
// calls Mount explicitly; there is no global registry and no init() side effect.
package metrics

import (
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

// brand is the deployment brand, used as the default org when a request carries
// no X-Org-Id (single-tenant deployments).
var brand string

// Deps is the NARROW dependency surface this subsystem declares — only the three
// things it uses. The composition root builds it from Config and passes it to
// Mount. No hanzoai/cloud import, no god-struct: a subsystem depends on what it
// needs, nothing more.
type Deps struct {
	// Logger is the canonical Hanzo logger; Mount derives a scoped child.
	Logger luxlog.Logger
	// DataDir is the per-deployment data root; per-org WALs land under it.
	DataDir string
	// Brand is the default tenant when a request omits X-Org-Id.
	Brand string
}

// orgOf resolves the tenant for a request: the gateway-minted X-Org-Id header,
// else the deployment brand, else "default".
func orgOf(c *zip.Ctx) string {
	if org := c.Header("X-Org-Id"); org != "" {
		return org
	}
	if brand != "" {
		return brand
	}
	return "default"
}

// Mount registers the native observability routes on the shared cloud App.
func Mount(app *zip.App, deps Deps) error {
	log := deps.Logger.New("subsystem", "o11y")
	reg = NewRegistry(deps.DataDir)
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
			"series": reg.For(orgOf(c)).Metrics.SeriesCount(), "org": orgOf(c),
		})
	})
	// Batch ingest — luxfi/metric.MetricBatch (the ZAP MsgMetricBatch wire shape).
	app.Post("/v1/metrics/batch", func(c *zip.Ctx) error {
		var b metric.MetricBatch
		if err := c.Bind(&b); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid metric batch"})
		}
		return c.JSON(http.StatusOK, map[string]any{"written": reg.For(orgOf(c)).Metrics.IngestBatch(&b)})
	})
	app.Post("/v1/metrics/write", func(c *zip.Ctx) error {
		var req struct {
			Series []Series `json:"series"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid write request"})
		}
		st := reg.For(orgOf(c)).Metrics
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
		res := reg.For(orgOf(c)).Metrics.Query(c.Query("name"), parseMatchers(c.Query("match")), start, end)
		return c.JSON(http.StatusOK, map[string]any{"count": len(res), "series": res})
	})

	// --- Logs (native, Loki-free) ---
	app.Get("/v1/logs/health", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{"status": "ok", "service": "logs", "version": Version, "records": reg.For(orgOf(c)).Logs.Count()})
	})
	app.Post("/v1/logs/write", func(c *zip.Ctx) error {
		var req struct {
			Records []LogRecord `json:"records"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid logs write"})
		}
		st := reg.For(orgOf(c)).Logs
		for _, r := range req.Records {
			st.Append(r)
		}
		return c.JSON(http.StatusOK, map[string]any{"written": len(req.Records)})
	})
	app.Get("/v1/logs/query", func(c *zip.Ctx) error {
		start, _ := strconv.ParseInt(c.Query("start"), 10, 64)
		end, _ := strconv.ParseInt(c.Query("end"), 10, 64)
		limit, _ := strconv.Atoi(c.Query("limit"))
		res := reg.For(orgOf(c)).Logs.Query(parseMatchers(c.Query("match")), start, end, c.Query("contains"), limit)
		return c.JSON(http.StatusOK, map[string]any{"count": len(res), "records": res})
	})

	// --- Traces (native, Tempo-free) ---
	app.Get("/v1/traces/health", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{"status": "ok", "service": "traces", "version": Version, "spans": reg.For(orgOf(c)).Traces.Count()})
	})
	app.Post("/v1/traces/write", func(c *zip.Ctx) error {
		var req struct {
			Spans []Span `json:"spans"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid traces write"})
		}
		st := reg.For(orgOf(c)).Traces
		for _, sp := range req.Spans {
			st.Append(sp)
		}
		return c.JSON(http.StatusOK, map[string]any{"written": len(req.Spans)})
	})
	app.Get("/v1/traces/trace", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{"spans": reg.For(orgOf(c)).Traces.ByTrace(c.Query("id"))})
	})
	app.Get("/v1/traces/query", func(c *zip.Ctx) error {
		start, _ := strconv.ParseInt(c.Query("start"), 10, 64)
		end, _ := strconv.ParseInt(c.Query("end"), 10, 64)
		limit, _ := strconv.Atoi(c.Query("limit"))
		res := reg.For(orgOf(c)).Traces.Recent(start, end, limit)
		return c.JSON(http.StatusOK, map[string]any{"count": len(res), "spans": res})
	})

	log.Info("mounted native ZAP observability store (metrics+logs+traces)",
		"version", Version, "durable", deps.DataDir != "", "brand", brand,
		"routes", "/v1/{metrics,logs,traces}/*", "tenancy", "X-Org-Id")
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
