// HIP-0106 Mount() entry point for the native metrics subsystem.
//
//	import _ "github.com/hanzoai/metrics" // init() registers the subsystem
//
// At startup the cloud binary iterates its registry and calls Mount() for each
// enabled subsystem, attaching /v1/metrics/* to the shared zip.App. Ingest is
// luxfi/metric.MetricBatch (the ZAP MsgMetricBatch payload); storage and query
// are native — there is no prometheus, no scrape endpoint, no /api/ path.
package metrics

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/hanzoai/cloud"
	"github.com/hanzoai/zip"
	metric "github.com/luxfi/metric"
)

// Version is surfaced on /v1/metrics/health.
const Version = "0.1.0"

// store is the process-global in-memory store. A durable per-tenant backend can
// replace it behind the same Store API without touching the handlers below.
var store = NewStore()

// init registers the subsystem. cloud.MountFunc takes app as `any` (to avoid an
// import cycle in pkg/cloud), so we assert it to *zip.App and call the typed Mount.
func init() {
	cloud.Register("metrics", 40, func(app any, deps cloud.Deps) error {
		zapp, ok := app.(*zip.App)
		if !ok {
			return fmt.Errorf("metrics.Mount: app is %T, want *zip.App", app)
		}
		return Mount(zapp, deps)
	})
}

// Mount registers the native metrics routes on the shared cloud App per HIP-0106.
func Mount(app *zip.App, deps cloud.Deps) error {
	log := deps.Logger.New("subsystem", "metrics")

	// Liveness/readiness — always answers, no auth, no external dep.
	app.Get("/v1/metrics/health", func(c *zip.Ctx) error {
		return c.JSON(http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "metrics",
			"version": Version,
			"series":  store.SeriesCount(),
		})
	})

	// Batch ingest — accepts a luxfi/metric.MetricBatch (the canonical ZAP
	// MsgMetricBatch wire shape). POST /v1/metrics/batch.
	app.Post("/v1/metrics/batch", func(c *zip.Ctx) error {
		var b metric.MetricBatch
		if err := c.Bind(&b); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid metric batch"})
		}
		return c.JSON(http.StatusOK, map[string]any{"written": store.IngestBatch(&b)})
	})

	// Native write — POST /v1/metrics/write {"series":[{"name","labels","samples"}]}.
	app.Post("/v1/metrics/write", func(c *zip.Ctx) error {
		var req struct {
			Series []Series `json:"series"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid write request"})
		}
		n := 0
		for _, ser := range req.Series {
			for _, smp := range ser.Samples {
				store.Append(ser.Name, ser.Labels, smp)
				n++
			}
		}
		return c.JSON(http.StatusOK, map[string]any{"written": n})
	})

	// Range query — GET /v1/metrics/query?name=X&start=<ns>&end=<ns>&match=k=v,k2=v2.
	app.Get("/v1/metrics/query", func(c *zip.Ctx) error {
		start, _ := strconv.ParseInt(c.Query("start"), 10, 64)
		end, _ := strconv.ParseInt(c.Query("end"), 10, 64)
		res := store.Query(c.Query("name"), parseMatchers(c.Query("match")), start, end)
		return c.JSON(http.StatusOK, map[string]any{"count": len(res), "series": res})
	})

	log.Info("mounted native ZAP metrics store", "version", Version, "routes", "/v1/metrics/{health,batch,write,query}")
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
