package metrics

import (
	"path/filepath"
	"sync"
)

// tenantSet holds the three signal stores for one org/tenant.
type tenantSet struct {
	Metrics *Store
	Logs    *LogStore
	Traces  *TraceStore
}

// Registry lazily creates per-tenant store sets. Each tenant's WALs live under
// <DataDir>/orgs/<org>/o11y/ (the HIP-0302 per-org convention), so one tenant's
// metrics/logs/traces never mingle with another's — the same binary serves
// lux.cloud and zoo.cloud with hard data isolation. A single-tenant deployment
// simply uses one org (the deployment brand).
type Registry struct {
	mu      sync.Mutex
	dataDir string
	tenants map[string]*tenantSet
}

// NewRegistry returns a registry rooted at dataDir ("" = in-memory, no durability).
func NewRegistry(dataDir string) *Registry {
	return &Registry{dataDir: dataDir, tenants: make(map[string]*tenantSet)}
}

// For returns the store set for org, creating it (and enabling per-org WAL
// durability when dataDir is set) on first use. An empty org collapses to
// "default".
func (r *Registry) For(org string) *tenantSet {
	if org == "" {
		org = "default"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ts := r.tenants[org]; ts != nil {
		return ts
	}
	ts := &tenantSet{Metrics: NewStore(), Logs: NewLogStore(), Traces: NewTraceStore()}
	if r.dataDir != "" {
		dir := filepath.Join(r.dataDir, "orgs", org, "o11y")
		_ = ts.Metrics.EnableDurability(filepath.Join(dir, "metrics.wal"))
		_ = ts.Logs.EnableDurability(filepath.Join(dir, "logs.wal"))
		_ = ts.Traces.EnableDurability(filepath.Join(dir, "traces.wal"))
	}
	r.tenants[org] = ts
	return ts
}

// Tenants returns the org slugs that currently have stores (for /health/admin).
func (r *Registry) Tenants() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.tenants))
	for k := range r.tenants {
		out = append(out, k)
	}
	return out
}
