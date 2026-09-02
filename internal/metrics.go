package internal

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects runtime operational metrics for the Nexus KV service.
type Metrics struct {
	startTime time.Time

	httpRequests atomic.Uint64
	http2xx      atomic.Uint64
	http4xx      atomic.Uint64
	http5xx      atomic.Uint64

	opGets      atomic.Uint64
	opSets      atomic.Uint64
	opDels      atomic.Uint64
	opLists     atomic.Uint64
	opSnapshots atomic.Uint64

	mu           sync.RWMutex
	endpointHits map[string]uint64
}

// GlobalMetrics is the singleton metrics instance.
var GlobalMetrics = NewMetrics()

// NewMetrics initializes a new Metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		startTime:    time.Now(),
		endpointHits: make(map[string]uint64),
	}
}

// RecordRequest tracks an HTTP request and status code.
func (m *Metrics) RecordRequest(endpoint string, statusCode int) {
	m.httpRequests.Add(1)
	switch {
	case statusCode >= 200 && statusCode < 300:
		m.http2xx.Add(1)
	case statusCode >= 400 && statusCode < 500:
		m.http4xx.Add(1)
	case statusCode >= 500:
		m.http5xx.Add(1)
	}

	m.mu.Lock()
	m.endpointHits[endpoint]++
	m.mu.Unlock()
}

// IncGet increments the get operation counter.
func (m *Metrics) IncGet() { m.opGets.Add(1) }

// IncSet increments the set operation counter.
func (m *Metrics) IncSet() { m.opSets.Add(1) }

// IncDel increments the del operation counter.
func (m *Metrics) IncDel() { m.opDels.Add(1) }

// IncList increments the list operation counter.
func (m *Metrics) IncList() { m.opLists.Add(1) }

// IncSnapshot increments the snapshot operation counter.
func (m *Metrics) IncSnapshot() { m.opSnapshots.Add(1) }

// Summary returns a map of metric values for JSON serialization.
func (m *Metrics) Summary(kv *KV) map[string]any {
	m.mu.RLock()
	endpointsCopy := make(map[string]uint64, len(m.endpointHits))
	for k, v := range m.endpointHits {
		endpointsCopy[k] = v
	}
	m.mu.RUnlock()

	uptime := time.Since(m.startTime).Round(time.Second)

	summary := map[string]any{
		"uptime_seconds": uptime.Seconds(),
		"uptime_human":   uptime.String(),
		"http_requests_total": map[string]uint64{
			"total": m.httpRequests.Load(),
			"2xx":   m.http2xx.Load(),
			"4xx":   m.http4xx.Load(),
			"5xx":   m.http5xx.Load(),
		},
		"operations_total": map[string]uint64{
			"get":      m.opGets.Load(),
			"set":      m.opSets.Load(),
			"del":      m.opDels.Load(),
			"list":     m.opLists.Load(),
			"snapshot": m.opSnapshots.Load(),
		},
		"endpoints": endpointsCopy,
	}

	if kv != nil {
		summary["kv"] = map[string]any{
			"keys_total":    kv.KeyCount(),
			"next_wal_idx":  kv.NextIdx(),
			"interval_secs": int(kv.Interval() / time.Second),
			"is_closed":     kv.Closed(),
		}
	}

	return summary
}

// MetricsMiddleware logs and records metrics for each HTTP request.
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)
		GlobalMetrics.RecordRequest(r.URL.Path, rw.statusCode)
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *statusResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
