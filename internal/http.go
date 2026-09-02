package internal

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

const (
	// MaxKeyLength is the maximum allowed length for a key (1 KB).
	MaxKeyLength = 1024
	// MaxValLength is the maximum allowed length for a value (1 MB).
	MaxValLength = 1024 * 1024
	// MaxBodyBytes limits HTTP request bodies to 2 MB to prevent memory exhaustion.
	MaxBodyBytes = 2 * 1024 * 1024
)

// writeJSON writes a JSON response with the appropriate Content-Type header.
func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

// writeError writes a structured JSON error response.
func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]string{"error": message})
}

// validateKey checks for non-empty key, length limit, and illegal characters.
func validateKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if len(key) > MaxKeyLength {
		return fmt.Errorf("key exceeds maximum length of %d bytes", MaxKeyLength)
	}
	if strings.ContainsAny(key, "\r\n\x00") {
		return fmt.Errorf("key contains invalid characters")
	}
	return nil
}

// validateVal checks for non-empty value and length limit.
func validateVal(val string) error {
	if val == "" {
		return fmt.Errorf("val cannot be empty")
	}
	if len(val) > MaxValLength {
		return fmt.Errorf("val exceeds maximum length of %d bytes", MaxValLength)
	}
	return nil
}

// SlogLoggingMiddleware logs requests using slog with execution duration and status code.
func SlogLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)
		duration := time.Since(start)

		// Don't spam logs for high-frequency polling on healthz/metrics/raft
		if r.URL.Path != "/healthz" && r.URL.Path != "/readyz" && r.URL.Path != "/metrics" && !strings.HasPrefix(r.URL.Path, "/raft/") {
			slog.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.statusCode,
				"duration_ms", duration.Milliseconds(),
				"remote", r.RemoteAddr,
			)
		}
	})
}

// NewRouter wires the HTTP API backed by the KV service and optional Raft node.
func NewRouter(kv *KV, raftNode *Node) http.Handler {
	r := chi.NewRouter()

	// Core middlewares
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(MetricsMiddleware)
	r.Use(SlogLoggingMiddleware)

	// Profiling endpoints at /debug/pprof
	r.Mount("/debug", middleware.Profiler())

	// Health check endpoint
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "healthy",
		})
	})
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "healthy",
		})
	})

	// Readiness check endpoint
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if kv != nil && kv.Closed() {
			writeError(w, http.StatusServiceUnavailable, "service is shutting down")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ready",
		})
	})
	r.Get("/ready", func(w http.ResponseWriter, r *http.Request) {
		if kv != nil && kv.Closed() {
			writeError(w, http.StatusServiceUnavailable, "service is shutting down")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ready",
		})
	})

	// Metrics endpoint (returns JSON operational metrics)
	r.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, GlobalMetrics.Summary(kv))
	})

	// Raft Consensus Endpoints
	if raftNode != nil {
		// GET /raft/status returns current cluster state of this node
		r.Get("/raft/status", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{
				"id":     raftNode.ID,
				"role":   raftNode.GetRole().String(),
				"term":   raftNode.Term(),
				"leader": raftNode.LeaderID(),
				"peers":  raftNode.peers,
			})
		})

		// POST /raft/request-vote handles incoming vote requests from candidates
		r.Post("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
			var args RequestVoteArgs
			if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
				writeError(w, http.StatusBadRequest, "bad json: "+err.Error())
				return
			}
			reply := raftNode.HandleRequestVote(args)
			writeJSON(w, http.StatusOK, reply)
		})

		// POST /raft/append-entries handles incoming heartbeats from the leader
		r.Post("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
			var args AppendEntriesArgs
			if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
				writeError(w, http.StatusBadRequest, "bad json: "+err.Error())
				return
			}
			reply := raftNode.HandleAppendEntries(args)
			writeJSON(w, http.StatusOK, reply)
		})
	}

	// GET /get?key=foo (Serves reads on both Leader and Read Replicas!)
	r.Get("/get", func(w http.ResponseWriter, r *http.Request) {
		GlobalMetrics.IncGet()
		key := r.URL.Query().Get("key")
		if err := validateKey(key); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		val, ok := kv.Get(key)
		if !ok {
			writeError(w, http.StatusNotFound, "key not found")
			return
		}

		// Backward-compatibility: support ?format=raw or Accept: text/plain
		if r.URL.Query().Get("format") == "raw" || r.Header.Get("Accept") == "text/plain" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, val)
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{
			"key": key,
			"val": val,
		})
	})

	// GET /list (Serves reads on both Leader and Read Replicas!)
	r.Get("/list", func(w http.ResponseWriter, r *http.Request) {
		GlobalMetrics.IncList()
		writeJSON(w, http.StatusOK, kv.List())
	})

	// POST /snapshot (Leader only!)
	r.Post("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if raftNode != nil && !raftNode.IsLeader() {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"error":  "not leader",
				"leader": raftNode.LeaderID(),
			})
			return
		}

		GlobalMetrics.IncSnapshot()
		if err := kv.Snapshot(); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"message": "snapshot complete",
		})
	})

	// GET /config/snapshot returns the current snapshot interval.
	r.Get("/config/snapshot", func(w http.ResponseWriter, r *http.Request) {
		secs := int(kv.Interval() / time.Second)
		writeJSON(w, http.StatusOK, map[string]int{
			"interval_secs": secs,
		})
	})

	// POST /config/snapshot sets the interval; {"interval_secs": 0} disables it.
	r.Post("/config/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if raftNode != nil && !raftNode.IsLeader() {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"error":  "not leader",
				"leader": raftNode.LeaderID(),
			})
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		var req struct {
			IntervalSecs *int `json:"interval_secs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad json: "+err.Error())
			return
		}
		if req.IntervalSecs == nil {
			writeError(w, http.StatusBadRequest, "missing required field: interval_secs")
			return
		}
		if *req.IntervalSecs < 0 {
			writeError(w, http.StatusBadRequest, "interval_secs must be >= 0")
			return
		}

		if err := kv.SetTiming(time.Duration(*req.IntervalSecs) * time.Second); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":            true,
			"interval_secs": *req.IntervalSecs,
		})
	})

	// POST /set (Mutations only allowed on Leader; Followers return leader address!)
	r.Post("/set", func(w http.ResponseWriter, r *http.Request) {
		if raftNode != nil && !raftNode.IsLeader() {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"error":  "not leader",
				"leader": raftNode.LeaderID(),
			})
			return
		}

		GlobalMetrics.IncSet()
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		var req struct {
			Key string `json:"key"`
			Val string `json:"val"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad json: "+err.Error())
			return
		}

		if err := validateKey(req.Key); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := validateVal(req.Val); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		idx, err := kv.Set(req.Key, req.Val)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"ok":  true,
			"idx": idx,
			"key": req.Key,
			"val": req.Val,
		})
	})

	// POST /del (Mutations only allowed on Leader; Followers return leader address!)
	r.Post("/del", func(w http.ResponseWriter, r *http.Request) {
		if raftNode != nil && !raftNode.IsLeader() {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"error":  "not leader",
				"leader": raftNode.LeaderID(),
			})
			return
		}

		GlobalMetrics.IncDel()
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad json: "+err.Error())
			return
		}

		if err := validateKey(req.Key); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		idx, err := kv.Del(req.Key)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"ok":  true,
			"idx": idx,
			"key": req.Key,
		})
	})

	return r
}
