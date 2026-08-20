package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// NewRouter wires the HTTP API (GET/SET/DEL) backed by the KV service.
func NewRouter(kv *KV) http.Handler {
	r := chi.NewRouter()

	r.Get("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val, ok := kv.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		fmt.Fprint(w, val)
	})

	r.Get("/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(kv.List())
	})

	// POST /snapshot triggers an immediate snapshot + log truncation.
	r.Post("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if err := kv.Snapshot(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "OK")
	})

	// GET /config/snapshot returns the current snapshot interval.
	r.Get("/config/snapshot", func(w http.ResponseWriter, r *http.Request) {
		kv.mu.Lock()
		secs := int(kv.interval / time.Second)
		kv.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(struct {
			IntervalSecs int `json:"interval_secs"`
		}{IntervalSecs: secs})
	})

	// POST /config/snapshot sets the interval; {"interval_secs": 0} disables it.
	r.Post("/config/snapshot", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			IntervalSecs int `json:"interval_secs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := kv.SetTiming(time.Duration(req.IntervalSecs) * time.Second); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, "OK")
	})

	r.Post("/set", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key string `json:"key"`
			Val string `json:"val"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		idx, err := kv.Set(req.Key, req.Val)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "OK %d", idx)
	})

	r.Post("/del", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		idx, err := kv.Del(req.Key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "OK %d", idx)
	})

	return r
}
