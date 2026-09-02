package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func setupTestServer(t *testing.T) (*KV, http.Handler) {
	t.Helper()
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "http_test.wal")

	kv, err := NewKV(walPath)
	if err != nil {
		t.Fatalf("failed to create test KV: %v", err)
	}
	t.Cleanup(func() {
		_ = kv.Close()
	})

	router := NewRouter(kv, nil)
	return kv, router
}

func TestHTTPConsistentJSONResponses(t *testing.T) {
	_, router := setupTestServer(t)

	// 1. POST /set -> returns JSON with Content-Type: application/json
	setBody := `{"key":"greet","val":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/set", bytes.NewBufferString(setBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on set, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json Content-Type, got %s", ct)
	}

	var setResp struct {
		OK  bool   `json:"ok"`
		Idx uint64 `json:"idx"`
		Key string `json:"key"`
		Val string `json:"val"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&setResp); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}
	if !setResp.OK || setResp.Idx != 1 || setResp.Key != "greet" || setResp.Val != "hello" {
		t.Fatalf("unexpected set response: %+v", setResp)
	}

	// 2. GET /get?key=greet -> returns JSON with Content-Type: application/json
	req = httptest.NewRequest(http.MethodGet, "/get?key=greet", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on get, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json Content-Type, got %s", ct)
	}

	var getResp struct {
		Key string `json:"key"`
		Val string `json:"val"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&getResp); err != nil {
		t.Fatalf("failed to decode get JSON: %v", err)
	}
	if getResp.Key != "greet" || getResp.Val != "hello" {
		t.Fatalf("unexpected get response: %+v", getResp)
	}

	// 3. GET /get?key=greet&format=raw -> returns raw string for backward compatibility
	req = httptest.NewRequest(http.MethodGet, "/get?key=greet&format=raw", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on raw get, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("expected text/plain Content-Type, got %s", ct)
	}
	if rec.Body.String() != "hello" {
		t.Fatalf("expected hello, got %q", rec.Body.String())
	}

	// 4. GET /list -> returns JSON with Content-Type: application/json
	req = httptest.NewRequest(http.MethodGet, "/list", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on list, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json Content-Type, got %s", ct)
	}
	var listResp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&listResp); err != nil {
		t.Fatalf("failed to decode list JSON: %v", err)
	}
	if listResp["greet"] != "hello" {
		t.Fatalf("unexpected list response: %+v", listResp)
	}

	// 5. POST /snapshot -> returns JSON with Content-Type: application/json
	req = httptest.NewRequest(http.MethodPost, "/snapshot", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on snapshot, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json Content-Type, got %s", ct)
	}

	// 6. POST /del -> returns JSON with Content-Type: application/json
	delBody := `{"key":"greet"}`
	req = httptest.NewRequest(http.MethodPost, "/del", bytes.NewBufferString(delBody))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on del, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json Content-Type, got %s", ct)
	}

	var delResp struct {
		OK  bool   `json:"ok"`
		Idx uint64 `json:"idx"`
		Key string `json:"key"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&delResp); err != nil {
		t.Fatalf("failed to decode del JSON: %v", err)
	}
	if !delResp.OK || delResp.Idx != 2 || delResp.Key != "greet" {
		t.Fatalf("unexpected del response: %+v", delResp)
	}

	// 7. GET /get?key=greet (now deleted) -> 404 with JSON error
	req = httptest.NewRequest(http.MethodGet, "/get?key=greet", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json Content-Type on 404, got %s", ct)
	}
}

func TestHTTPInputValidation(t *testing.T) {
	_, router := setupTestServer(t)

	tests := []struct {
		name       string
		method     string
		url        string
		body       string
		wantCode   int
		wantErrSub string
	}{
		{
			name:       "empty key on /get",
			method:     http.MethodGet,
			url:        "/get?key=",
			body:       "",
			wantCode:   http.StatusBadRequest,
			wantErrSub: "key cannot be empty",
		},
		{
			name:       "whitespace key on /get",
			method:     http.MethodGet,
			url:        "/get?key=%20%20%20",
			body:       "",
			wantCode:   http.StatusBadRequest,
			wantErrSub: "key cannot be empty",
		},
		{
			name:       "oversized key on /get",
			method:     http.MethodGet,
			url:        "/get?key=" + strings.Repeat("a", 1025),
			body:       "",
			wantCode:   http.StatusBadRequest,
			wantErrSub: "key exceeds maximum length",
		},
		{
			name:       "key with newline on /set",
			method:     http.MethodPost,
			url:        "/set",
			body:       `{"key":"foo\nbar","val":"123"}`,
			wantCode:   http.StatusBadRequest,
			wantErrSub: "key contains invalid characters",
		},
		{
			name:       "empty key on /set",
			method:     http.MethodPost,
			url:        "/set",
			body:       `{"key":"","val":"123"}`,
			wantCode:   http.StatusBadRequest,
			wantErrSub: "key cannot be empty",
		},
		{
			name:       "empty val on /set",
			method:     http.MethodPost,
			url:        "/set",
			body:       `{"key":"foo","val":""}`,
			wantCode:   http.StatusBadRequest,
			wantErrSub: "val cannot be empty",
		},
		{
			name:       "oversized val on /set",
			method:     http.MethodPost,
			url:        "/set",
			body:       fmt.Sprintf(`{"key":"foo","val":"%s"}`, strings.Repeat("x", 1024*1024+1)),
			wantCode:   http.StatusBadRequest,
			wantErrSub: "val exceeds maximum length",
		},
		{
			name:       "negative interval on /config/snapshot",
			method:     http.MethodPost,
			url:        "/config/snapshot",
			body:       `{"interval_secs": -10}`,
			wantCode:   http.StatusBadRequest,
			wantErrSub: "interval_secs must be >= 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.url, bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("expected status %d, got %d, body: %s", tc.wantCode, rec.Code, rec.Body.String())
			}

			var errResp struct {
				Error string `json:"error"`
			}
			if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
				t.Fatalf("expected json error response: %v", err)
			}
			if !strings.Contains(errResp.Error, tc.wantErrSub) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErrSub, errResp.Error)
			}
		})
	}
}

func TestHTTPObservabilityEndpoints(t *testing.T) {
	kv, router := setupTestServer(t)

	// 1. /healthz
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /healthz, got %d", rec.Code)
	}

	// 2. /readyz
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /readyz, got %d", rec.Code)
	}

	// 3. /metrics (JSON)
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /metrics, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json for metrics, got %s", ct)
	}

	var metricsResp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&metricsResp); err != nil {
		t.Fatalf("expected valid JSON from /metrics: %v", err)
	}
	if _, ok := metricsResp["uptime_seconds"]; !ok {
		t.Fatalf("metrics missing uptime_seconds: %v", metricsResp)
	}

	// 5. /debug/pprof/
	req = httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /debug/pprof/, got %d", rec.Code)
	}

	// 6. Test /readyz returns 503 when KV is closed
	_ = kv.Close()
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on /readyz after KV is closed, got %d", rec.Code)
	}
}

func TestRaftHTTPFollowerRejectionAndLeaderAcceptance(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "raft_http.wal")
	kv, err := NewKV(walPath)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}
	defer kv.Close()

	// Create node as Follower with known leader "node-1:8001"
	node := NewNode("node-2:8002", []string{"node-1:8001"}, kv)
	defer node.Close()

	node.mu.Lock()
	node.Role = StateFollower
	node.leaderID = "node-1:8001"
	node.mu.Unlock()

	router := NewRouter(kv, node)

	// 1. /raft/status
	req := httptest.NewRequest(http.MethodGet, "/raft/status", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /raft/status, got %d", rec.Code)
	}

	// 2. Follower rejects POST /set and returns leader address!
	setBody := `{"key":"k1","val":"v1"}`
	req = httptest.NewRequest(http.MethodPost, "/set", bytes.NewBufferString(setBody))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on follower write, got %d", rec.Code)
	}
	var errResp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode follower rejection: %v", err)
	}
	if errResp["error"] != "not leader" || errResp["leader"] != "node-1:8001" {
		t.Fatalf("unexpected rejection payload: %+v", errResp)
	}

	// 3. Promote node to Leader and verify POST /set succeeds!
	node.mu.Lock()
	node.Role = StateLeader
	node.mu.Unlock()

	req = httptest.NewRequest(http.MethodPost, "/set", bytes.NewBufferString(setBody))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on leader write, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHTTPWebUIServing(t *testing.T) {
	_, router := setupTestServer(t)

	// 1. GET / should return index.html
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on /, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("expected text/html content-type on /, got %s", ct)
	}
	if !strings.Contains(rec.Body.String(), "Nexus") {
		t.Fatalf("expected index.html content on /")
	}

	// 2. GET /static/app.css should return CSS
	req = httptest.NewRequest(http.MethodGet, "/static/app.css", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on /static/app.css, got %d", rec.Code)
	}
}


