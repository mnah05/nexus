package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mnah/nexus/internal"
)

func replaceEnv(existing []string, updates map[string]string) []string {
	result := make([]string, 0, len(existing)+len(updates))
	for _, entry := range existing {
		key, _, _ := strings.Cut(entry, "=")
		if value, ok := updates[key]; ok {
			result = append(result, key+"="+value)
			delete(updates, key)
			continue
		}
		result = append(result, entry)
	}
	for key, value := range updates {
		result = append(result, key+"="+value)
	}
	return result
}

func main() {
	// Initialize structured logger
	var logHandler slog.Handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	if os.Getenv("LOG_FORMAT") == "json" {
		logHandler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	}
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	// WAL path defaults to wal.log, overridable via first CLI arg.
	walPath := "wal.log"
	if len(os.Args) > 1 && os.Args[1] != "" {
		walPath = os.Args[1]
	}

	// Port defaults to 8080, overridable via the PORT env var.
	addr := ":8080"
	port := "8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
		port = p
	}

	// Cluster configuration (NODE_ID and PEERS)
	// Example: NODE_ID=localhost:8001 PEERS=localhost:8002,localhost:8003
	nodeID := os.Getenv("NODE_ID")
	peersStr := os.Getenv("PEERS")

	// Allow CLI overrides:
	// ./nexus-server <walPath> <nodeID> <peer1,peer2>
	if len(os.Args) > 2 {
		nodeID = os.Args[2]
	}
	if len(os.Args) > 3 {
		peersStr = os.Args[3]
	}

	// Recover state from snapshot and WAL before serving any request.
	kv, err := internal.NewKV(walPath)
	if err != nil {
		slog.Error("failed to initialize kv", "error", err)
		os.Exit(1)
	}

	var raftNode *internal.Node
	if nodeID != "" || peersStr != "" {
		if nodeID == "" {
			nodeID = "localhost:" + port
		}
		var peers []string
		if peersStr != "" {
			for _, p := range strings.Split(peersStr, ",") {
				p = strings.TrimSpace(p)
				if p != "" && p != nodeID {
					peers = append(peers, p)
				}
			}
		}
		slog.Info("starting in Raft cluster mode", "node_id", nodeID, "peers", peers)
		raftNode = internal.NewNode(nodeID, peers, kv)
	}

	// Admin controls are intentionally local-demo oriented. They let the UI
	// stop a node to exercise elections and restart it without losing its WAL.
	shutdownSignal := make(chan os.Signal, 1)

	server := &http.Server{
		Addr:              addr,
		Handler:           internal.NewRouter(kv, raftNode),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	baseHandler := server.Handler
	adminHandler := http.NewServeMux()
	adminHandler.HandleFunc("/admin/shutdown", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"message": "node shutting down",
		})
		go func() {
			time.Sleep(100 * time.Millisecond)
			select {
			case shutdownSignal <- syscall.SIGTERM:
			default:
			}
		}()
	})
	adminHandler.HandleFunc("/admin/restart", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"message": "node restarting",
		})
		go func() {
			time.Sleep(100 * time.Millisecond)
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdownCtx); err != nil {
				slog.Error("restart shutdown error", "error", err)
			}
			if raftNode != nil {
				raftNode.Close()
			}
			if err := kv.Close(); err != nil {
				slog.Error("restart KV close error", "error", err)
			}
			executable, err := os.Executable()
			if err != nil {
				slog.Error("restart executable lookup failed", "error", err)
				os.Exit(1)
			}
			if err := syscall.Exec(executable, os.Args, os.Environ()); err != nil {
				slog.Error("node restart failed", "error", err)
				os.Exit(1)
			}
		}()
	})
	adminHandler.HandleFunc("/admin/restart-peer", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var request struct {
			Port string `json:"port"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&request); err != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "bad json: " + err.Error()})
			return
		}
		portNumber, err := strconv.Atoi(request.Port)
		if err != nil || portNumber < 8001 || portNumber > 8003 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "port must be 8001, 8002, or 8003"})
			return
		}
		if request.Port == port {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "use /admin/restart for the current node"})
			return
		}

		// This peer launcher is intentionally limited to the local Makefile layout.
		// Docker has its own restart policy and arbitrary deployments need a supervisor.
		if !strings.HasPrefix(nodeID, "localhost:") || !strings.Contains(walPath, "/tmp/nexus_cluster/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "peer restart is available for make cluster-start only"})
			return
		}

		targetAddress := "localhost:" + request.Port
		if connection, dialErr := net.DialTimeout("tcp", targetAddress, 150*time.Millisecond); dialErr == nil {
			_ = connection.Close()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "node is already online"})
			return
		}

		peers := make([]string, 0, 2)
		for candidate := 8001; candidate <= 8003; candidate++ {
			if candidate != portNumber {
				peers = append(peers, fmt.Sprintf("localhost:%d", candidate))
			}
		}
		targetWal := fmt.Sprintf("/tmp/nexus_cluster/n%d.wal", portNumber-8000)
		executable, err := os.Executable()
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		env := replaceEnv(os.Environ(), map[string]string{
			"PORT":    request.Port,
			"NODE_ID": targetAddress,
			"PEERS":   strings.Join(peers, ","),
		})
		process, err := os.StartProcess(executable, []string{executable, targetWal}, &os.ProcAttr{
			Env:   env,
			Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		})
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = process.Release()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "message": "peer restart launched"})
	})
	adminHandler.Handle("/", baseHandler)
	server.Handler = adminHandler

	// Channel to catch OS termination signals
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM)

	// Start server in background goroutine
	serverErr := make(chan error, 1)
	go func() {
		slog.Info("nexus kv listening", "addr", addr, "wal", walPath)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Wait for OS shutdown signal or server error
	select {
	case err := <-serverErr:
		slog.Error("server error", "error", err)
		if raftNode != nil {
			raftNode.Close()
		}
		_ = kv.Close()
		os.Exit(1)
	case sig := <-shutdownSignal:
		slog.Info("shutdown signal received", "signal", sig.String())
	}

	// Graceful shutdown context with 10s timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	slog.Info("shutting down HTTP server...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "error", err)
	} else {
		slog.Info("HTTP server stopped cleanly")
	}

	if raftNode != nil {
		slog.Info("stopping Raft node...")
		raftNode.Close()
		slog.Info("Raft node stopped cleanly")
	}

	slog.Info("closing KV service and WAL...")
	if err := kv.Close(); err != nil {
		slog.Error("kv close error", "error", err)
	} else {
		slog.Info("KV service closed cleanly")
	}

	slog.Info("graceful shutdown complete")
}
