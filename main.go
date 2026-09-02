package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mnah/nexus/internal"
)

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
		raftNode = internal.NewNode(nodeID, peers)
	}

	// Recover state from snapshot and WAL before serving any request.
	kv, err := internal.NewKV(walPath)
	if err != nil {
		slog.Error("failed to initialize kv", "error", err)
		if raftNode != nil {
			raftNode.Close()
		}
		os.Exit(1)
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           internal.NewRouter(kv, raftNode),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Channel to catch OS termination signals
	shutdownSignal := make(chan os.Signal, 1)
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
