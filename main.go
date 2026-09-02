package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
	if len(os.Args) > 1 {
		walPath = os.Args[1]
	}

	// Recover state from snapshot and WAL before serving any request.
	kv, err := internal.NewKV(walPath)
	if err != nil {
		slog.Error("failed to initialize kv", "error", err)
		os.Exit(1)
	}

	// Port defaults to 8080, overridable via the PORT env var.
	addr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           internal.NewRouter(kv),
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

	slog.Info("closing KV service and WAL...")
	if err := kv.Close(); err != nil {
		slog.Error("kv close error", "error", err)
	} else {
		slog.Info("KV service closed cleanly")
	}

	slog.Info("graceful shutdown complete")
}
