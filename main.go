package main

import (
	"log"
	"net/http"
	"os"

	"mnah/nexus/internal"
)

func main() {
	// WAL path defaults to wal.log, overridable via first CLI arg.
	walPath := "wal.log"
	if len(os.Args) > 1 {
		walPath = os.Args[1]
	}

	// Recover state from the WAL before serving any request.
	kv, err := internal.NewKV(walPath)
	if err != nil {
		log.Fatalf("kv: %v", err)
	}

	// Port defaults to 8080, overridable via the PORT env var.
	addr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}

	log.Printf("nexus kv listening on %s (wal: %s)", addr, walPath)
	log.Fatal(http.ListenAndServe(addr, internal.NewRouter(kv)))
}
