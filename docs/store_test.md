# `internal/store_test.go` — Store Tests

Unit tests for the in-memory store. Run with:

```sh
go test -race ./...
```

The `-race` flag matters: the concurrency test is designed to expose
data races under the race detector.

## Coverage

| Test | Verifies |
|---|---|
| `TestStoreGetSet` | Basic set/get round-trip; missing keys return `(zero, false)` |
| `TestStoreSetOverwrite` | Setting an existing key replaces the value |
| `TestStoreDel` | Delete removes the key; deleting a missing key is a safe no-op |
| `TestStoreConcurrent` | 8 writer goroutines × 10k ops hammering the same key — passes only if locking is correct |
| `TestStoreGenericTypes` | Works with `string` and struct value types |

## Design note

Tests share one key where contention is wanted (`TestStoreConcurrent`)
to maximize collision probability, and use distinct keys elsewhere to
keep assertions deterministic.
