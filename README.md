# Nexus KV

A persistent, distributed key-value store built with Go and Raft consensus. It provides leader election, replicated writes, read replicas, atomic snapshots, WAL durability, and an embedded web dashboard.

## Demo

[Watch the Nexus demo video](https://raw.githubusercontent.com/mnah05/nexus/main/Cap%202026-09-03%20at%2008.06.58.mp4)

## Quick Start

```sh
make run             # Run one node at http://localhost:8080
make cluster-start   # Start a 3-node cluster on ports 8001-8003
make open-ui         # Open the cluster dashboard
make cluster-stop    # Stop the cluster
make test             # Run tests with the race detector
```

Docker Compose is also supported:

```sh
make docker-up
make docker-down
```

## API

| Method | Endpoint | Purpose |
| :--- | :--- | :--- |
| `GET` | `/` | Web dashboard |
| `GET` | `/get?key=<k>` | Read a value |
| `GET` | `/list` | List all values |
| `POST` | `/set` | Replicated leader write |
| `POST` | `/del` | Replicated leader delete |
| `GET` | `/raft/status` | Raft state and leader |
| `GET` | `/healthz` | Health check |
| `GET` | `/metrics` | Operational metrics |

## Development

Run tests directly with:

```sh
go test -v -race ./...
```

See the [architecture](docs/architecture.md), [Raft guide](docs/raft-election.md), and [consensus roadmap](docs/raft-roadmap.md) for more detail.
