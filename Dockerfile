# Stage 1: Build the binary with embedded web UI
FROM golang:1.24-alpine AS builder

WORKDIR /build

# Copy go.mod and go.sum for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and embedded web assets
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o nexus-server main.go

# Stage 2: Minimal runtime image
FROM alpine:3.21

WORKDIR /app

RUN apk --no-cache add ca-certificates curl tzdata && \
    mkdir -p /data

# Copy compiled binary from builder
COPY --from=builder /build/nexus-server /app/nexus-server

# Expose default port
EXPOSE 8080

# Default entrypoint
ENTRYPOINT ["/app/nexus-server"]
CMD ["/data/wal.log"]
