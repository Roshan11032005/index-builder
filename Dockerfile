# ════════════════════════════════════════════════════════════════════════════════
# Stage 1: Build — pure Go, no CGO required
# ════════════════════════════════════════════════════════════════════════════════
FROM golang:1.22-bookworm AS builder

WORKDIR /app

# Download dependencies first (layer-cached until go.mod/go.sum change)
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source (includes committed proto/rocksdb/*.pb.go)
COPY . .

# Build a fully static binary (no CGO, no libc dependency)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o index-builder ./cmd/builder

# ════════════════════════════════════════════════════════════════════════════════
# Stage 2: Runtime — scratch (zero OS overhead, smallest possible image)
# ════════════════════════════════════════════════════════════════════════════════
FROM scratch

# CA certificates — needed for TLS connections to S3 / MinIO over HTTPS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# The binary
COPY --from=builder /app/index-builder /index-builder

# Operational HTTP server (metrics / health / force-compact)
EXPOSE 8086

# Default environment — override at runtime via docker run -e or .env mount
ENV HTTP_PORT=8086
ENV POLL_INTERVAL=10s
ENV COMPACTION_DELTA_LIMIT=50
ENV COMPACTION_MAX_AGE=24h
ENV SST_TEMP_DIR=/tmp/sst

ENTRYPOINT ["/index-builder"]