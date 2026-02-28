# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Cache module downloads separately from source files.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w -extldflags=-static" \
    -trimpath \
    -o /out/liteq \
    ./cmd/server

# ── Stage 2: minimal runtime image ───────────────────────────────────────────
FROM scratch

# Copy timezone data and CA certificates from the builder so HTTPS webhooks work.
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the binary.
COPY --from=builder /out/liteq /liteq

# Data volume: queue storage + node identity are persisted here.
VOLUME ["/data"]

# Main HTTP port.
EXPOSE 8080
# Prometheus metrics port.
EXPOSE 9090

ENTRYPOINT ["/liteq"]
CMD ["--config", "/etc/liteq/config.yaml"]
