# EpochQ

> **The queue that replaces your cron jobs.**

EpochQ is a lightweight, self-hostable message queue server built for developers who need **durable, time-based message delivery** without the operational complexity of Kafka, RabbitMQ, or cloud-vendor lock-in.

[![Go](https://img.shields.io/badge/Go-1.24-blue)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Website](https://img.shields.io/badge/website-epochq.dev-7c5cfc)](https://sneh-joshi.github.io/epochq)

---

## Why EpochQ?

| Problem | Old Way | With EpochQ |
|---|---|---|
| Send email 1 hr after signup | Cron job + DB polling | `Publish(body, WithDelay(time.Hour))` |
| Retry failed payment after 24 hr | Scheduler service | `Publish(body, WithDelay(24*time.Hour))` |
| Cancel unpaid order in 30 min | DB scan every minute | `Publish(body, WithDeliverAt(orderTime+30min))` |
| Publish blog post at 9 am | Cron + CMS flag | `Publish(body, WithDeliverAt(monday9am))` |

**Key differentiator:** Native scheduled delivery via an in-memory Min-Heap — O(1) peek, O(log N) insert, no polling.

---

## Features

- **Scheduled delivery** — `deliverAt` in any future UTC millisecond, up to 90 days ahead
- **Three consumer models** — HTTP poll, WebSocket push, Webhook push
- **Dead-letter queue** — automatic DLQ per queue, manual replay via API
- **Durable storage** — append-only WAL + bbolt index, survives restarts
- **Visibility timeout + ACK** — at-least-once, exactly-once-friendly delivery
- **Namespaces** — logical grouping, auto-created on first use
- **Auth** — static API key (`X-Api-Key` header)
- **Prometheus metrics** — `/metrics` endpoint on port 9090
- **Built-in dashboard** — `/dashboard` with live queue depths
- **Single binary** — no runtime dependencies, ~20 MB Docker image
- **Go SDK** — idiomatic client for producers and consumers

---

## Quick Start

### Docker Compose (recommended)

```bash
git clone https://github.com/sneh-joshi/epochq
cd epochq/docker
docker compose up -d
```

Open your browser at http://localhost:8080/dashboard.

### Binary

```bash
go build -o epochq ./cmd/server
./epochq --config config.yaml
```

### Docker (single container)

```bash
docker run -p 8080:8080 -p 9090:9090 \
  -v $(pwd)/data:/data \
  epochq/epochq:latest
```

---

## HTTP API — 30 second tour

```bash
BASE=http://localhost:8080

# Publish immediately
curl -s -X POST $BASE/namespaces/payments/queues/invoices/messages \
  -H "Content-Type: application/json" \
  -d '{"body":"eyJhbW91bnQiOjQyfQ=="}'   # base64("{"amount":42}")

# Publish in 1 hour (deliverAt = now + 3600000 ms)
curl -s -X POST $BASE/namespaces/payments/queues/invoices/messages \
  -H "Content-Type: application/json" \
  -d "{\"body\":\"eyJhbW91bnQiOjk5fQ==\", \"deliver_at\":$(( $(date +%s%3N) + 3600000 ))}"

# Consume (poll up to 10 messages)
curl -s "$BASE/namespaces/payments/queues/invoices/messages?n=10"

# ACK
curl -s -X DELETE "$BASE/messages/<receipt_handle>"

# NACK (requeue)
curl -s -X POST "$BASE/messages/<receipt_handle>/nack"
```

---

## Go SDK

```go
import "github.com/sneh-joshi/epochq/pkg/client"

c := client.New("http://localhost:8080",
    client.WithAPIKey("your-secret"),  // omit when auth is disabled
)

// Publish immediately
id, err := c.Publish(ctx, "payments", "invoices", []byte(`{"amount":42}`))

// Schedule in 1 hour
id, err = c.Publish(ctx, "payments", "invoices", payload,
    client.WithDelay(time.Hour),
)

// Schedule at an absolute time
id, err = c.Publish(ctx, "payments", "invoices", payload,
    client.WithDeliverAt(time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)),
)

// Batch publish
ids, err := c.PublishBatch(ctx, "payments", "invoices", [][]byte{body1, body2})

// Consume
msgs, err := c.Consume(ctx, "payments", "invoices", 10,
    client.WithVisibilityTimeout(60*time.Second),
)
for _, m := range msgs {
    if err := process(m.Body); err != nil {
        _ = c.Nack(ctx, m.ReceiptHandle) // requeue
        continue
    }
    _ = c.Ack(ctx, m.ReceiptHandle)
}

// DLQ operations
dlqMsgs, _ := c.DrainDLQ(ctx, "payments", "invoices", 100)
replayed, _ := c.ReplayDLQ(ctx, "payments", "invoices", 100)

// Webhook subscription
subID, _ := c.Subscribe(ctx, "payments", "invoices", "https://myapp.com/hook", "hmac-secret")
_ = c.Unsubscribe(ctx, subID)

// Namespace management
_ = c.CreateNamespace(ctx, "analytics")
nsList, _ := c.ListNamespaces(ctx)
_ = c.DeleteNamespace(ctx, "analytics")

// Observability
health, _ := c.Health(ctx)   // status, nodeID, uptime, version
stats, _ := c.Stats(ctx)     // per-queue ready/in-flight/dlq depths
```

### Consumer poll loop pattern

```go
ticker := time.NewTicker(500 * time.Millisecond)
defer ticker.Stop()

for range ticker.C {
    msgs, err := c.Consume(ctx, "payments", "invoices", 10)
    if err != nil {
        log.Println("consume error:", err)
        continue
    }
    for _, m := range msgs {
        if err := handle(m); err != nil {
            _ = c.Nack(ctx, m.ReceiptHandle)
        } else {
            _ = c.Ack(ctx, m.ReceiptHandle)
        }
    }
}
```

---

## Configuration

```yaml
# config.yaml (minimal)
node:
  host: "0.0.0.0"
  port: 8080
  data_dir: "./data"

auth:
  enabled: false        # set to true + api_key to require X-Api-Key
  api_key: ""

metrics:
  enabled: true
  port: 9090            # Prometheus scrape target: http://host:9090/metrics

queue:
  default_visibility_timeout_ms: 30000
  max_retries: 5
  max_messages: 100000
```

See [config.yaml](config.yaml) for the full reference with all defaults.

---

## Observability

| Endpoint | Description |
|---|---|
| `GET /health` | JSON status, node ID, queue count, uptime, version |
| `GET /metrics` | Prometheus text (also available on port 9090) |
| `GET /dashboard` | Live browser dashboard |
| `GET /api/stats` | JSON queue depths (used by dashboard) |

### Prometheus metrics

```
epochq_messages_published_total{namespace,queue}
epochq_messages_consumed_total{namespace,queue}
epochq_messages_acked_total{namespace,queue}
epochq_messages_nacked_total{namespace,queue}
epochq_messages_dlq_routed_total{namespace,queue}
epochq_http_requests_total{method,path,status}
epochq_http_request_duration_milliseconds_sum{method,path}
epochq_http_request_duration_milliseconds_count{method,path}
```

---

## Performance (single node)

| Metric | Value |
|---|---|
| Write throughput (fsync=interval) | ~50,000 msgs/sec |
| Write throughput (fsync=never) | ~500,000 msgs/sec |
| Read throughput | ~100,000 msgs/sec |
| Scheduled message heap | 10M entries ≈ 1 GB RAM |
| Min-heap peek latency | O(1) |
| Delivery accuracy | ±10 ms (OS timer resolution) |

---

## Architecture overview

```
Producer → HTTP POST → Broker.Publish → queue.Manager → StorageEngine (WAL+Log+Index)
                                           ↓
                                    Scheduler (Min-Heap)
                                           ↓ deliverAt reached
Consumer ← HTTP GET  ← Broker.Consume ← queue.Queue.DequeueN
Consumer → DELETE    → Broker.Ack     → queue.Queue.Ack
Consumer → POST/nack → Broker.Nack    → queue.Queue.Nack → DLQ (if maxRetries hit)
```

See [docs/architecture.md](docs/architecture.md) for the full design.

---

## Project structure

```
epochq/
├── cmd/server/          — server entry point
├── pkg/client/          — public Go SDK
├── internal/
│   ├── broker/          — central orchestrator
│   ├── queue/           — state machine, manager, message
│   ├── scheduler/       — min-heap timer goroutine
│   ├── storage/local/   — WAL, append log, bbolt index, compaction
│   ├── dlq/             — dead-letter queue manager
│   ├── namespace/       — namespace registry (JSON persistence)
│   ├── metrics/         — Prometheus text exporter (no client_golang)
│   ├── consumer/        — webhook delivery + manager
│   ├── config/          — YAML config loader + validation
│   ├── node/            — ULID node identity
│   └── transport/       — HTTP + WebSocket handlers
├── dashboard/           — dashboard HTML (served at /dashboard)
├── docker/              — Dockerfile + docker-compose.yml
├── docs/                — architecture, API reference, getting started
└── config.yaml          — default configuration
```

---

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR.

```bash
go test ./... -count=1 -timeout 90s   # run all tests
go build ./...                         # verify compilation
```

Please follow our [Code of Conduct](CODE_OF_CONDUCT.md). For security issues, see [SECURITY.md](SECURITY.md).

---

## License

MIT — see [LICENSE](LICENSE).
