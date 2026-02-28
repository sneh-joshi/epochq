# EpochQ HTTP API Reference

**Base URL:** `http://<host>:<port>` (default port 8080)

**Authentication:** When `auth.enabled = true`, every request must include the header:
```
X-Api-Key: <your-api-key>
```

**Request/Response format:** `application/json`

**Message body encoding:** All message bodies are [base64](https://en.wikipedia.org/wiki/Base64) encoded (standard encoding, no line breaks). Decode the `body` field before processing.

**Time:** All timestamps are UTC milliseconds since Unix epoch (`int64`).

---

## Table of Contents

- [Health & Observability](#health--observability)
- [Namespaces](#namespaces)
- [Queues](#queues)
- [Messages — Publish](#messages--publish)
- [Messages — Consume](#messages--consume)
- [Messages — Ack / Nack](#messages--ack--nack)
- [Dead-Letter Queue (DLQ)](#dead-letter-queue-dlq)
- [Webhook Subscriptions](#webhook-subscriptions)
- [Error responses](#error-responses)

---

## Health & Observability

### `GET /health`

Returns the server's current status.

**Response `200 OK`**

```json
{
  "status": "ok",
  "node_id": "01JP5X4PRJ3G2V9WB8E7KT8M5F",
  "queues": 4,
  "uptime": "2m30s",
  "uptime_ms": 150000,
  "version": "1.0.0"
}
```

---

### `GET /metrics`

Returns Prometheus-format metrics (text/plain; version=0.0.4).
Also available on the dedicated metrics port (default 9090).

**Metrics exposed:**

| Metric | Labels | Description |
|--------|--------|-------------|
| `epochq_messages_published_total` | namespace, queue | Messages published |
| `epochq_messages_consumed_total` | namespace, queue | Messages delivered to consumers |
| `epochq_messages_acked_total` | namespace, queue | Messages ACKed |
| `epochq_messages_nacked_total` | namespace, queue | Messages NACKed |
| `epochq_messages_dlq_routed_total` | namespace, queue | Messages routed to DLQ |
| `epochq_http_requests_total` | method, path, status | HTTP requests |
| `epochq_http_request_duration_milliseconds_sum` | method, path | Sum of request durations |
| `epochq_http_request_duration_milliseconds_count` | method, path | Request count |

---

### `GET /dashboard`

Returns the built-in HTML dashboard. Open in a browser.

---

### `GET /api/stats`

### `GET /api/stats`

Returns a paginated page of per-queue depth snapshots (used by the dashboard).

**Query parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `page` | 1 | Page number (1-based) |
| `limit` | 50 | Page size (1–200) |

**Response `200 OK`**

```json
{
  "queues": [
    {
      "namespace": "payments",
      "name": "invoices",
      "key": "payments/invoices",
      "ready": 42,
      "in_flight": 3,
      "scheduled": 5,
      "depth": 50,
      "dlq_depth": 1
    }
  ],
  "total": 1,
  "page": 1,
  "limit": 50,
  "total_pages": 1
}
```

Queues are sorted alphabetically by namespace then name. DLQ depth is only looked up for queues on the current page — making this endpoint efficient regardless of total queue count.

---

### `GET /api/stats/summary`

Returns aggregate counts in a single O(N) pass — zero per-queue storage I/O. Used by the dashboard summary cards.

**Response `200 OK`**

```json
{
  "total_queues": 12,
  "namespaces": 3,
  "total_depth": 847,
  "total_scheduled": 35,
  "dlq_alerts": 2
}
```

| Field | Description |
|-------|-------------|
| `total_queues` | Total number of primary queues |
| `namespaces` | Number of unique namespaces with at least one queue |
| `total_depth` | Sum of ready + in-flight + scheduled across all queues |
| `total_scheduled` | Total messages awaiting future delivery |
| `dlq_alerts` | Number of queues that have at least one DLQ message |

---

## Namespaces

Namespaces are logical containers for queues. They are created automatically when a message is first published to a queue (auto-create behaviour). Use the namespace API to manage them explicitly.

---

### `POST /namespaces`

Create a namespace.

**Request body**

```json
{ "name": "payments" }
```

Name rules: 1–64 lowercase alphanumeric characters or hyphens, must start with a letter or digit.

**Response `201 Created`**

```json
{ "status": "created", "name": "payments" }
```

**Errors**

| Code | Meaning |
|------|---------|
| 400 | Invalid name |
| 409 | Namespace already exists |

---

### `GET /namespaces`

List all registered namespaces sorted by name.

**Response `200 OK`**

```json
{
  "namespaces": [
    { "name": "analytics", "created_at": 1740744000000 },
    { "name": "payments",   "created_at": 1740700000000 }
  ]
}
```

---

### `DELETE /namespaces/{ns}`

Delete a namespace. The caller is responsible for deleting all queues first.

**Response `204 No Content`**

**Errors**

| Code | Meaning |
|------|---------|
| 404 | Namespace not found |

---

## Queues

### `POST /namespaces/{ns}/queues/{name}`

Create a queue with optional configuration.

**Path parameters**

| Parameter | Description |
|-----------|-------------|
| `ns` | Namespace name |
| `name` | Queue name |

**Name rules:** Both namespace and queue name must match `^[a-z0-9][a-z0-9\-]{0,63}$` — lowercase letters, digits, and hyphens only; 1–64 characters; must begin with a letter or digit. Uppercase, underscores, spaces and other characters are rejected with `400 Bad Request`.

**Request body** (all fields optional — omit to use server defaults)

```json
{
  "visibility_timeout_ms": 30000,
  "max_messages": 100000,
  "max_retries": 3,
  "max_batch_size": 100
}
```

**Response `201 Created`**

```json
{ "status": "created" }
```

**Errors**

| Code | Meaning |
|------|---------|
| 400 | Invalid namespace or queue name (must be lowercase alphanumeric + hyphens) |

---

### `GET /namespaces/{ns}/queues`

List all queues in a namespace.

**Response `200 OK`**

```json
{
  "queues": [
    "payments/invoices",
    "payments/refunds"
  ]
}
```

---

### `DELETE /namespaces/{ns}/queues/{name}`

Permanently delete a queue and all its messages.

**Response `204 No Content`**

---

## Messages — Publish

### `POST /namespaces/{ns}/queues/{name}/messages`

Publish a single message.

**Request body**

```json
{
  "body": "<base64-encoded payload>",
  "deliver_at": 1740747600000,
  "max_retries": 3,
  "metadata": {
    "source": "checkout-service",
    "trace-id": "abc123"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `body` | string | yes | Base64-encoded message payload |
| `deliver_at` | int64 | no | UTC ms; 0 or omit = deliver immediately |
| `max_retries` | int | no | Override queue default; 0 = use queue default |
| `metadata` | object | no | User-defined string key/value pairs |

**Scheduled delivery:** If `deliver_at` is in the future, the message is placed in the scheduler's Min-Heap and will not appear in consume calls until the time is reached.

**Response `201 Created`**

```json
{ "id": "01JP5X4PRJ3G2V9WB8E7KT8M5F" }
```

---

### `POST /namespaces/{ns}/queues/{name}/messages/batch`

Publish multiple messages in a single request (up to `max_batch_size`, default 100).

**Request body** — array of message objects (same schema as single publish)

```json
[
  { "body": "bXNnMQ==" },
  { "body": "bXNnMg==", "deliver_at": 1740747600000 },
  { "body": "bXNnMw==", "metadata": { "priority": "high" } }
]
```

**Response `201 Created`**

```json
{ "ids": ["01JP…", "01JP…", "01JP…"] }
```

---

## Messages — Consume

### `GET /namespaces/{ns}/queues/{name}/messages`

Consume (dequeue) and lock messages for processing.

**Query parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `n` | 1 | Max messages to return (1–`max_batch_size`) |
| `visibility_timeout_ms` | 30000 | Lock duration; message reappears if not ACKed in time |

**Response `200 OK`**

```json
{
  "messages": [
    {
      "id": "01JP5X4PRJ3G2V9WB8E7KT8M5F",
      "body": "eyJhbW91bnQiOjQyfQ==",
      "receipt_handle": "01JP5X4…-rh",
      "namespace": "payments",
      "queue": "invoices",
      "attempt": 1,
      "published_at": 1740744000000,
      "metadata": { "source": "checkout-service" }
    }
  ]
}
```

Returns an empty `messages` array when the queue is empty. Never blocks.

**Important:** The message remains locked (`IN_FLIGHT`) until either:
- You call `DELETE /messages/{receipt_handle}` to ACK it, or
- The `visibility_timeout_ms` elapses, at which point it becomes READY again for redelivery.

---

## Messages — Ack / Nack

### `DELETE /messages/{receipt_handle}`

ACK a message — signal successful processing and permanently remove it from the queue.

**Path parameters:** `receipt_handle` from the consume response.

**Response `204 No Content`**

**Errors**

| Code | Meaning |
|------|---------|
| 410 | Receipt handle unknown (already ACKed, expired, or invalid) |

---

### `POST /messages/{receipt_handle}/nack`

NACK a message — signal failed processing. The message will be:
- Re-queued as READY (if remaining retries > 0), or
- Moved to the dead-letter queue (if retries exhausted).

**Response `204 No Content`**

---

## Dead-Letter Queue (DLQ)

Each queue has a paired dead-letter queue that receives messages when `max_retries` is exhausted. The DLQ is just another queue with a special name (`__dlq__<primary-name>`).

---

### `GET /namespaces/{ns}/queues/{name}/dlq`

Drain messages from the DLQ (peek-and-lock, like a regular consume).

**Query parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `limit` | 10 | Max messages to return |

**Response `200 OK`** — same shape as consume response.

---

### `POST /namespaces/{ns}/queues/{name}/dlq/replay`

Move messages from the DLQ back into the primary queue for re-processing.

**Query parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `limit` | 100 | Max messages to replay |

**Response `200 OK`**

```json
{ "replayed": 7 }
```

---

## Webhook Subscriptions

### `POST /namespaces/{ns}/queues/{name}/subscriptions`

Register a webhook. EpochQ will POST each READY message to the given URL.

**Request body**

```json
{
  "url": "https://myapp.example.com/webhook/invoices",
  "secret": "my-hmac-secret"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | string | yes | EpochQ will POST messages here |
| `secret` | string | no | HMAC-SHA256 signing secret; leave empty to disable |

**Webhook delivery**

EpochQ sends:
```
POST <url>
Content-Type: application/json
X-EpochQ-Signature: sha256=<hmac-sha256(secret, body)>

{message object}
```

- `200 OK` → auto-ACK
- `4xx` (except 429) → treated as permanent failure; message moves to DLQ immediately
- `5xx` or `429` or timeout → retry with exponential backoff (configured by `webhook.retry_delays_ms`)
- After all retries exhausted → message moves to DLQ

**Response `201 Created`**

```json
{ "id": "01JP5X4…-sub" }
```

---

### `DELETE /subscriptions/{id}`

Unregister a webhook subscription.

**Response `204 No Content`**

---

## WebSocket

### `GET /namespaces/{ns}/queues/{name}/ws`

Upgrade to a WebSocket connection. The server pushes messages as they become READY.

**Protocol:** The server sends JSON message objects and expects ACK frames in return. See [architecture.md](architecture.md) for the frame format.

---

## Error responses

All error responses use the same JSON shape:

```json
{ "error": "human readable description" }
```

| Status | When |
|--------|------|
| 400 Bad Request | Invalid JSON or missing required fields |
| 401 Unauthorized | Missing or invalid `X-Api-Key` |
| 404 Not Found | Namespace, queue, or receipt handle not found |
| 409 Conflict | Resource already exists (namespace, queue) |
| 410 Gone | Receipt handle expired or already ACKed |
| 429 Too Many Requests | Rate limit exceeded |
| 500 Internal Server Error | Unexpected server error (check server logs) |
