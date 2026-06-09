# MarketPulse 📊

A real-time e-commerce analytics platform built to explore event-driven architecture in Go. Tracks storefront events (page views, cart actions, orders), streams them through Kafka, and serves analytics via a REST API backed by MongoDB and Redis.

**Live demo:** `https://marketpulse-demo.up.railway.app` *(see [deploy guide](#deployment))*

---

## Why I built this

I wanted to go deeper on a few things that came up repeatedly in production:
- How to handle high-throughput event ingestion without blocking the caller
- The trade-offs of pre-computed aggregates vs on-the-fly MongoDB aggregation pipelines
- Making Kafka consumer offsets truly safe under horizontal scaling
- Keeping microservices genuinely independent (no shared DB, no sync RPC calls)

The domain is e-commerce because the data is rich: you get funnels, time-series revenue, per-product drill-downs, and real-time concurrency — all interesting problems.

---

## Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              Client (SDK / browser)          │
                    └────────────────────┬────────────────────────┘
                                         │ POST /v1/events
                                         │ POST /v1/orders
                    ┌────────────────────▼────────────────────────┐
                    │          Ingestion Service  :8080            │
                    │   • Validates & enriches events              │
                    │   • Publishes to Kafka (async)               │
                    │   • Returns 202 immediately                  │
                    └────────────────────┬────────────────────────┘
                                         │
                               Kafka topic: ecommerce.events
                               (3 partitions, keyed by store_id)
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │          Stream Processor  (×3)              │
                    │   • Consumes events (manual offset commit)   │
                    │   • Persists raw events → MongoDB            │
                    │   • Extracts orders → MongoDB                │
                    │   • Increments Redis real-time counters      │
                    │   • Updates daily aggregates (upsert)        │
                    └────────────────────┬────────────────────────┘
                                         │
                            MongoDB          Redis
                         ┌──────────┐    ┌──────────┐
                         │ events   │    │ rt:*     │ ← 5-min sliding counters
                         │ orders   │    │ cache:*  │ ← query result cache
                         │ aggs     │    └──────────┘
                         └────┬─────┘
                              │
                    ┌─────────▼───────────────────────────────────┐
                    │          Analytics API  :8081                │
                    │   GET /v1/analytics/revenue                  │
                    │   GET /v1/analytics/funnel                   │
                    │   GET /v1/analytics/top-products             │
                    │   GET /v1/analytics/realtime                 │
                    │   GET /v1/analytics/searches                 │
                    └─────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Language | Go 1.22 | Concurrency primitives, fast startup, small binaries |
| Event streaming | Apache Kafka | Durable, ordered, horizontally scalable |
| Primary storage | MongoDB 7 | Flexible schema for events; aggregation pipeline for analytics |
| Cache | Redis 7 | Sub-ms real-time counters; query result caching |
| HTTP | Gin | Low overhead, good middleware ecosystem |
| Observability | Prometheus + Zap | Structured logs, standard metrics endpoint |
| Containers | Docker + K8s | Multi-stage builds → ~15MB distroless images |
| CI/CD | GitHub Actions | Lint → test → build → push → deploy pipeline |

---

## Project Structure

```
marketpulse/
├── cmd/
│   ├── ingestion/       # HTTP server + Kafka producer
│   ├── processor/       # Kafka consumer + MongoDB/Redis writes
│   └── analytics/       # REST analytics API
├── internal/
│   ├── domain/          # Core types, interfaces — no framework imports
│   ├── kafka/           # Producer & consumer wrappers
│   ├── mongodb/         # Repository implementations
│   ├── redis/           # Cache & realtime counter helpers
│   ├── analytics/       # Business logic (caching, funnel math)
│   ├── middleware/       # Gin middleware (logging, metrics, auth)
│   └── config/          # Env-based config loader
├── deployments/
│   ├── docker-compose.yml
│   └── Dockerfile.*     # One per service, multi-stage
├── k8s/                 # Namespace, ConfigMap, Deployments, HPA, Ingress
├── scripts/seed/        # Demo data generator
└── .github/workflows/   # CI + release pipelines
```

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Go 1.22+

### Run locally

```bash
# 1. Clone
git clone https://github.com/nidhi24rajput/marketpulse.git
cd marketpulse

# 2. Start infrastructure + services
docker compose -f deployments/docker-compose.yml up -d

# 3. Wait ~20s for Kafka to be ready, then seed demo data
go run ./scripts/seed --store=demo-store --days=30 --orders=300

# 4. Query analytics
curl -H "X-Store-ID: demo-store" http://localhost:8081/v1/analytics/revenue
curl -H "X-Store-ID: demo-store" http://localhost:8081/v1/analytics/funnel
curl -H "X-Store-ID: demo-store" http://localhost:8081/v1/analytics/top-products?limit=5
curl -H "X-Store-ID: demo-store" http://localhost:8081/v1/analytics/realtime

# 5. Send a test event
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Store-ID: demo-store" \
  -d '{"type":"product_view","session_id":"abc123","properties":{"product_id":"prod-001","product_name":"Headphones","price":249.99}}'
```

### Kafka UI

Open `http://localhost:8090` to browse topics, consumer groups, and message offsets.

---

## API Reference

### Ingestion Service (:8080)

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/events` | Track a single event |
| `POST` | `/v1/events/batch` | Track up to 100 events in one call |
| `POST` | `/v1/orders` | Record a completed order |
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics |

**Event payload:**
```json
{
  "type": "product_view",
  "session_id": "sess-abc",
  "user_id": "user-123",
  "properties": {
    "product_id": "prod-001",
    "product_name": "Wireless Headphones",
    "price": 249.99
  }
}
```
Header: `X-Store-ID: your-store-id`

**Supported event types:** `page_view`, `product_view`, `add_to_cart`, `remove_from_cart`, `checkout_start`, `order_placed`, `order_paid`, `order_shipped`, `search`

### Analytics Service (:8081)

All endpoints require `X-Store-ID` header. Optional `from` / `to` query params (format `YYYY-MM-DD`, default last 30 days).

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/analytics/revenue` | Daily revenue, order count, avg order value |
| `GET` | `/v1/analytics/funnel` | Conversion rates at each purchase funnel step |
| `GET` | `/v1/analytics/top-products` | Top N products by revenue (`?limit=10`) |
| `GET` | `/v1/analytics/realtime` | Live 5-min session/event/order counters (Redis) |
| `GET` | `/v1/analytics/searches` | Most searched queries (`?limit=20`) |

**Revenue response:**
```json
{
  "data": [
    {
      "date": "2024-01-15",
      "revenue": 4821.50,
      "order_count": 38,
      "avg_order_value": 126.88
    }
  ],
  "from": "2024-01-01",
  "to": "2024-01-31"
}
```

---

## Design Decisions

### Why manual Kafka offset commits?

Auto-commit can acknowledge a message before the handler finishes processing it. If the service crashes mid-write, that event is lost. Manual commit (after a successful MongoDB write) gives at-least-once delivery — duplicates are possible but data loss is not. The `_id` on each event document provides idempotency.

### Why pre-computed daily aggregates?

Running a full aggregation pipeline over millions of raw events on every API request is expensive. The processor maintains a `daily_aggregates` collection via upsert — the analytics API reads pre-computed numbers for most queries, only hitting the raw collections for ad-hoc breakdowns. This trades some write amplification for predictable read latency.

### Why 3 replicas for the processor?

Kafka assigns one partition per consumer in a group. With 3 partitions and 3 processor replicas, each replica processes exactly 1 partition — no idle consumers, no hot partition. Adding more replicas beyond partition count would leave extras idle.

### TTL index on raw events

Raw events expire after 90 days via a MongoDB TTL index. Analytics history is preserved in the aggregates collection indefinitely. This keeps storage costs bounded without manual cleanup jobs.

---

## Deployment

### Railway (free tier — recommended for demos)

```bash
# Install Railway CLI
npm install -g @railway/cli
railway login

# Create a new project
railway init

# Add MongoDB, Redis, and Kafka plugins in the Railway dashboard
# Then deploy each service:
railway up --service ingestion
railway up --service processor
railway up --service analytics
```

Set environment variables in Railway's dashboard (use the values from `deployments/docker-compose.yml` as a reference).

### Kubernetes

```bash
# Apply all manifests
kubectl apply -f k8s/

# Check rollout
kubectl rollout status deployment/ingestion -n marketpulse
kubectl rollout status deployment/processor -n marketpulse
kubectl rollout status deployment/analytics -n marketpulse

# Scale processor to match partition count
kubectl scale deployment processor --replicas=3 -n marketpulse
```

---

## Running Tests

```bash
# Unit tests
go test ./...

# With race detector (always use this before merging)
go test -race ./...

# With coverage report
go test -coverprofile=coverage.out ./... && go tool cover -html=coverage.out
```

---

## Observability

Every service exposes `/metrics` in Prometheus format. Example metrics:

- `http_requests_total{method, path, status}` — request throughput
- `http_request_duration_seconds{method, path}` — latency histogram
- Standard Go runtime metrics (GC, goroutines, memory)

For local Grafana: add a Grafana service to docker-compose and point it at `http://host.docker.internal:8080/metrics`.

---

## Roadmap

- [ ] Python service: ML-based product recommendations (collaborative filtering)
- [ ] React dashboard: real-time charts consuming the analytics API
- [ ] gRPC internal transport between services (replace HTTP for processor→analytics)
- [ ] Dead-letter topic for failed events with retry backoff
- [ ] Multi-tenant auth: JWT with per-store claims
- [ ] OpenTelemetry distributed tracing

---
