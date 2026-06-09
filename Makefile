.PHONY: up down build test lint seed clean

# ── Local dev ──────────────────────────────────────────────────────────────────
up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

logs:
	docker compose -f deployments/docker-compose.yml logs -f

# ── Build ──────────────────────────────────────────────────────────────────────
build:
	go build -o bin/ingestion  ./cmd/ingestion
	go build -o bin/processor  ./cmd/processor
	go build -o bin/analytics  ./cmd/analytics

build-docker:
	docker build -f deployments/Dockerfile.ingestion -t marketpulse-ingestion .
	docker build -f deployments/Dockerfile.processor -t marketpulse-processor .
	docker build -f deployments/Dockerfile.analytics -t marketpulse-analytics .

# ── Test & Lint ────────────────────────────────────────────────────────────────
test:
	go test -race -count=1 ./...

coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	open coverage.html

lint:
	golangci-lint run ./...

# ── Demo data ──────────────────────────────────────────────────────────────────
seed:
	go run ./scripts/seed --store=demo-store --days=30 --orders=300

seed-large:
	go run ./scripts/seed --store=demo-store --days=90 --orders=1000

# ── Kubernetes ────────────────────────────────────────────────────────────────
k8s-apply:
	kubectl apply -f k8s/

k8s-delete:
	kubectl delete -f k8s/

k8s-status:
	kubectl get pods -n marketpulse

# ── Helpers ───────────────────────────────────────────────────────────────────
clean:
	rm -rf bin/ coverage.out coverage.html

tidy:
	go mod tidy
	go mod verify
