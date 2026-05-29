# Quick commands for the cdc project. Run `make help` for the list.

OUTPUT ?= events.jsonl

.PHONY: help build run test vet tidy clean \
        setup-pg setup-docker docker-up docker-down docker-logs

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Build all packages
	go build ./...

run: ## Run the cdc engine (OUTPUT=events.jsonl by default)
	go run ./cmd/cdc --output $(OUTPUT)

test: ## Run all tests
	go test ./...

vet: ## Run go vet
	go vet ./...

tidy: ## Tidy go.mod/go.sum
	go mod tidy

clean: ## Remove build artifacts and the output file
	go clean
	rm -f $(OUTPUT)

setup-pg: ## Configure local Homebrew Postgres for logical replication
	./scripts/setup_pg.sh

setup-docker: ## Bring up Dockerized Postgres (preconfigured)
	./scripts/setup_pg_docker.sh

docker-up: ## Start the Postgres container
	docker compose up -d

docker-down: ## Stop and remove the Postgres container
	docker compose down

docker-logs: ## Follow Postgres container logs
	docker compose logs -f postgres
