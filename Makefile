.PHONY: all init build test test-integration vet lint fmt check

# The root module holds durable, sandbox and flow. The Kubernetes runner is its
# own module so importers do not inherit client-go.
MODULES := . sandbox/runner/k8s
INTEGRATION := ./durable/adapters/... ./durable/examples/chaos/...

all: check

init:
	git config core.hooksPath .githooks
	@for m in $(MODULES); do (cd $$m && go mod download && go mod tidy) || exit 1; done

build:
	@for m in $(MODULES); do (cd $$m && go build ./...) || exit 1; done

test:
	@for m in $(MODULES); do (cd $$m && go test -race ./...) || exit 1; done

test-integration:
	docker compose up -d --wait
	psql "$${DURABLE_POSTGRES_URL:-postgres://orchagent:orchagent@localhost:5432/orchagent?sslmode=disable}" -f durable/examples/quickstart/schema.sql
	go test -tags=integration $(INTEGRATION)

vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done

lint:
	@for m in $(MODULES); do (cd $$m && golangci-lint run) || exit 1; done

fmt:
	gofmt -w .

check: fmt vet lint test
