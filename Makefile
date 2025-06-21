.PHONY: build clear docker/build lint lintfix fmt fmt-check githooks run test test-docs test-summary test-verbose test-coverage test-coverage-html vet

build:
	@go build -o bin cmd/main.go
	@echo "Built binary at: $(shell pwd)/bin/main"

clear:
	@rm -f ./n8n-tracer.state.json

docker/build:
	docker build --tag n8n-tracer:local .

lint:
	golangci-lint run

lintfix:
	golangci-lint run --fix

fmt:
	go fmt ./...

fmt-check:
	@if [ -n "$$(go fmt ./...)" ]; then \
		echo "Found unformatted Go files. Please run 'make fmt'"; \
		exit 1; \
	fi

githooks:
	@lefthook install
	@echo "Git hooks installed successfully"

run: build
	@./bin/main
	@echo "Running binary at: $(shell pwd)/bin/main"

test:
	go test -race ./...

test-docs:
	gotestsum --format gotestdox -- -race ./...

test-summary:
	gotestsum -- -race ./...

test-verbose:
	go test -race -v ./...

test-coverage:
	go test -race -coverprofile=coverage.out -covermode=atomic $(shell go list ./... | grep -v -E '/(internal/harness|cmd)$$')

test-coverage-html: test-coverage
	go tool cover -html=coverage.out -o coverage.html
	open coverage.html

vet:
	go vet ./...
