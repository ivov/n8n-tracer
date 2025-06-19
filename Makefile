.PHONY: build clear docker/build lint lintfix fmt fmt-check githooks run test test-verbose test-coverage vet

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

test-verbose:
	go test -race -v ./...

test-coverage:
	go test -coverprofile=coverage.out $(shell go list ./... | grep -v -E '/(internal/harness|cmd)$$')
	go tool cover -html=coverage.out -o coverage.html
	open coverage.html

vet:
	go vet ./...
