# Development

To set up a development environment, follow these steps.

0. Clone repo:

```sh
git clone https://github.com/ivov/n8n-tracer.git
```

1. Install deps:

```sh
brew install go lefthook golangci-lint
go install gotest.tools/gotestsum@latest
make githooks
```

2. Set up env vars:

```sh
export N8N_DEPLOYMENT_MODE=regular
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export WATCH_FILE_PATH=~/.n8n/n8nEventLog.log
```

3. Start up an in-memory OTEL backend:

```sh
docker run --rm --name jaeger -p 16686:16686 -p 4318:4318 jaegertracing/all-in-one:latest
```

4. Make your changes, clear state and run:

```
make clear; make run

2025/06/18 18:42:51 n8n-tracer is starting... Press Ctrl+C to exit
2025/06/18 18:42:51 Configured for OTLP endpoint: http://localhost:4318
2025/06/18 18:42:51 Connected to OTLP endpoint
2025/06/18 18:42:51 Starting initial catch-up scan
2025/06/18 18:42:51 Starting health check server at port 8888
2025/06/18 18:42:51 Starting GC to clear spans older than 24h0m0s every 1h0m0s
2025/06/18 18:42:51 Processed 13 events
2025/06/18 18:42:51 Completed initial catch-up scan
```

5. Run n8n workflows locally, and inspect traces at `http://localhost:16686`
